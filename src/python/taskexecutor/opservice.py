import abc
import collections
import ipaddress
import json
import os
import pickle
import re
import string
import time
from enum import Enum
from functools import reduce
from itertools import chain, product

import docker
import psutil

import taskexecutor.builtinservice as bs
import taskexecutor.constructor as cnstr
import taskexecutor.utils as utils
from taskexecutor.config import CONFIG
from taskexecutor.dbclient import MySQLClient, PostgreSQLClient, DBError
from taskexecutor.httpsclient import ApiClient, GitLabClient
from taskexecutor.logger import LOGGER

__all__ = ["SomethingInDocker", "Cron", "Postfix", "SshD", "HttpServer", "Apache", "SharedAppServer", "PersonalAppServer",
           "MySQL", "PostgreSQL", "PersonalKVStore"]


class ServiceStatus(Enum):
    UP = True
    DOWN = False


class ServiceReloadError(Exception):
    pass


class ConfigValidationError(Exception):
    pass


class ConfigConstructionError(Exception):
    pass


class BaseService:
    def __init__(self, name, spec):
        self._name = name
        self._spec = spec

    @property
    def name(self): return self._name

    @property
    def spec(self): return self._spec

    def __str__(self):
        return "{0}(name='{1}', spec={2})".format(self.__class__.__name__, self.name, self.spec)


class NetworkingService(BaseService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._sockets_map = dict()

    @property
    def socket(self):
        return collections.namedtuple("Socket", self._sockets_map.keys())(**self._sockets_map)

    def get_socket(self, protocol):
        return self._sockets_map[protocol]

    def set_socket(self, protocol, socket_obj):
        self._sockets_map[protocol] = socket_obj


class ConfigurableService(BaseService):
    _cache = dict()
    _cache_path = os.path.join(utils.rgetattr(CONFIG, 'opservice.config_templates_cache', 'var/cache/te'),
                               'config_templates.pkl')

    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._tmpl_srcs = collections.defaultdict(dict)

    @classmethod
    def _dump_cache(cls):
        dirpath = os.path.dirname(cls._cache_path)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
        with open(cls._cache_path, "wb") as f:
            pickle.dump(cls._cache, f)
        LOGGER.debug("Config templates cache dumped to {}".format(cls._cache_path))

    @classmethod
    def _load_cache(cls):
        try:
            with open(cls._cache_path, "rb") as f:
                cls._cache.update(pickle.load(f))
                LOGGER.debug("Config templates cache updated from {}".format(cls._cache_path))
        except Exception as e:
            LOGGER.warning("Failed to load config templates cache, ERROR: {}".format(e))
            if cls._cache:
                LOGGER.info("In-memory config templates cache is not empty, dumping to disk")
                cls._dump_cache()

    @staticmethod
    def resolve_path_template(path_pattern, context_obj):
        subst_vars = [var.strip("{}") for var in re.findall(r"{[^{}]+}", path_pattern)]
        for subst_var in subst_vars:
            path_pattern = re.sub(r"{{{}}}".format(subst_var), "{}", path_pattern)
        subst_attrs = [reduce(getattr, subst_var.split("."), context_obj) for subst_var in subst_vars]
        return path_pattern.format(*subst_attrs)

    @property
    def config_base_path(self):
        return getattr(self, "_config_base_path", None) or os.path.join("/opt", self.name, "conf")

    def _context_name_of(self, context_obj):
        if context_obj is self:
            return 'SERVICE'
        elif context_obj.__class__.__name__.upper() == 'REDIRECT':
            return 'WEBSITE'
        else:
            return context_obj.__class__.__name__.upper()

    def get_config_template(self, template_source):
        if ConfigurableService._cache.get(template_source) \
                and ConfigurableService._cache[template_source]["timestamp"] + 10 > time.time():
            return ConfigurableService._cache[template_source]["value"]
        try:
            with GitLabClient(**utils.asdict(CONFIG.gitlab)) as gitlab:
                template = gitlab.get(template_source)
                ConfigurableService._cache[template_source] = {"timestamp": time.time(), "value": template}
                ConfigurableService._dump_cache()
                return template
        except Exception as e:
            LOGGER.warning("Failed to fetch config template from GitLab, ERROR: {}".format(e))
            LOGGER.warning("Probing local cache")
            ConfigurableService._load_cache()
            template = None
            if ConfigurableService._cache.get(template_source):
                template = ConfigurableService._cache[template_source].get("value")
            if not template:
                raise e
            return template

    def set_config(self, path_template, file_link, context_type="SERVICE"):
        self._tmpl_srcs[context_type][path_template] = file_link

    def get_config(self, path_template, context=None, config_type='templated'):
        context = context or self
        context_type = self._context_name_of(context)
        if context_type == 'SERVICE':
            for k, v in chain(utils.asdict(self.spec).items(), utils.asdict(self.spec.instanceProps).items()):
                if not hasattr(context, k): setattr(context, k, v)
        path = self.resolve_path_template(path_template, context)
        file_link = self._tmpl_srcs[context_type].get(path_template)
        if not file_link:
            path_resolved = {ctx: {self.resolve_path_template(k, context): v for k, v in mapp.items()}
                             for ctx, mapp in self._tmpl_srcs.items()}
            file_link = path_resolved[context_type].get(path_template)
            LOGGER.debug(f"'Path-resolved' template sources map: {path_resolved}, "
                         f"search path: '{context_type}'.'{path_template}'")
        # chroot all non-absolute paths to config_base_path
        if not os.path.isabs(path): path = os.path.join(self.config_base_path, path)
        if file_link:
            config = cnstr.get_conffile(config_type, path)
            config.template = self.get_config_template(file_link)
            return config
        LOGGER.debug(f"Template sources map: {self._tmpl_srcs}, "
                     f"search path: '{context_type}'.'{path_template}'")
        raise ConfigConstructionError(f"No '{path}' config defined for service {self.spec.name} "
                                      f"in '{context_type}' context")

    def get_configs_in_context(self, context):
        return (self.get_config(t, context) for t in self._tmpl_srcs[self._context_name_of(context)].keys())


class PersonalService(BaseService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._account_id = spec.accountId
        self._unix_account = None

    @property
    def unix_account(self):
        if not self._unix_account:
            with ApiClient(**CONFIG.apigw) as api:
                try:
                    self._unix_account = api.unixAccount().filter(accountId=self._account_id).get()[0]
                except IndexError:
                    pass
        return self._unix_account


class WebServer(ConfigurableService, NetworkingService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self.ssl_certs_base_path = "/opt/ssl"

    @property
    def sites_conf_path(self):
        return getattr(self, "_sites_conf_path", None) or os.path.join(self.config_base_path, "sites")

    def get_website_configs(self, website):
        return list(self.get_configs_in_context(website))

    def get_ssl_key_pair_files(self, basename):
        cert_file_path = os.path.join(self.ssl_certs_base_path, "{}.pem".format(basename))
        key_file_path = os.path.join(self.ssl_certs_base_path, "{}.key".format(basename))
        cert_file = cnstr.get_conffile('basic', cert_file_path)
        key_file = cnstr.get_conffile('basic', key_file_path)
        return cert_file, key_file


class ArchivableService(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_archive_stream(self, source, params=None):
        pass


class ApplicationServer(BaseService, ArchivableService):
    @property
    def interpreter(self):
        name = (getattr(self.spec.template, "language", "")).lower() or None
        version = getattr(self.spec.template, "version", "").split(".")
        version_major = next(iter(version[0:1]), None) or None
        version_minor = next(iter(version[1:2]), None)
        suffix = getattr(self.spec.instanceProps, "security_level", None)
        Interpreter = collections.namedtuple("Interpreter", "name version_major version_minor suffix")
        if any((name, version_major, version_minor, suffix)):
            return Interpreter(name, version_major, version_minor, suffix)

    def get_archive_stream(self, source, params=None):
        basedir = (params or {}).get('basedir')
        stdout, stderr = utils.exec_command(f'nice -n 19 '
                                            f'tar'
                                            f' --ignore-command-error'
                                            f' --ignore-failed-read'
                                            f' --warning=no-file-changed'
                                            f' -czf -'
                                            f' -C {basedir} {source}', return_raw_streams=True)
        return stdout, stderr


class DatabaseServer(ConfigurableService, NetworkingService, ArchivableService, metaclass=abc.ABCMeta):
    @staticmethod
    @abc.abstractmethod
    def normalize_addrs(addrs_list):
        pass

    @abc.abstractmethod
    def get_user(self, name):
        pass

    @abc.abstractmethod
    def get_all_database_names(self):
        pass

    @abc.abstractmethod
    def get_database(self, name):
        pass

    @abc.abstractmethod
    def create_user(self, name, password_hash, addrs_list):
        pass

    @abc.abstractmethod
    def set_password(self, user_name, password_hash, addrs_list):
        pass

    @abc.abstractmethod
    def drop_user(self, name, addrs_list):
        pass

    @abc.abstractmethod
    def create_database(self, name):
        pass

    @abc.abstractmethod
    def drop_database(self, name):
        pass

    @abc.abstractmethod
    def allow_database_access(self, database_name, user_name, addrs_list):
        pass

    @abc.abstractmethod
    def deny_database_access(self, database_name, user_name, addrs_list):
        pass

    @abc.abstractmethod
    def allow_database_writes(self, database_name, user_name, addrs_list):
        pass

    @abc.abstractmethod
    def deny_database_writes(self, database_name, user_name, addrs_list):
        pass

    @abc.abstractmethod
    def allow_database_reads(self, database_name, user_name, addrs_list):
        pass

    @abc.abstractmethod
    def get_database_size(self, database_name):
        pass

    @abc.abstractmethod
    def get_all_databases_size(self):
        pass

    @abc.abstractmethod
    def restrict_user_cpu(self, name, time):
        pass

    @abc.abstractmethod
    def unrestrict_user_cpu(self, name):
        pass

    @abc.abstractmethod
    def preset_user_session_vars(self, user_name, addrs_list, vars_map):
        pass

    @abc.abstractmethod
    def set_initial_permissions(self, user_name, addrs_list):
        pass


class OpService(BaseService, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def start(self):
        pass

    @abc.abstractmethod
    def stop(self):
        pass

    @abc.abstractmethod
    def restart(self):
        pass

    @abc.abstractmethod
    def reload(self):
        pass

    @abc.abstractmethod
    def status(self):
        pass


class UpstartService(OpService):
    def start(self):
        LOGGER.info(f'starting {self.name} service via Upstart')
        utils.exec_command(f'start {self.name}')

    def stop(self):
        LOGGER.info(f'stopping {self.name} service via Upstart')
        utils.exec_command(f'stop {self.name}')

    def restart(self):
        LOGGER.info(f'restarting {self.name} service via Upstart')
        utils.exec_command(f'restart {self.name}')

    def reload(self):
        LOGGER.info(f'reloading {self.name} service via Upstart')
        utils.exec_command(f'reload {self.name}')

    def status(self):
        status = ServiceStatus.DOWN
        try:
            status = ServiceStatus.UP if 'running' in utils.exec_command(f'status {self.name}') else ServiceStatus.DOWN
        except utils.CommandExecutionError:
            pass
        return status


class DockerService(OpService):
    @staticmethod
    def _normalize_run_args(args):
        def build_mount(v):
            target = v.pop("target") if "target" in v else None
            source = v.pop("source") if "source" in v else None
            return docker.types.Mount(target, source, **v)

        def build_publish(spec_chunks):
            res = []
            spec_chunks = iter(spec_chunks)
            while True:
                try:
                    chunk = next(spec_chunks)
                    try:
                        ipaddress.IPv4Address(chunk)
                        res.append((chunk, int(next(spec_chunks))))
                    except ipaddress.AddressValueError:
                        res.append(int(chunk))
                except StopIteration:
                    break
            return res if 0 > len(res) > 1 else res[0]

        volumes = args.pop("volumes") if "volumes" in args else {}
        args["mounts"] = list(map(build_mount, volumes))
        ports = args.pop("ports") if "ports" in args else ()
        args["ports"] = {e.split(":")[-1]: build_publish(e.split(":")[0:-1]) for e in ports}
        if "pid" in args:
            args["pid_mode"] = args["pid"]
            del args["pid"]
        return args

    @property
    def container(self):
        return next(iter(
            self._docker_client.containers.list(filters={"name": "^/" + self._container_name + "$"}, all=True)
        ), None)

    @property
    def env(self):
        return self._env

    @property
    def defined_commands(self):
        if self.container:
            self.container.reload()
            image = self.container.image
        else:
            image = self._pull_image()
        return {k.split(".")[-1]: string.Template(v)
                for k, v in image.labels.items()
                if k.startswith("ru.majordomo.docker.exec.")}

    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._docker_client = docker.from_env()
        self._docker_client.login(**utils.asdict(CONFIG.docker_registry))
        self.image = spec.template.sourceUri.replace("docker://", "")
        self._container_name = getattr(self, "_container_name", self.name)
        self._default_run_args = {"name": self._container_name,
                                  "detach": True,
                                  "init": True,
                                  "tty": False,
                                  "restart_policy": {"Name": "always"},
                                  "network": "host"}

    @utils.synchronized
    def _pull_image(self):
        LOGGER.info("Pulling {} docker image".format(self.image))
        try:
            self._docker_client.images.pull(self.image)
        except docker.errors.APIError as e:
            LOGGER.warning("Failed to pull docker image {}: {}".format(self.image, e))

        return self._docker_client.images.get(self.image)

    def _setup_env(self):
        self._env = {"${}".format(k): v for k, v in os.environ.items()}
        self._env.update({"${{{}}}".format(k): v for k, v in os.environ.items()})
        self._env.update(utils.attrs_to_env(self))

    def _subst_env_vars(self, to_subst):
        if isinstance(to_subst, str):
            for each in sorted(self.env, key=len, reverse=True):
                each = str(each)
                if each in to_subst:
                    return to_subst.replace(each, self.env[each])
            return to_subst
        elif isinstance(to_subst, list):
            return [self._subst_env_vars(e) for e in to_subst]
        elif isinstance(to_subst, dict):
            return {k: self._subst_env_vars(v) for k, v in to_subst.items()}
        else:
            return to_subst

    def exec_defined_cmd(self, cmd_name, **kwargs):
        cmd = self.defined_commands.get(cmd_name)
        if not cmd:
            raise ValueError("{} is not defined for {}, see Docker image labels".format(cmd_name, self.name))
        if self.status() != ServiceStatus.UP:
            raise RuntimeError("{} is not running".format(self._container_name))
        cmd = cmd.safe_substitute(**kwargs)
        self.container.reload()
        LOGGER.info("Running command inside {} container: {}".format(self._container_name, cmd))
        res = self.container.exec_run(cmd)
        if res.exit_code > 0:
            raise RuntimeError(res.output.decode())
        return res.output.decode()

    def start(self):
        image = self._pull_image()
        arg_hints = json.loads(image.labels.get("ru.majordomo.docker.arg-hints-json", "{}"))
        if arg_hints:
            LOGGER.info("Docker image {} has run arguments hints: {}".format(self.image, arg_hints))
        run_args = self._default_run_args.copy()
        self._setup_env()
        LOGGER.debug("`environment`: {}".format(self._env))
        run_args.update(self._normalize_run_args(self._subst_env_vars(arg_hints)))
        for each in run_args.get("mounts", ()):
            dir = each.get("Source")
            if dir and not os.path.isfile(dir):
                LOGGER.info("Creating {} directory".format(dir))
                os.makedirs(dir, exist_ok=True)
        if self.container:
            LOGGER.warn("Container {} already exists".format(self._container_name))
            self.stop()
        LOGGER.info("Running container {} with arguments: {}".format(self._container_name, run_args))
        self._docker_client.containers.run(self.image, **run_args)

    def stop(self):
        LOGGER.info("Stopping and removing container {}".format(self._container_name))
        self.container.stop()
        self.container.remove()

    def restart(self):
        if self.container.attrs["NetworkSettings"]["Ports"]:
            self._pull_image()
            self.stop()
            self.start()
        else:
            timestamp = str(int(time.time()))
            old_container = None
            if self.container:
                LOGGER.info("Renaming {0} container to {0}_{1}".format(self._container_name, timestamp))
                old_container = self.container
                old_container.rename("{}_{}".format(self._container_name, timestamp))
            try:
                self.start()
            except Exception:
                if old_container:
                    LOGGER.warn("Failed to start new container {0}, renaming {0}_{1} back".format(self._container_name,
                                                                                                  timestamp))
                    old_container.rename(self._container_name)
                raise
            if old_container:
                LOGGER.info("Killing and removing container {}_{}".format(self._container_name, timestamp))
                old_container.kill()
                old_container.remove()

    def reload(self):
        if self.status() == ServiceStatus.DOWN:
            LOGGER.warn("{} is down, starting it".format(self._container_name))
            self.start()
            return
        image = self._pull_image()
        if self.container.image.id != image.id:
            LOGGER.info("Image ID differs from existing container's image, restarting")
            self.restart()
            return
        self.container.reload()
        LOGGER.info("Reloading service inside container {}".format(self._container_name))
        if "reload-cmd" in self.defined_commands:
            self.exec_defined_cmd("reload-cmd")
        else:
            pid = self.container.attrs["State"]["Pid"]
            if psutil.pid_exists(pid):
                LOGGER.info("Sending SIGHUP to first process in container {} "
                            "(PID {})".format(self._container_name, pid))
                psutil.Process(pid).send_signal(psutil.signal.SIGHUP)
            else:
                raise ServiceReloadError("No such PID: {}".format(pid))

    def status(self):
        if self.container:
            self.container.reload()
            if self.container.status == "running":
                return ServiceStatus.UP
        return ServiceStatus.DOWN


class SomethingInDocker(ConfigurableService, NetworkingService, DockerService):
    pass


class HttpServer(WebServer, DockerService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self.ssl_certs_base_path = CONFIG.nginx.ssl_certs_path


class SharedAppServer(WebServer, ApplicationServer, DockerService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._config_base_path = os.path.join("/opt", self.name)
        self._sites_conf_path = os.path.join(self._config_base_path, "sites-available")
        self.security_level = getattr(getattr(self.spec, "instanceProps", None), "security_level", "default")


class PersonalAppServer(WebServer, ApplicationServer, DockerService, PersonalService): ...


class Cron(DockerService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self.passwd_root = "/opt"
        self.spool = "/opt/cron/tabs"

    def _get_uid(self, user_name):
        passwd = cnstr.get_conffile('lines', os.path.join(self.passwd_root, 'etc/passwd'))
        matched = passwd.get_lines("^{}:".format(user_name))
        if len(matched) != 1:
            raise ValueError("Cannot determine user {0},"
                             "lines found in {2}: {1}".format(user_name, matched, passwd.file_path))
        return int(matched[0].split(":")[2])

    def _get_crontab_file(self, user_name):
        return cnstr.get_conffile('lines', os.path.join(self.spool, user_name),
                                  owner_uid=self._get_uid(user_name), mode=0o600)

    def create_crontab(self, user_name, cron_tasks_list):
        crontab = self._get_crontab_file(user_name)
        crontab.body = "#{} crontab".format(user_name)
        for each in cron_tasks_list:
            crontab.add_line("#{}".format(each.execTimeDescription))
            crontab.add_line("{0.execTime} {0.command}".format(each))
        crontab.body += "\n"
        crontab.save()

    def get_crontab(self, user_name):
        try:
            return self._get_crontab_file(user_name).body
        except ValueError:
            return ''

    def delete_crontab(self, user_name):
        crontab = self._get_crontab_file(user_name)
        if crontab.exists:
            self._get_crontab_file(user_name).delete()


class Postfix(DockerService):
    def start(self):
        uid = utils.rgetattr(CONFIG, 'posfix.uid', 13)
        home = utils.rgetattr(CONFIG, 'postfix.home', '/opt/postfix')
        mgr = bs.LinuxUserManager()
        postfix_user = mgr.get_user('postfix')
        if not postfix_user:
            mgr.create_user('postfix',
                            uid=uid, home_dir=home, pass_hash=None, shell=mgr.disabled_shell, extra_groups=['postdrop'])
        elif postfix_user.uid != uid:
            mgr.change_uid('postfix', uid)
        nobody = mgr.get_user('nobody')
        if not nobody:
            mgr.create_user('nobody', uid=65534, home_dir='/nowhere', pass_hash=None, shell=mgr.disabled_shell)
        super().start()

    def enable_sendmail(self, uid):
        if self.status() is not ServiceStatus.UP:
            LOGGER.warning(f'{self.name} is down, trying to start it')
            self.start()
        self.exec_defined_cmd("enable-uid-cmd", uid=uid)

    def disable_sendmail(self, uid):
        if self.status() is not ServiceStatus.UP:
            LOGGER.warning(f'{self.name} is down, trying to start it')
            self.start()
        self.exec_defined_cmd("disable-uid-cmd", uid=uid)


class SshD(DockerService):
    def start(self):
        uid = utils.rgetattr(CONFIG, 'sshd.uid', 103)
        home = utils.rgetattr(CONFIG, 'sshd.home', '/var/run/sshd')
        mgr = bs.LinuxUserManager()
        sshd_user = mgr.get_user('sshd')
        if not sshd_user:
            mgr.create_user('sshd',
                            uid=uid, home_dir=home, pass_hash=None, shell=mgr.disabled_shell)
        elif sshd_user.uid != uid:
            mgr.change_uid('sshd', uid)


class Apache(WebServer, ApplicationServer, UpstartService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._config_base_path = os.path.join("/opt", self.name)
        self.static_base_path = CONFIG.nginx.static_base_path
        self.log_base_path = os.path.join("/var/log", self.name)
        self.run_base_path = os.path.join("/var/run", self.name)
        self.lock_base_path = os.path.join("/var/lock", self.name)
        self.init_base_path = os.path.join("/etc/init", self.name)

    def reload(self):
        utils.set_apparmor_mode("enforce", "/usr/sbin/apache2")
        LOGGER.info("Testing apache2 config in {}".format(self.config_base_path))
        utils.exec_command("apache2ctl -d {} -t".format(self.config_base_path))
        super().reload()
        utils.set_apparmor_mode("enforce", "/usr/sbin/apache2")


class MySQL(DatabaseServer, OpService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._config_base_path = "/opt/mysql"
        self._dbclient = None
        self._full_privileges = CONFIG.mysql.common_privileges + CONFIG.mysql.write_privileges
        self._ignored_config_variables = CONFIG.mysql.ignored_config_variables

    @property
    def dbclient(self):
        if not self._dbclient:
            return MySQLClient(host=self.socket.mysql.address,
                               port=self.socket.mysql.port,
                               user=CONFIG.mysql.user,
                               password=CONFIG.mysql.password,
                               database="mysql")
        else:
            return self._dbclient

    @staticmethod
    def normalize_addrs(addrs_list):
        return [net.with_netmask for net in ipaddress.collapse_addresses(ipaddress.IPv4Network(n) for n in addrs_list)]

    def reload(self):
        LOGGER.info("Applying variables from config")
        config = self.get_config(os.path.join(self.config_base_path, "my.cnf"))
        config_vars = dict()
        mysqld_section_started = False
        for line in config.body.split("\n"):
            if line.strip() == "[mysqld]":
                mysqld_section_started = True
                continue
            if mysqld_section_started and line.startswith("["):
                break
            if mysqld_section_started and line and not line.startswith("#") and "=" in line:
                variable, *value = line.split("=")
                value = "=".join(value)
                if "-" not in variable:
                    config_vars[variable.strip()] = value.strip().strip('"\'')
        actual_vars = {row[0]: row[1] for row in self.dbclient.execute_query("SHOW VARIABLES", ())}
        for variable, value in config_vars.items():
            if variable in self._ignored_config_variables:
                continue
            if re.match(r"\d+(K|M|G)", value):
                value = int(value[:-1]) * {"K": 1024, "M": 1048576, "G": 1073741824}[value[-1]]
            if isinstance(value, str) and value.isdecimal():
                value = int(value)
            if actual_vars.get(variable) in ("ON", "OFF") and value in (1, 0):
                value = {1: "ON", 0: "OFF"}[value]
            if actual_vars.get(variable) != str(value):
                LOGGER.debug("MySQL variable: {0}, "
                             "old value: {1}, new value: {2}".format(variable, actual_vars.get(variable), value))
                if isinstance(value, int):
                    self.dbclient.execute_query("SET GLOBAL {0}={1}".format(variable, value), ())
                else:
                    self.dbclient.execute_query("SET GLOBAL {0}=%s".format(variable), (value,))

    def status(self):
        try:
            if self.dbclient.execute_query("SELECT 1", ())[0][0] == 1:
                return ServiceStatus.UP
        except Exception as e:
            LOGGER.warn(e)
            return ServiceStatus.DOWN

    def start(self):
        pass

    def stop(self):
        pass

    def restart(self):
        pass

    def get_user(self, name):
        name, password_hash, comma_separated_addrs = self.dbclient.execute_query(
            "SELECT User, Password, GROUP_CONCAT(Host) FROM mysql.user WHERE User = %s", (name,))[0]
        if not name:
            return "", "", []
        addrs = [] if not comma_separated_addrs else comma_separated_addrs.split(",")
        return name, password_hash, addrs

    def get_all_database_names(self):
        return [r[0] for r in self.dbclient.execute_query("SHOW DATABASES", ())]

    def get_database(self, name):
        rows = self.dbclient.execute_query("SELECT Db, User FROM mysql.db WHERE Db = %s", (name,))
        if not rows:
            return next((db_name for db_name in self.get_all_database_names() if db_name == name), ""), []
        name = rows[0][0]
        users = [self.get_user(row[1]) for row in set(rows)]
        return name, users

    def create_user(self, name, password_hash, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("CREATE USER %s@%s IDENTIFIED BY PASSWORD %s", (name, address, password_hash))

    def set_password(self, name, password_hash, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("SET PASSWORD FOR %s@%s = %s", (name, address, password_hash))

    def drop_user(self, name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("DROP USER %s@%s", (name, address))
            self.dbclient.execute_query("DELETE FROM mysql_custom.session_vars WHERE user=%s",
                                        ("{}@{}".format(name, address),))

    def create_database(self, name):
        self.dbclient.execute_query("CREATE DATABASE IF NOT EXISTS `{}`".format(name), ())

    def drop_database(self, name):
        self.dbclient.execute_query("DROP DATABASE  IF EXISTS `{}`".format(name), ())

    def allow_database_access(self, database_name, user_name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("GRANT {0} ON `{1}`.* TO "
                                        "%s@%s".format(", ".join(self._full_privileges), database_name),
                                        (user_name, address))

    def deny_database_access(self, database_name, user_name, addrs_list):
        for address, priv in product(addrs_list, self._full_privileges):
            try:
                self.dbclient.execute_query(f'REVOKE {priv} ON `{database_name}`.* FROM %s@%s', (user_name, address))
            except DBError as e:
                if e.args[0] != 1141: raise

    def allow_database_writes(self, database_name, user_name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("GRANT {0} ON `{1}`.* TO "
                                        "%s@%s".format(", ".join(CONFIG.mysql.write_privileges), database_name),
                                        (user_name, address))

    def deny_database_writes(self, database_name, user_name, addrs_list):
        for address, priv in product(addrs_list, CONFIG.mysql.write_privileges):
            try:
                self.dbclient.execute_query(f'REVOKE {priv} ON `{database_name}`.* FROM %s@%s', (user_name, address))
            except DBError as e:
                if e.args[0] != 1141: raise

    def allow_database_reads(self, database_name, user_name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("GRANT {0} ON `{1}`.* TO "
                                        "%s@%s".format(", ".join(CONFIG.mysql.common_privileges), database_name),
                                        (user_name, address))

    def get_database_size(self, database_name):
        return int(self.dbclient.execute_query(
            "SELECT SUM(data_length+index_length) FROM information_schema.tables WHERE table_schema=%s",
            (database_name,)
        )[0][0])

    def get_all_databases_size(self):
        stdout = utils.exec_command('cd /mysql/DB/ && find ./* -maxdepth 0 -type d -printf "%f\n" | xargs -n1 du -sb')
        return dict(
            line.split('\t')[::-1] for line in stdout[0:-1].split('\n')
        )

    def get_archive_stream(self, source, params=None):
        stdout, stderr = utils.exec_command(
            "mysqldump -h{0.address} -P{0.port} "
            "-u{1.user} -p{1.password} {2} | nice -n 19 gzip -9c".format(self.socket.mysql, CONFIG.mysql, source),
            return_raw_streams=True
        )
        return stdout, stderr

    def restrict_user_cpu(self, name, time):
        self.dbclient.execute_query("REPLACE INTO mysql_restrict.CPU_RESTRICT (USER, MAX_CPU) VALUES (%s, %s)",
                                    (name, time))

    def unrestrict_user_cpu(self, name):
        self.dbclient.execute_query("DELETE FROM mysql_restrict.CPU_RESTRICT WHERE USER = %s", (name,))

    def preset_user_session_vars(self, user_name, addrs_list, vars_map):
        for address in addrs_list:
            self.dbclient.execute_query("REPLACE INTO mysql_custom.session_vars("
                                        "user, "
                                        "query_cache_type, "
                                        "character_set_client, "
                                        "character_set_connection, "
                                        "character_set_results, "
                                        "collation_connection,"
                                        "innodb_strict_mode"
                                        ") VALUES(%s, %s, %s, %s, %s, %s, %s)",
                                        ("{}@{}".format(user_name, address),
                                         vars_map.get("query_cache_type"),
                                         vars_map.get("character_set_client"),
                                         vars_map.get("character_set_connection"),
                                         vars_map.get("character_set_results"),
                                         vars_map.get("collation_connection"),
                                         vars_map.get("innodb_strict_mode")))

    def set_initial_permissions(self, user_name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("GRANT SELECT ON mysql_custom.session_vars TO %s@%s", (user_name, address))


class PostgreSQL(DatabaseServer, OpService):
    def __init__(self, name, spec):
        super().__init__(name, spec)
        self._config_base_path = "/etc/postgresql/9.3/main"
        self._dbclient = None
        self._hba_conf = cnstr.get_conffile('lines', os.path.join(self.config_base_path, 'pg_hba.conf'))
        self._full_privileges = CONFIG.postgresql.common_privileges + CONFIG.postgresql.write_privileges

    @property
    def dbclient(self):
        if not self._dbclient:
            return PostgreSQLClient(host=self.socket.postgresql.address,
                                    port=self.socket.postgresql.port,
                                    user=CONFIG.postgresql.user,
                                    password=CONFIG.postgresql.password,
                                    database="postgres")
        else:
            return self._dbclient

    @staticmethod
    def normalize_addrs(addrs_list):
        return ipaddress.collapse_addresses(ipaddress.IPv4Network(net) for net in addrs_list)

    @staticmethod
    def _validate_hba_conf(config_body):
        for lineno, line in enumerate(config_body.split("\n")):
            if line.startswith("#") or len(line) == 0:
                continue
            options = []
            address = None
            mask = None
            fields = line.split()
            while "=" in fields[-1]:
                options.append(fields.pop(-1))
            if len(fields) < 4:
                raise ConfigValidationError("Too few fields in line {0}: {1}".format(lineno, line))
            elif len(fields) == 4:
                conn_type, database, user, method = fields
            elif len(fields) == 5:
                conn_type, database, user, address, method = fields
            elif len(fields) == 6:
                conn_type, database, user, address, mask, method = fields
            else:
                raise ConfigValidationError("Too many fields in line {0}: {1}".format(lineno, line))
            if conn_type not in ("local", "host", "hostssl", "hostnossl"):
                raise ConfigValidationError("Unknown connection type '{0}' "
                                            "in line {1}: {2}".format(conn_type, lineno, line))
            if conn_type == "local" and address:
                raise ConfigValidationError("Address field is not permitted for 'local' "
                                            "connection type in line {0}: {1}".format(lineno, line))
            if conn_type != "local" and not address:
                raise ConfigValidationError("Address field is required for '{0}' "
                                            "connection type in line {1}: {2}".format(conn_type, lineno, line))
            if address and mask:
                address = "{0}/{1}".format(address, mask)
            if address and not re.match(r"^(.?([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*"
                                        r"([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", address):
                try:
                    ipaddress.IPv4Network(address)
                except ipaddress.AddressValueError:
                    try:
                        ipaddress.IPv6Network(address)
                    except ipaddress.AddressValueError:
                        raise ConfigValidationError("Invalid address '{0}' in "
                                                    "line {1}: {2}".format(address, lineno, line))
            if method not in ("trust", "reject", "md5", "password", "gss", "sspi",
                              "krb5", "ident", "peer", "ldap", "radius", "pam"):
                raise ConfigValidationError("Unknown auth method '{0}' in "
                                            "line {1}: {2}".format(conn_type, lineno, line))

    def _update_hba_conf(self, database_name, users_list):
        hba_conf = self.get_config("pg_hba.conf")
        for user in users_list:
            networks_list = ipaddress.collapse_addresses([ipaddress.IPv4Network(addr)
                                                          for addr in user.allowedIPAddresses])
            for network in networks_list:
                config_line = "host {0} {1} {2} md5".format(database_name, user.name, network)
                if not hba_conf.has_line(config_line):
                    hba_conf.add_line(config_line)
        self._validate_hba_conf(hba_conf.body)
        hba_conf.save()

    def get_user(self, name):
        rows = self.dbclient.execute_query("SELECT rolpassword FROM pg_authid WHERE rolname = %s", (name,))
        if not rows:
            return "", "", []
        password_hash = rows[0][0]
        related_config_lines = self._hba_conf.get_lines(r"host\s.+\s{}\s.+\smd5".format(name)) or []
        addrs = [line[3] for line in related_config_lines]
        return name, password_hash, addrs

    def get_all_database_names(self):
        return [r[0] for r in
                self.dbclient.execute_query("SELECT datname FROM pg_database WHERE datistemplate = false", ())]

    def get_database(self, name):
        related_config_lines = self._hba_conf.get_lines(r"host\s{}\s.+\s.+\smd5".format(name)) or []
        users = [self.get_user(user_name) for user_name in [line[2] for line in related_config_lines]]
        return name, users

    def create_user(self, name, password_hash, addrs_list):
        self.dbclient.execute_query("CREATE ROLE %s WITH "
                                    "NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION "
                                    "PASSWORD %s", (name, password_hash))

    def set_password(self, user_name, password_hash, addrs_list):
        self.dbclient.execute_query("ALTER ROLE %s WITH "
                                    "NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION "
                                    "PASSWORD %s", (user_name, password_hash))

    def drop_user(self, name, addrs_list):
        self.dbclient.execute_query("DROP ROLE %s", (name,))
        related_lines = list()
        for address in addrs_list:
            related_lines.extend(self._hba_conf.get_lines(r"host\s.+\{0}\s{1}\smd5".format(name, address)))
        for line in related_lines:
            self._hba_conf.remove_line(line)
        self._validate_hba_conf(self._hba_conf.body)
        self._hba_conf.save()
        self.reload()

    def create_database(self, name):
        self.dbclient.execute_query("CREATE DATABASE %", (name,))

    def drop_database(self, name):
        self.dbclient.execute_query("DROP DATABASE %s", (name,))

    def allow_database_access(self, database_name, user_name, addrs_list):
        self.dbclient.execute_query("GRANT {} ON DATABASE %s TO %s".format(self._full_privileges),
                                    (database_name, user_name))
        for addr in addrs_list:
            line = "host {0} {1} {2} md5".format(database_name, user_name, addr)
            if not self._hba_conf.has_line(line):
                self._hba_conf.add_line(line)
        self._validate_hba_conf(self._hba_conf.body)
        self._hba_conf.save()
        self.reload()

    def deny_database_access(self, database_name, user_name, addrs_list):
        self.dbclient.execute_query("REVOKE {} ON DATABASE %s FROM %s".format(self._full_privileges),
                                    (database_name, user_name))
        related_lines = list()
        for address in addrs_list:
            related_lines.extend(
                self._hba_conf.get_lines(r"host\s{0}\{1}\s{2}\smd5".format(database_name, user_name, address))
            )
        for line in related_lines:
            self._hba_conf.remove_line(line)
        self._validate_hba_conf(self._hba_conf.body)
        self._hba_conf.save()
        self.reload()

    def allow_database_writes(self, database_name, user_name, addrs_list):
        self.dbclient.execute_query("GRANT {} ON DATABASE %s TO %s".format(CONFIG.postgresql.write_privileges),
                                    (database_name, user_name))

    def deny_database_writes(self, database_name, user_name, addrs_list):
        self.dbclient.execute_query("REVOKE {} ON DATABASE %s FROM %s".format(CONFIG.postgresql.write_privileges),
                                    (database_name, user_name))

    def allow_database_reads(self, database_name, user_name, addrs_list):
        self.dbclient.execute_query("GRANT {} ON DATABASE %s TO %s".format(CONFIG.postgresql.common_privileges),
                                    (database_name, user_name))

    def get_database_size(self, database_name):
        return int(self.dbclient.execute_query("SELECT pg_database_size(%s)", (database_name,))[0][0])

    def get_all_databases_size(self):
        databases = [row[0] for row in
                     self.dbclient.execute_query("SELECT datname FROM pg_database WHERE datistemplate=false", ())]
        return {database: self.get_database_size(database) for database in databases}

    def get_archive_stream(self, source, params=None):
        stdout, stderr = utils.exec_command(
            "pg_dump --host {0.address} --port {0.port} --user {1.user} --password {1.password} "
            "{2} | gzip -9c".format(self.socket.psql, CONFIG.postgresql, source), return_raw_streams=True
        )
        return stdout, stderr

    def restrict_user_cpu(self, name, time):
        return

    def unrestrict_user_cpu(self, name):
        return

    def preset_user_session_vars(self, user_name, addrs_list, vars_map):
        return

    def set_initial_permissions(self, user_name, addrs_list):
        return


class PersonalKVStore(DockerService, PersonalService): ...
