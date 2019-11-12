import abc
import docker
import json
import re
import os
import psutil
import string
import sys
import time
import ipaddress
from functools import reduce

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.baseservice
import taskexecutor.dbclient
import taskexecutor.httpsclient
import taskexecutor.utils

__all__ = ["Builder"]

UP = True
DOWN = False


class BuilderTypeError(Exception):
    pass


class ServiceReloadError(Exception):
    pass


class ConfigValidationError(Exception):
    pass


class OpService(metaclass=abc.ABCMeta):
    def __init__(self, name, declaration):
        self.name = name
        self._log_base_path = "/var/log"
        self._run_base_path = "/var/run"
        self._lock_base_path = "/var/lock"
        self._init_base_path = str()

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @name.deleter
    def name(self):
        del self._name

    @property
    def log_base_path(self):
        return self._log_base_path

    @log_base_path.setter
    def log_base_path(self, value):
        self._log_base_path = value

    @log_base_path.deleter
    def log_base_path(self):
        del self._log_base_path

    @property
    def run_base_path(self):
        return self._run_base_path

    @run_base_path.setter
    def run_base_path(self, value):
        self._run_base_path = value

    @run_base_path.deleter
    def run_base_path(self):
        del self._run_base_path

    @property
    def lock_base_path(self):
        return self._lock_base_path

    @lock_base_path.setter
    def lock_base_path(self, value):
        self._lock_base_path = value

    @lock_base_path.deleter
    def lock_base_path(self):
        del self._lock_base_path

    @property
    def init_base_path(self):
        return self._init_base_path

    @init_base_path.setter
    def init_base_path(self, value):
        self._init_base_path = value

    @init_base_path.deleter
    def init_base_path(self):
        del self._init_base_path

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

    def __repr__(self):
        return "{0}(name={1}, " \
               "log_base_path={2}, " \
               "run_base_path={3}, " \
               "lock_base_path={4}, " \
               "init_base_path={5}))".format(self.__class__.__name__,
                                             self.name,
                                             self.log_base_path,
                                             self.run_base_path,
                                             self.lock_base_path,
                                             self.init_base_path)


class UpstartService(OpService):
    def __init__(self, name, declaration):
        super().__init__(name, declaration)
        self.init_base_path = "/etc/init"

    def start(self):
        LOGGER.info("starting {} service via Upstart".format(self.name))
        taskexecutor.utils.exec_command("start {}".format(self.name))

    def stop(self):
        LOGGER.info("stopping {} service via Upstart".format(self.name))
        taskexecutor.utils.exec_command("stop {}".format(self.name))

    def restart(self):
        LOGGER.info("restarting {} service via Upstart".format(self.name))
        taskexecutor.utils.exec_command("restart {}".format(self.name))

    def reload(self):
        LOGGER.info("reloading {} service via Upstart".format(self.name))
        taskexecutor.utils.exec_command("reload {}".format(self.name))

    def status(self):
        status = DOWN
        try:
            status = UP if "running" in taskexecutor.utils.exec_command("status {}".format(self.name)) else DOWN
        except taskexecutor.utils.CommandExecutionError:
            pass
        return status


class SysVService(OpService):
    def __init__(self, name, declaration):
        super().__init__(name, declaration)
        self.init_base_path = "/etc/init.d"

    def start(self):
        LOGGER.info("starting {} service via init script".format(self.name))
        taskexecutor.utils.exec_command("invoke-rc.d {} start".format(self.name))

    def stop(self):
        LOGGER.info("stopping {} service via init script".format(self.name))
        taskexecutor.utils.exec_command("invoke-rc.d {} stop".format(self.name))

    def restart(self):
        LOGGER.info("restarting {} service via init script".format(self.name))
        taskexecutor.utils.exec_command("invoke-rc.d {} restart".format(self.name))

    def reload(self):
        LOGGER.info("reloading {} service via init script".format(self.name))
        taskexecutor.utils.exec_command("invoke-rc.d {} reload".format(self.name))

    def status(self):
        try:
            taskexecutor.utils.exec_command("invoke-rc.d {} status".format(self.name))
            return UP
        except Exception as e:
            LOGGER.warn(e)
            return DOWN


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
    def image(self):
        return self._image

    @image.setter
    def image(self, value):
        self._image = value

    @property
    def container(self):
        return next(iter(
            self._docker_client.containers.list(filters={"name": "^/" + self._container_name + "$"})
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

    def __init__(self, name, declaration):
        super().__init__(name, declaration)
        self._docker_client = docker.from_env()
        self._docker_client.login(**CONFIG.docker_registry._asdict())
        if hasattr(declaration, "template") and declaration.template and declaration.template.sourceUri:
            self._image = declaration.template.sourceUri.replace("docker://", "")
        elif hasattr(declaration, "template"):
            self._image = "{}/webservices/{}:master".format(CONFIG.docker_registry.registry, declaration.template.name)
        else:
            self._image = "{}/webservices/{}:master".format(CONFIG.docker_registry.registry, self.name)
        self._container_name = getattr(self, "_container_name", self.name)
        self._default_run_args = {"name": self._container_name,
                                  "detach": True,
                                  "init": True,
                                  "tty": False,
                                  "restart_policy": {"Name": "always"},
                                  "network": "host"}

    @taskexecutor.utils.synchronized
    def _pull_image(self):
        LOGGER.info("Pulling {} docker image".format(self.image))
        self._docker_client.images.pull(self.image)
        return self._docker_client.images.get(self.image)

    def _setup_env(self):
        self._env = {"${}".format(k): v for k, v in os.environ.items()}
        self._env.update({"${{{}}}".format(k): v for k, v in os.environ.items()})
        self._env.update(taskexecutor.utils.attrs_to_env(self))

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
        if self.status() != UP:
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
        arg_hints = json.loads(image.labels.get("ru.majordomo.docker.arg-hints-json"), "{}")
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
                LOGGER.warn("Failed to start new container {0}, renaming {0}_{1} back".format(self._container_name, timestamp))
                old_container.rename(self._container_name)
            raise
        if old_container:
            LOGGER.info("Killing and removing container {}_{}".format(self._container_name, timestamp))
            old_container.kill()
            old_container.remove()

    def reload(self):
        if self.status() == DOWN:
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
                return UP
        return DOWN


class SomethingInDocker(taskexecutor.baseservice.ConfigurableService,
                        taskexecutor.baseservice.NetworkingService, DockerService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.ConfigurableService.__init__(self)
        taskexecutor.baseservice.NetworkingService.__init__(self)
        DockerService.__init__(self, name, declaration)
        self.config_base_path = os.path.join("/opt", self.name)


class NginxInDocker(taskexecutor.baseservice.WebServer, DockerService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.WebServer.__init__(self)
        DockerService.__init__(self, name, declaration)
        self.config_base_path = "/opt/nginx/conf"
        self.site_template_name = "@NginxServerDocker"
        self.ssl_certs_base_path = CONFIG.nginx.ssl_certs_path

    def get_website_config(self, site_id):
        config = self.get_abstract_config(self.site_template_name,
                                          os.path.join("/etc/nginx/sites-available", site_id + ".conf"),
                                          config_type="website")
        config.enabled_path = os.path.join("/etc/nginx/sites-enabled/{}.conf".format(site_id))
        return config


class ApacheInDocker(taskexecutor.baseservice.WebServer, taskexecutor.baseservice.ApplicationServer, DockerService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.WebServer.__init__(self)
        taskexecutor.baseservice.ApplicationServer.__init__(self)
        DockerService.__init__(self, name, declaration)
        short_name = "apache2-{0.name}{0.version_major}{0.version_minor}".format(self.interpreter)
        self.image = "{}/webservices/{}:master".format(CONFIG.docker_registry.registry, short_name)
        self.sites_conf_path = "/etc/{}/sites-available".format(self.name)
        self.security_level = "-".join([e for e in self.name.split("-")
                                        if e != "apache2" and not e.startswith(self.interpreter.name)]) or None
        self.site_template_name = "@ApacheVHost"
        self.config_base_path = os.path.join("/etc", self.name)


class CronInDocker(DockerService):
    def __init__(self, name, declaration):
        super().__init__(name, declaration)
        self.passwd_root = "/opt"
        self.spool = "/opt/cron/tabs"

    def _get_uid(self, user_name):
        passwd = taskexecutor.constructor.get_conffile('lines', os.path.join(self.passwd_root, "etc/passwd"))
        matched = passwd.get_lines("^{}:".format(user_name))
        if len(matched) != 1:
            raise ValueError("Cannot determine user {0},"
                             "lines found in {2}: {1}".format(user_name, matched, passwd.file_path))
        return int(matched[0].split(":")[2])

    def _get_crontab_file(self, user_name):
        return taskexecutor.constructor.get_conffile("lines", os.path.join(self.spool, user_name),
                                                     owner_uid=self._get_uid(user_name), mode=0o600)

    def create_crontab(self, user_name, cron_tasks_list):
        crontab = self._get_crontab_file(user_name)
        crontab.body = "#{} crontab".format(user_name)
        for each in cron_tasks_list:
            crontab.add_line("#{}".format(each.execTimeDescription))
            crontab.add_line("{0.execTime} {0.command}".format(each))
        crontab.body += "\n"
        crontab.save()
        self.reload()

    def get_crontab(self, user_name):
        return self._get_crontab_file(user_name).body

    def delete_crontab(self, user_name):
        crontab = self._get_crontab_file(user_name)
        if crontab.exists:
            self._get_crontab_file(user_name).delete()
            self.reload()


class PostfixInDocker(DockerService):
    def enable_sendmail(self, uid):
        self.exec_defined_cmd("enable-uid-cmd", uid=uid)

    def disable_sendmail(self, uid):
        self.exec_defined_cmd("disable-uid-cmd", uid=uid)


class PersonalAppServer(taskexecutor.baseservice.WebServer, taskexecutor.baseservice.ApplicationServer, DockerService):
    def __init__(self, name, declaration):
        self._account_id = declaration.accountId
        self._unix_account = None
        taskexecutor.baseservice.WebServer.__init__(self)
        taskexecutor.baseservice.ApplicationServer.__init__(self)
        DockerService.__init__(self, name, declaration)
        self.config_base_path = os.path.join("/opt", self.name, "conf")
        self.sites_conf_path = os.path.join("/opt", self.name, "conf", "sites")

    def get_website_config(self, site_id):
        config = self.get_abstract_config(self.site_template_name,
                                          os.path.join(self.sites_conf_path, site_id + ".conf"),
                                          config_type="website")
        config.enabled_path = os.path.join("/tmp", site_id + ".conf")

    @property
    def unix_account(self):
        if not self._unix_account:
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                try:
                    self._unix_account = api.unixAccount().filter(accountId=self._account_id).get()[0]
                except IndexError:
                    pass
        return self._unix_account


class Nginx(taskexecutor.baseservice.WebServer, SysVService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.WebServer.__init__(self)
        SysVService.__init__(self, name, declaration)
        self.site_template_name = "@NginxServer"
        self.config_base_path = "/etc/nginx"
        self.static_base_path = CONFIG.nginx.static_base_path
        self.ssl_certs_base_path = CONFIG.nginx.ssl_certs_path

    def reload(self):
        taskexecutor.utils.set_apparmor_mode("enforce", "/usr/sbin/nginx")
        LOGGER.info("Testing nginx config")
        taskexecutor.utils.exec_command("nginx -t",)
        super().reload()
        taskexecutor.utils.set_apparmor_mode("enforce", "/usr/sbin/nginx")


class Apache(taskexecutor.baseservice.WebServer, taskexecutor.baseservice.ApplicationServer, UpstartService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.WebServer.__init__(self)
        taskexecutor.baseservice.ApplicationServer.__init__(self)
        UpstartService.__init__(self, name, declaration)
        self.site_template_name = "@ApacheVHost"
        self.config_base_path = os.path.join("/etc", self.name)
        self.static_base_path = CONFIG.nginx.static_base_path

    def reload(self):
        taskexecutor.utils.set_apparmor_mode("enforce", "/usr/sbin/apache2")
        LOGGER.info("Testing apache2 config in {}".format(self.config_base_path))
        taskexecutor.utils.exec_command("apache2ctl -d {} -t".format(self.config_base_path))
        super().reload()
        taskexecutor.utils.set_apparmor_mode("enforce", "/usr/sbin/apache2")


# HACK: the two 'Unmanaged' classes below are responsible for reloading services at baton.intr only
# would be removed when this server is gone
class UnmanagedNginx(taskexecutor.baseservice.WebServer, OpService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.WebServer.__init__(self)
        OpService.__init__(self, name, declaration)
        self.site_template_name = "@BatonNginxServer"
        self.config_base_path = "/usr/local/nginx/conf"

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def restart(self):
        raise NotImplementedError

    def reload(self):
        LOGGER.info("Testing nginx config")
        taskexecutor.utils.exec_command("/usr/local/nginx/sbin/nginx -t", shell="/usr/local/bin/bash")
        LOGGER.info("Reloading nginx")
        taskexecutor.utils.exec_command("/usr/local/nginx/sbin/nginx -s reload", shell="/usr/local/bin/bash")


class UnmanagedApache(taskexecutor.baseservice.WebServer, OpService):
    def __init__(self, name, declaration):
        apache_name_mangle = {"apache2-php4": "apache",
                              "apache2-php52": "apache5",
                              "apache2-php53": "apache53"}
        taskexecutor.baseservice.WebServer.__init__(self)
        OpService.__init__(self, apache_name_mangle[name], declaration)
        self.site_template_name = "@BatonApacheVHost"
        LOGGER.info("Apache name rewrited to '{}'".format(self.name))
        self.config_base_path = os.path.join("/usr/local", self.name, "conf")

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def restart(self):
        raise NotImplementedError

    def reload(self):
        LOGGER.info("Testing apache config: {}/conf/httpd.conf".format(self.config_base_path))
        taskexecutor.utils.exec_command("/usr/sbin/jail "
                                        "/usr/jail t 127.0.0.1 "
                                        "{0}/bin/httpd -T -f {0}/conf/httpd.conf".format(self.config_base_path),
                                        shell="/usr/local/bin/bash")
        LOGGER.info("Reloading apache")
        taskexecutor.utils.exec_command("{}/bin/apachectl2 graceful".format(self.config_base_path),
                                        shell="/usr/local/bin/bash")


class MySQL(taskexecutor.baseservice.DatabaseServer, SysVService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.DatabaseServer.__init__(self)
        SysVService.__init__(self, name, declaration)
        self.config_base_path = "/etc/mysql"
        self._dbclient = None
        self._full_privileges = CONFIG.mysql.common_privileges + CONFIG.mysql.write_privileges
        self._ignored_config_variables = CONFIG.mysql.ignored_config_variables

    @property
    def dbclient(self):
        if not self._dbclient:
            return taskexecutor.dbclient.MySQLClient(host=self.socket.mysql.address,
                                                     port=self.socket.mysql.port,
                                                     user=CONFIG.mysql.user,
                                                     password=CONFIG.mysql.password,
                                                     database="mysql")
        else:
            return self._dbclient

    @staticmethod
    def normalize_addrs(addrs_list):
        networks = ipaddress.collapse_addresses(ipaddress.IPv4Network(net)
                                                for net in CONFIG.database.default_allowed_networks + addrs_list)
        return [net.with_netmask for net in networks]

    def reload(self):
        LOGGER.info("Applying variables from config")
        config = self.get_concrete_config(os.path.join(self.config_base_path, "my.cnf"))
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
                return UP
        except Exception as e:
            LOGGER.warn(e)
            return DOWN

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
        for address in addrs_list:
            self.dbclient.execute_query("REVOKE {0} ON `{1}`.* FROM "
                                        "%s@%s".format(", ".join(self._full_privileges), database_name),
                                        (user_name, address))

    def allow_database_writes(self, database_name, user_name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("GRANT {0} ON `{1}`.* TO "
                                        "%s@%s".format(", ".join(CONFIG.mysql.write_privileges), database_name),
                                        (user_name, address))

    def deny_database_writes(self, database_name, user_name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("REVOKE {0} ON `{1}`.* FROM "
                                        "%s@%s".format(", ".join(CONFIG.mysql.write_privileges), database_name),
                                        (user_name, address))

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
        stdout = taskexecutor.utils.exec_command('cd /mysql/DB/ && find ./* -maxdepth 0 -type d -printf "%f\n" | xargs -n1 du -sb')
        return dict(
            line.split('\t')[::-1] for line in stdout[0:-1].split('\n')
        )

    def get_archive_stream(self, source, params={}):
        stdout, stderr = taskexecutor.utils.exec_command(
                "mysqldump -h{0.address} -P{0.port} "
                "-u{1.user} -p{1.password} {2} | nice -n 19 gzip -9c".format(self.socket.mysql, CONFIG.mysql, source),
                return_raw_streams=True
        )
        return stdout, stderr

    def restrict_user_cpu(self, name, time):
        self.dbclient.execute_query("REPLACE INTO mysql_restrict.CPU_RESTRICT (USER, MAX_CPU) VALUES (%s, %s)", (name, time))

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


class PostgreSQL(taskexecutor.baseservice.DatabaseServer, SysVService):
    def __init__(self, name, declaration):
        taskexecutor.baseservice.DatabaseServer.__init__(self)
        SysVService.__init__(self, name, declaration)
        self.config_base_path = "/etc/postgresql/9.3/main"
        self._dbclient = None
        self._hba_conf = taskexecutor.constructor.get_conffile("lines",
                                                               os.path.join(self.config_base_path, "pg_hba.conf"))
        self._full_privileges = CONFIG.postgresql.common_privileges + CONFIG.postgresql.write_privileges

    @property
    def dbclient(self):
        if not self._dbclient:
            return taskexecutor.dbclient.PostgreSQLClient(host=self.socket.postgresql.address,
                                                          port=self.socket.postgresql.port,
                                                          user=CONFIG.postgresql.user,
                                                          password=CONFIG.postgresql.password,
                                                          database="postgres")
        else:
            return self._dbclient

    @staticmethod
    def normalize_addrs(addrs_list):
        networks = ipaddress.collapse_addresses(ipaddress.IPv4Network(net)
                                                for net in CONFIG.database.default_allowed_networks + addrs_list)
        return networks

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
        hba_conf = self.get_concrete_config("pg_hba.conf")
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

    def get_archive_stream(self, source, params={}):
        stdout, stderr = taskexecutor.utils.exec_command(
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


class Builder:
    def __new__(cls, service_type, docker=False, personal=False):
        OpServiceClass = {docker:                                           SomethingInDocker,
                          service_type.endswith("CRON"):                    CronInDocker,
                          service_type.endswith("POSTFIX"):                 PostfixInDocker,
                          service_type == "STAFF_NGINX":                    Nginx if not docker else NginxInDocker,
                          service_type.startswith("WEBSITE_"):              Apache if not docker else ApacheInDocker,
                          service_type.startswith("WEBSITE_") and personal: PersonalAppServer,
                          service_type == "DATABASE_MYSQL":                 MySQL,
                          service_type == "DATABASE_POSTGRES":              PostgreSQL}.get(True)
        if not OpServiceClass:
            raise BuilderTypeError("Unknown OpService type: {}".format(service_type))
        return OpServiceClass
