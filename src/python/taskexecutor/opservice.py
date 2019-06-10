import abc
import docker
import json
import re
import os
import psutil
import sys
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
    def __init__(self, name):
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
    def __init__(self, name):
        super().__init__(name)
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
    def __init__(self, name):
        super().__init__(name)
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
        volumes = args.pop("volumes") if "volumes" in args else {}
        def build_mount(v):
            target = v.pop("target") if "target" in v else None
            source = v.pop("source") if "source" in v else None
            return docker.types.Mount(target, source, **v)
        args["mounts"] = list(map(build_mount, volumes))
        return args

    def __init__(self, name):
        super().__init__(name)
        self._docker_client = docker.from_env()
        self._docker_client.login(**CONFIG.docker_registry)
        default_image = "{}/webservices/{}:latest".format(CONFIG.docker_registry.registry, self.name)
        self._image = getattr(self, "_image", default_image)
        self._container_name = getattr(self, "_container_name", self.name)
        self._default_run_args = {"name": self._container_name,
                                  "detach": True,
                                  "init": True,
                                  "tty": False,
                                  "restart_policy": {"Name": "always"},
                                  "network": "host"}
        self._docker_client.images.pull(self._image)
        image = self._docker_client.images.get(self._image)
        self._reload_cmd = image.labels.get("ru.majordomo.docker.exec.reload-cmd", "")

    def _env_var_from_self(self, var):
        return reduce(lambda o, a: getattr(o, a, None),
                      var.lstrip("$").strip("{}").lower().replace("_", ".").replace("-", "_").split("."),
                      self)

    def _subst_env_vars(self, args):
        res = {}
        for k, v in args.items():
            if isinstance(v, dict):
                res[k] = self._subst_env_vars(v)
            elif isinstance(v, str) and v.startswith("$"):
                res[k] = self._env_var_from_self(v)
            else:
                res[k] = v
        return res

    def start(self):
        self._docker_client.images.pull(self._image)
        image = self._docker_client.images.get(self._image)
        arg_hints = json.loads(image.labels.get("ru.majordomo.docker.arg-hints-json"), "{}")
        run_args = self._default_run_args.copy()
        run_args.update(self._normalize_run_args(self._subst_env_vars(arg_hints)))
        self._docker_client.containers.run(self._image, **run_args)

    def stop(self):
        container = self._docker_client.containers.get(self._container_name)
        container.stop()
        container.remove()

    def restart(self):
        self.stop()
        self.start()

    def reload(self):
        container = self._docker_client.containers.get(self._container_name)
        if self._reload_cmd:
            res = self._docker_client.exec_run(self._reload_cmd)
            if res.exit_code > 0:
                raise ServiceReloadError(res.output.decode())
        else:
            container.reload()
            pid = container.attrs["State"]["Pid"]
            if psutil.pid_exists(pid):
                psutil.Process(pid).send_signal(psutil.signal.SIGHUP)
            else:
                raise ServiceReloadError("No such PID: {}".format(pid))

    def status(self):
        try:
            container = self._docker_client.containers.get(self._container_name)
            container.reload()
            if container.status == "running":
                return UP
            else:
                return DOWN
        except docker.errors.NotFound:
            return DOWN


class NginxInDocker(taskexecutor.baseservice.WebServer, DockerService):
    def __init__(self, name):
        taskexecutor.baseservice.WebServer.__init__(self)
        DockerService.__init__(self, name)


class ApacheInDocker(taskexecutor.baseservice.WebServer, taskexecutor.baseservice.ApplicationServer, DockerService):
    def __init__(self, name):
        taskexecutor.baseservice.WebServer.__init__(self)
        taskexecutor.baseservice.ApplicationServer.__init__(self)
        short_name = "apache2-{0.name}{0.version_major}{0.version_minor}".format(self.interpreter)
        self._image = "{}/webservices/{}:latest".format(CONFIG.docker_registry.registry, short_name)
        DockerService.__init__(self, name)


class Nginx(taskexecutor.baseservice.WebServer, SysVService):
    def __init__(self, name):
        taskexecutor.baseservice.WebServer.__init__(self)
        SysVService.__init__(self, name)
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
    def __init__(self, name):
        taskexecutor.baseservice.WebServer.__init__(self)
        taskexecutor.baseservice.ApplicationServer.__init__(self)
        UpstartService.__init__(self, name)
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
    def __init__(self, name):
        taskexecutor.baseservice.WebServer.__init__(self)
        OpService.__init__(self, name)
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
    def __init__(self, name):
        apache_name_mangle = {"apache2-php4": "apache",
                              "apache2-php52": "apache5",
                              "apache2-php53": "apache53"}
        taskexecutor.baseservice.WebServer.__init__(self)
        OpService.__init__(self, apache_name_mangle[name])
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
    def __init__(self, name):
        taskexecutor.baseservice.DatabaseServer.__init__(self)
        SysVService.__init__(self, name)
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
                                        "collation_connection"
                                        ") VALUES(%s, %s, %s, %s, %s, %s)",
                                        ("{}@{}".format(user_name, address),
                                         vars_map.get("query_cache_type"),
                                         vars_map.get("character_set_client"),
                                         vars_map.get("character_set_connection"),
                                         vars_map.get("character_set_results"),
                                         vars_map.get("collation_connection")))

    def set_initial_permissions(self, user_name, addrs_list):
        for address in addrs_list:
            self.dbclient.execute_query("GRANT SELECT ON mysql_custom.session_vars TO %s@%s", (user_name, address))


class PostgreSQL(taskexecutor.baseservice.DatabaseServer, SysVService):
    def __init__(self, name):
        taskexecutor.baseservice.DatabaseServer.__init__(self)
        SysVService.__init__(self, name)
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
    def __new__(cls, service_type, docker=False):
        OpServiceClass = {service_type == "STAFF_NGINX": Nginx if not docker else NginxInDocker,
                          service_type.startswith("WEBSITE_"): Apache if not docker else ApacheInDocker,
                          service_type == "DATABASE_MYSQL": MySQL,
                          service_type == "DATABASE_POSTGRES": PostgreSQL}.get(True)
        if not OpServiceClass:
            raise BuilderTypeError("Unknown OpService type: {}".format(service_type))
        return OpServiceClass
