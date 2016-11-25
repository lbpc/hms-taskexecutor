import os
import re
import abc
import collections
import ipaddress

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.httpsclient
import taskexecutor.dbclient
import taskexecutor.conffile
import taskexecutor.utils

__all__ = ["Builder"]


class OpService(metaclass=abc.ABCMeta):
    def __init__(self, name):
        self.name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @name.deleter
    def name(self):
        del self._name

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


class UpstartService(OpService):
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


class SysVService(OpService):
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


class NetworkingService:
    def __init__(self):
        self._sockets_map = dict()

    @property
    def socket(self):
        return collections.namedtuple("Socket", self._sockets_map.keys())(**self._sockets_map)

    def get_socket(self, protocol):
        return self._sockets_map[protocol]

    def set_socket(self, protocol, socket_obj):
        self._sockets_map[protocol] = socket_obj


class ConfigurableService:
    def __init__(self):
        self._concrete_configs_set = set()
        self._template_sources_map = dict()
        self._config_base_path = None

    @staticmethod
    def is_concrete_config(name):
        return False if re.match(r"{.+}", name) else True

    def set_config_from_template_obj(self, template_obj):
        self.set_template_source(template_obj.name, template_obj.fileLink)
        if self.is_concrete_config(template_obj.name):
            self.add_concrete_config(template_obj.name)

    def get_abstract_config(self, template_name, rel_path, config_type="templated"):
        config = taskexecutor.conffile.Builder(config_type, os.path.join(self.config_base_path, rel_path))
        config.template = self.get_config_template(self.get_template_source(template_name))
        return config

    def get_template_source(self, name):
        return self._template_sources_map[name]

    def set_template_source(self, name, value):
        self._template_sources_map[name] = value

    def add_concrete_config(self, rel_path):
        config = taskexecutor.conffile.Builder("templated", os.path.join(self.config_base_path, rel_path))
        self._concrete_configs_set.add(config)

    def get_concrete_configs_set(self):
        for config in self._concrete_configs_set:
            config.template = self.get_config_template(self.get_template_source(config.file_path))
        return self._concrete_configs_set

    def get_concrete_config(self, rel_path):
        for config in self._concrete_configs_set:
            if config.file_path == rel_path:
                config.template = self.get_config_template(self.get_template_source(config.file_path))
                return config
        raise KeyError("No such config: {}".format(rel_path))

    def get_config_template(self, template_source):
        with taskexecutor.httpsclient.GitLabClient(**CONFIG.gitlab._asdict()) as gitlab:
            return gitlab.get(template_source)

    @property
    def config_base_path(self):
        return self._config_base_path

    @config_base_path.setter
    def config_base_path(self, value):
        self._config_base_path = value

    @config_base_path.deleter
    def config_base_path(self):
        del self._config_base_path


class WebServer(ConfigurableService, NetworkingService):
    def __init__(self):
        ConfigurableService.__init__(self)
        NetworkingService.__init__(self)
        self._site_config_path_pattern = "sites-available/{}.conf"
        self._site_template_name = str()

    @property
    def site_template_name(self):
        return self._site_template_name

    @site_template_name.setter
    def site_template_name(self, value):
        self._site_template_name = value

    @site_template_name.deleter
    def site_template_name(self):
        del self._site_template_name

    @property
    def site_config_path_pattern(self):
        return self._site_config_path_pattern

    @site_config_path_pattern.setter
    def site_config_path_pattern(self, value):
        self._site_config_path_pattern = value

    @site_config_path_pattern.deleter
    def site_config_path_pattern(self):
        del self._site_config_path_pattern

    def get_website_config(self, site_id):
        return self.get_abstract_config(self.site_template_name,
                                        os.path.join(self.config_base_path,
                                                     self.site_config_path_pattern.format(site_id)),
                                        config_type="website")


class Nginx(WebServer, SysVService):
    def __init__(self):
        WebServer.__init__(self)
        SysVService.__init__(self, "nginx")
        self.site_template_name = "{NginxServer}.j2"
        self.config_base_path = "/etc/nginx"

    def reload(self):
        LOGGER.info("Testing nginx config")
        taskexecutor.utils.exec_command("nginx -t",)
        super().reload()
        taskexecutor.utils.set_apparmor_mode("enforce", "/usr/sbin/nginx")


class Apache(WebServer, UpstartService):
    def __init__(self, name):
        WebServer.__init__(self)
        UpstartService.__init__(self, name)
        self.site_template_name = "{ApacheVHost}.j2"
        self.config_base_path = "/etc/{}".format(self.name)

    def reload(self):
        LOGGER.info("Testing apache2 config in {}".format(self.config_base_path))
        taskexecutor.utils.exec_command("apache2ctl -d {} -t".format(self.config_base_path))
        super().reload()


# HACK: the two 'Unmanaged' classes below are responsible for reloading services at baton.intr only
# would be removed when this server is gone
class UnmanagedNginx(WebServer, OpService):
    def __init__(self):
        WebServer.__init__(self)
        OpService.__init__(self, "nginx")
        self.site_template_name = "{BatonNginxServer}.j2"
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


class UnmanagedApache(WebServer, OpService):
    def __init__(self, name):
        apache_name_mangle = {"apache2-php4": "apache",
                              "apache2-php52": "apache5",
                              "apache2-php53": "apache53"}
        WebServer.__init__(self)
        OpService.__init__(self, apache_name_mangle[name])
        LOGGER.info("Apache name rewrited to '{}'".format(self.name))
        self.config_base_path = "/usr/local/{}/conf".format(self.name)

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


class DatabaseServer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_user(self, name, password_hash, addrs_list):
        pass

    @abc.abstractmethod
    def update_user(self, user_name, password_hash, addrs_list):
        pass

    @abc.abstractmethod
    def drop_user(self, name):
        pass

    @abc.abstractmethod
    def create_database(self, name, allowed_users_list):
        pass

    @abc.abstractmethod
    def drop_database(self, name):
        pass

    @abc.abstractmethod
    def allow_database_access(self, database_name, users_list):
        pass

    @abc.abstractmethod
    def deny_database_access(self, database_name, users_list):
        pass

    @abc.abstractmethod
    def allow_database_writes(self, database_name, users_list):
        pass

    @abc.abstractmethod
    def deny_database_writes(self, database_name, users_list):
        pass


class MySQL(DatabaseServer, ConfigurableService, NetworkingService, SysVService):
    def __init__(self):
        ConfigurableService.__init__(self)
        NetworkingService.__init__(self)
        SysVService.__init__(self, "mysql")
        self.config_base_path = "/etc/mysql"
        self._queryset = list()
        self._full_privileges = CONFIG.mysql.common_privileges + CONFIG.mysql.write_privileges
        self._write_privileges = CONFIG.mysql.write_privileges

    @staticmethod
    def generate_allowed_addrs_set(addr_list):
        networks = [ipaddress.IPv4Network(net) for net in CONFIG.database.default_allowed_networks + addr_list]
        return set([net.with_netmask for net in networks])

    def _get_allowed_addrs_set(self, user_name):
        credentials = {"host": self.socket.mysql.address,
                       "port": self.socket.mysql.port,
                       "user": CONFIG.mysql.user,
                       "password": CONFIG.mysql.password}
        with taskexecutor.dbclient.MySQLClient(**credentials) as c:
            c.execute("SELECT host FROM mysql.user WHERE user = %s", (user_name,))
            return set([row[0] for row in c.fetchall()])

    def _query_grant_privileges(self, database_name, users_list, privileges_list):
        for user in users_list:
            self._queryset.extend(
                [("GRANT {} ON %s.* TO %s@%s IDENTIFIED BY "
                  "PASSWORD %s".format(", ".join(privileges_list)),
                  (database_name, user.name, address, user.passwordHash))
                 for address in self.generate_allowed_addrs_set(user.allowedAddressList)]
            )

    def _query_revoke_privileges(self, database_name, users_list, privileges_list):
        for user in users_list:
            self._queryset.extend(
                [("REVOKE {} ON %s.* FROM %s@%s".format(", ".join(privileges_list)),
                  (database_name, user.name, address))
                 for address in self.generate_allowed_addrs_set(user.allowedAddressList)]
            )

    def _run_queryset(self):
        credentials = {"host": self.socket.mysql.address,
                       "port": self.socket.mysql.port,
                       "user": CONFIG.mysql.user,
                       "password": CONFIG.mysql.password}
        with taskexecutor.dbclient.MySQLClient(**credentials) as c:
            for query, values in self._queryset:
                LOGGER.info("Executing query: {}".format(query % values))
                c.execute(query, values)
        self._queryset.clear()

    def create_user(self, name, password_hash, addrs_list):
        self._queryset.extend(
                [("CREATE USER %s@%s IDENTIFIED BY PASSWORD %s",
                  (name, address, password_hash)) for address in self.generate_allowed_addrs_set(addrs_list)]
        )
        self._run_queryset()

    def update_user(self, name, password_hash, addrs_list):
        current_addrs_set = self._get_allowed_addrs_set(name)
        staging_addrs_set = self.generate_allowed_addrs_set(addrs_list)
        self._queryset.extend(
                [("DROP USER %s@%s", (address,)) for address in current_addrs_set.difference(staging_addrs_set)]
        )
        self._queryset.extend(
                [("CREATE USER %s@%s IDENTIFIED BY PASSWORD %s",
                  (name, address, password_hash)) for address in staging_addrs_set.difference(current_addrs_set)]
        )
        self._queryset.extend(
                [("SET PASSWORD FOR %s@%s = %s",
                  (name, address, password_hash)) for address in current_addrs_set.intersection(staging_addrs_set)]
        )
        self._run_queryset()

    def drop_user(self, name):
        self._queryset.append((
            "SELECT GROUP_CONCAT('`', user, '`@`', host, '`') INTO @users FROM mysql.user WHERE user = %s;"
            "SET @users = CONCAT('DROP USER ', @users);"
            "PREPARE drop_statement FROM @users;"
            "EXECUTE drop_statement;"
            "DEALLOCATE PREPARE drop_statement", (name,)
        ))
        self._run_queryset()

    def create_database(self, name, allowed_users_list):
        self._queryset.append(("CREATE DATABASE IF NOT EXISTS %s", (name,)))
        self._query_grant_privileges(name, allowed_users_list, self._full_privileges)
        self._run_queryset()

    def drop_database(self, name):
        self._queryset.append(("DROP DATABASE %s", (name,)))
        self._run_queryset()

    def allow_database_access(self, database_name, users_list):
        self._query_grant_privileges(database_name, users_list, self._full_privileges)
        self._run_queryset()

    def deny_database_access(self, database_name, users_list):
        self._query_revoke_privileges(database_name, users_list, self._full_privileges)
        self._run_queryset()

    def allow_database_writes(self, database_name, users_list):
        self._query_grant_privileges(database_name, users_list, self._write_privileges)

    def deny_database_writes(self, database_name, users_list):
        self._query_revoke_privileges(database_name, users_list, self._write_privileges)


class PostgreSQL(DatabaseServer, ConfigurableService, NetworkingService, SysVService):
    def __init__(self):
        ConfigurableService.__init__(self)
        NetworkingService.__init__(self)
        SysVService.__init__(self, "postgresql")
        self.config_base_path = "/etc/postgresql/9.3/main"
        self._queryset = list()
        self._full_privileges = CONFIG.postgresql.common_privileges + CONFIG.postgresql.write_privileges
        self._write_privileges = CONFIG.postgresql.write_privileges

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
                raise Exception("Too few fields in line {0}: {1}".format(lineno, line))
            elif len(fields) == 4:
                conn_type, database, user, method = fields
            elif len(fields) == 5:
                conn_type, database, user, address, method = fields
            elif len(fields) == 6:
                conn_type, database, user, address, mask, method = fields
            else:
                raise Exception("Too many fields in line {0}: {1}".format(lineno, line))
            if conn_type not in ("local", "host", "hostssl", "hostnossl"):
                raise Exception("Unknown connection type '{0}' in line {1}: {2}".format(conn_type, lineno, line))
            if conn_type == "local" and address:
                raise Exception("Address field is not permitted for 'local' "
                                "connection type in line {0}: {1}".format(lineno, line))
            if conn_type != "local" and not address:
                raise Exception("Address field is required for '{0}' "
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
                        raise Exception("Invalid address '{0}' in line {1}: {2}".format(address, lineno, line))
            if method not in ("trust", "reject", "md5", "password", "gss", "sspi",
                              "krb5", "ident", "peer", "ldap", "radius", "pam"):
                raise Exception("Unknown auth method '{0}' in line {1}: {2}".format(conn_type, lineno, line))

    def _run_queryset(self):
        credentials = {"host": self.socket.postgresql.address,
                       "port": self.socket.postgresql.port,
                       "user": CONFIG.postgresql.user,
                       "password": CONFIG.postgresql.password}
        with taskexecutor.dbclient.PostgreSQLClient(**credentials) as c:
            for query, values in self._queryset:
                LOGGER.info("Executing query: {}".format(query % values))
                c.execute(query, values)
        self._queryset.clear()

    def _query_update_privileges(self, action, database_name, users_list, privileges_list):
        query_base = {"grant": "GRANT {} ON DATABASE %s TO %s",
                      "revoke": "REVOKE {} ON DATABASE %s FROM %s"}[action].format(", ".join(privileges_list))
        for user in users_list:
            self._queryset.extend([(query_base, (database_name, user.name))])

    def _update_hba_conf(self, database_name, users_list):
        hba_conf = self.get_concrete_config("pg_hba.conf")
        for user in users_list:
            networks_list = ipaddress.collapse_addresses([ipaddress.IPv4Network(addr)
                                                          for addr in user.allowedAddressList])
            for network in networks_list:
                config_line = "host {0} {1} {2} md5".format(database_name, user.name, network)
                if not hba_conf.has_line(config_line):
                    hba_conf.add_line(config_line)
        self._validate_hba_conf(hba_conf.body)
        hba_conf.save()

    def create_user(self, name, password_hash, addrs_list):
        self._queryset.append(("CREATE ROLE %s WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION "
                               "PASSWORD %s", (name, password_hash)))
        self._run_queryset()

    def update_user(self, user_name, password_hash, addrs_list):
        self._queryset.append(("ALTER ROLE %s WITH NOSUPERUSER INHERIT NOCREATEROLE NOCREATEDB LOGIN NOREPLICATION "
                               "PASSWORD %s", (user_name, password_hash)))
        self._run_queryset()

    def drop_user(self, name):
        self._queryset.append(("DROP ROLE %s", (name,)))
        self._run_queryset()

    def create_database(self, name, allowed_users_list):
        self._queryset.append(("CREATE DATABASE %", (name, )))
        self._query_update_privileges("grant", name, allowed_users_list, self._full_privileges)
        self._run_queryset()
        self._update_hba_conf(name, allowed_users_list)
        self.reload()

    def drop_database(self, name):
        self._queryset.append(("DROP DATABASE %s", (name, )))

    def allow_database_access(self, database_name, users_list):
        self._query_update_privileges("grant", database_name, users_list, self._full_privileges)
        self._run_queryset()

    def deny_database_access(self, database_name, users_list):
        self._query_update_privileges("revoke", database_name, users_list, self._full_privileges)
        self._run_queryset()

    def allow_database_writes(self, database_name, users_list):
        self._query_update_privileges("grant", database_name, users_list, self._write_privileges)
        self._run_queryset()

    def deny_database_writes(self, database_name, users_list):
        self._query_update_privileges("revoke", database_name, users_list, self._write_privileges)
        self._run_queryset()


class Builder:
    def __new__(cls, service_obj):
        service_name = cls.parse_service_type_name(service_obj.serviceType.name)
        if service_name == "nginx":
            service = Nginx()
        elif service_name.startswith("apache2"):
            service = Apache(service_name)
        elif service_name == "mysql":
            service = MySQL()
        if isinstance(service, NetworkingService):
            for socket in service_obj.serviceSockets:
                service.set_socket(cls.get_protocol_from_socket_name(socket.name), socket)
        if isinstance(service, ConfigurableService) and service.config_base_path:
            for template in service_obj.serviceTemplate.configTemplates:
                service.set_config_from_template_obj(template)
        return service

    @classmethod
    def parse_service_type_name(cls, name):
        return "-".join(name.lower().split("_")[1:])

    @classmethod
    def get_protocol_from_socket_name(cls, name):
        return name.split("@")[0].split("-")[-1]
