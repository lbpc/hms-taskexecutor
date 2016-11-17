import re
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from taskexecutor.httpsclient import GitLabClient
from taskexecutor.dbclient import MySQLClient
from taskexecutor.utils import ConfigFile, exec_command, set_apparmor_mode
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER


class OpService(metaclass=ABCMeta):
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

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def restart(self):
        pass

    @abstractmethod
    def reload(self):
        pass


class UpstartService(OpService):
    def start(self):
        LOGGER.info("starting {} service via Upstart".format(self.name))
        exec_command("start {}".format(self.name))

    def stop(self):
        LOGGER.info("stopping {} service via Upstart".format(self.name))
        exec_command("stop {}".format(self.name))

    def restart(self):
        LOGGER.info("restarting {} service via Upstart".format(self.name))
        exec_command("restart {}".format(self.name))

    def reload(self):
        LOGGER.info("reloading {} service via Upstart".format(self.name))
        exec_command("reload {}".format(self.name))


class SysVService(OpService):
    def start(self):
        LOGGER.info("starting {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} start".format(self.name))

    def stop(self):
        LOGGER.info("stopping {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} stop".format(self.name))

    def restart(self):
        LOGGER.info("restarting {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} restart".format(self.name))

    def reload(self):
        LOGGER.info("reloading {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} reload".format(self.name))


class NetworkingService:
    def __init__(self):
        self._sockets_map = dict()

    @property
    def socket(self):
        return namedtuple(
                "Socket", self._sockets_map.keys())(**self._sockets_map)

    def get_socket(self, protocol):
        return self._sockets_map[protocol]

    def set_socket(self, protocol, socket_obj):
        self._sockets_map[protocol] = socket_obj


class ConfigurableService:
    def __init__(self):
        self._abstract_configs_map = dict()
        self._concrete_configs_list = list()
        self._template_sources_map = dict()
        self._config_base_path = None

    @staticmethod
    def is_concrete_config(name):
        return False if re.match(r"{.+}", name) else True

    def set_config_from_template_obj(self, template_obj):
        self.set_template_source(template_obj.name, template_obj.fileLink)
        if self.is_concrete_config(template_obj.name):
            self.add_concrete_config(template_obj.name)

    def get_abstract_config(self, template_name, file_path):
        config = ConfigFile(file_path)
        config.template = \
            self.get_config_template(self.get_template_source(template_name))
        return config

    def get_template_source(self, name):
        return self._template_sources_map[name]

    def set_template_source(self, name, value):
        self._template_sources_map[name] = value

    def add_concrete_config(self, file_path):
        config = ConfigFile(file_path)
        self._concrete_configs_list.append(config)

    def get_concrete_configs_list(self):
        for config in self._concrete_configs_list:
            config.template = self.get_config_template(
                    self.get_template_source(config.file_path)
            )
        return self._concrete_configs_list

    def get_config_template(self, template_source):
        with GitLabClient(**CONFIG.gitlab._asdict()) as gitlab:
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
        self._site_config_path_pattern = "{0}/sites-available/{1}.conf"
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
        return self.get_abstract_config(
            self.site_template_name,
            self.site_config_path_pattern.format(self.config_base_path, site_id)
        )


class Nginx(WebServer, SysVService):
    def __init__(self):
        WebServer.__init__(self)
        SysVService.__init__(self, "nginx")
        self.site_template_name = "{NginxServer}.j2"
        self.config_base_path = "/etc/nginx"

    def reload(self):
        LOGGER.info("Testing nginx config")
        exec_command("nginx -t",)
        super().reload()
        set_apparmor_mode("enforce", "/usr/sbin/nginx")


class Apache(WebServer, UpstartService):
    def __init__(self, name):
        WebServer.__init__(self)
        UpstartService.__init__(self, name)
        self.site_template_name = "{ApacheVHost}.j2"
        self.config_base_path = "/etc/{}".format(self.name)

    def reload(self):
        LOGGER.info(
                "Testing apache2 config in {}".format(self.config_base_path)
        )
        exec_command("apache2ctl -d {} -t".format(self.config_base_path))
        super().reload()


# HACK: the two 'Unmanaged' classes below are responsible for
# reloading services at baton.intr only
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
        exec_command("/usr/local/nginx/sbin/nginx -t",
                     shell="/usr/local/bin/bash")
        LOGGER.info("Reloading nginx")
        exec_command("/usr/local/nginx/sbin/nginx -s reload",
                     shell="/usr/local/bin/bash")


class UnmanagedApache(WebServer, OpService):
    def __init__(self, name):
        apache_name_mangle = {"apache2-php4": "apache",
                              "apache2-php52": "apache5",
                              "apache2-php53": "apache53"}
        WebServer.__init__(self)
        OpService.__init__(self, apache_name_mangle[name])
        LOGGER.info("Apache name rewrited to '{}'".format(self.name))
        self.cfg_base = "/usr/local/{}/conf".format(self.name)

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def restart(self):
        raise NotImplementedError

    def reload(self):
        LOGGER.info("Testing apache config: "
                    "{}/conf/httpd.conf".format(self.cfg_base))
        exec_command(
            "/usr/sbin/jail "
            "/usr/jail t 127.0.0.1 "
            "{0}/bin/httpd -T -f {0}/conf/httpd.conf".format(self.cfg_base),
            shell="/usr/local/bin/bash"
        )
        LOGGER.info("Reloading apache")
        exec_command("{}/bin/apachectl2 graceful".format(self.cfg_base),
                     shell="/usr/local/bin/bash")


class DatabaseServer(metaclass=ABCMeta):
    def __init__(self):
        self._queryset = list()

    @property
    def queryset(self):
        return self._queryset

    @abstractmethod
    def add_create_user_query(self, name, password_hash, addresses_list):
        pass

    @abstractmethod
    def add_update_password_query(self, user_name,
                                  password_hash, addresses_list):
        pass

    @abstractmethod
    def add_drop_user_query(self, name):
        pass

    @abstractmethod
    def add_create_database_query(self, name, users_list):
        pass

    @abstractmethod
    def add_drop_database_query(self, name):
        pass

    @abstractmethod
    def add_grant_privileges_query(self, database_name,
                                   users_list, privileges_list):
        pass

    @abstractmethod
    def add_revoke_privileges_query(self, database_name,
                                    users_list, privileges_list):
        pass

    @abstractmethod
    def run_queryset(self):
        pass


class MySQL(DatabaseServer, NetworkingService, SysVService):

    def __init__(self):
        DatabaseServer.__init__(self)
        NetworkingService.__init__(self)
        SysVService.__init__(self, "mysql")

    def add_create_user_query(self, name, password_hash, addresses_list):
        self._queryset.append(
            (
                "CREATE USER `%s`@`%s` IDENTIFIED BY PASSWORD '%s'",
                (name, address, password_hash)
            ) for address in addresses_list
        )

    def add_update_password_query(self, user_name,
                                  password_hash, addresses_list):
        self._queryset.append(
            (
                "SET PASSWORD FOR `%s`@`%s` = '%s'",
                (user_name, address, password_hash)
            ) for address in addresses_list
        )

    def add_drop_user_query(self, name):
        self._queryset.append(
                ("DROP USER %s", (name,))
        )

    def add_create_database_query(self, name, users_list):
        self._queryset.append(
            ("CREATE DATABASE IF NOT EXISTS %s", (name,))
        )

    def add_drop_database_query(self, name):
        self._queryset.append(
            ("DROP DATABASE %s", (name,))
        )

    def add_grant_privileges_query(self, database_name,
                                   users_list, privileges_list):
        for user in users_list:
            self._queryset.append(
                ("GRANT {} ON `%s`.* "
                 "TO `%s`@`%s` "
                 "IDENTIFIED BY "
                 "PASSWORD '%s'".format(", ".join(privileges_list)),
                 (database_name, user.name, address, user.passwordHash))
                for address in user.addressList
            )

    def add_revoke_privileges_query(self, database_name,
                                    users_list, privileges_list):
        for user in users_list:
            self._queryset.append(
                ("REVOKE {} ON `%s`.* "
                 "FROM `%s`@`%s`".format(", ".join(privileges_list)),
                 (database_name, user.name, address))
                for address in user.addressList
            )

    def run_queryset(self):
        with MySQLClient(host=self.socket.mysql.address,
                         port=self.socket.mysql.port,
                         user=CONFIG.mysql.user,
                         password=CONFIG.mysql.password) as c:
            for query, values in self._queryset:
                LOGGER.info("Executing query: {}".format(query % values))
                c.execute(query, values)
        self._queryset.clear()


class OpServiceBuilder:
    def __new__(self, service_obj):
        service_name = \
            self.parse_service_type_name(service_obj.serviceType.name)
        if service_name == "nginx":
            service = Nginx()
        elif service_name.startswith("apache2"):
            service = Apache(service_name)
        elif service_name == "mysql":
            service = MySQL()
        if isinstance(service, NetworkingService):
            for socket in service_obj.serviceSockets:
                service.set_socket(
                        self.get_protocol_from_socket_name(socket.name), socket
                )
        if isinstance(service, ConfigurableService):
            for template in service_obj.serviceTemplate.configTemplates:
                service.set_config_from_template_obj(template)
        return service

    @staticmethod
    def parse_service_type_name(name):
        return "-".join(name.lower().split("_")[1:])

    @staticmethod
    def get_protocol_from_socket_name(name):
        return name.split("@")[0].split("-")[-1]
