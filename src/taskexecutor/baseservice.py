import abc
import collections
import functools
import os
import re

from taskexecutor.config import CONFIG
import taskexecutor.constructor
import taskexecutor.httpsclient


class ConfigConstructionError(Exception):
    pass


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

    @property
    def config_base_path(self):
        return self._config_base_path

    @config_base_path.setter
    def config_base_path(self, value):
        self._config_base_path = value

    @config_base_path.deleter
    def config_base_path(self):
        del self._config_base_path

    @staticmethod
    def is_concrete_config(path):
        return not path.startswith("@")

    def construct_path(self, path_pattern):
        subst_vars = [var.strip("{}") for var in re.findall(r"{[^{}]+}", path_pattern)]
        for subst_var in subst_vars:
            path_pattern = re.sub(r"{{{}}}".format(subst_var), "{}", path_pattern)
        subst_attrs = [functools.reduce(getattr, subst_var.split("."), self) for subst_var in subst_vars]
        return path_pattern.format(*subst_attrs)

    def set_config(self, path, file_link):
        path = self.construct_path(path)
        self.set_template_source(path, file_link)
        if self.is_concrete_config(path):
            self.add_concrete_config(path)

    def get_abstract_config(self, template_name, path, config_type="templated"):
        constructor = taskexecutor.constructor.Constructor()
        config = constructor.get_conffile(config_type, self.construct_path(path))
        template_source = self.get_template_source(template_name)
        if not template_source:
            raise ConfigConstructionError("No '{0}' config defined for service {1}".format(template_name, self))
        config.template = self.get_config_template(template_source)
        return config

    def get_template_source(self, name):
        return self._template_sources_map.get(name)

    def set_template_source(self, name, value):
        self._template_sources_map[name] = value

    def add_concrete_config(self, path):
        constructor = taskexecutor.constructor.Constructor()
        config = constructor.get_conffile("templated", self.construct_path(path))
        self._concrete_configs_set.add(config)

    def get_concrete_configs_set(self):
        for config in self._concrete_configs_set:
            template_source = self.get_template_source(config.file_path)
            if template_source:
                config.template = self.get_config_template(template_source)
        return self._concrete_configs_set

    def get_concrete_config(self, path):
        path = self.construct_path(path)
        for config in self._concrete_configs_set:
            if config.file_path == path:
                template_source = self.get_template_source(config.file_path)
                if template_source:
                    config.template = self.get_config_template(template_source)
                    return config
        raise ConfigConstructionError("No '{0}' config defined for service {1}".format(path, self))

    def get_config_template(self, template_source):
        with taskexecutor.httpsclient.GitLabClient(**CONFIG.gitlab._asdict()) as gitlab:
            return gitlab.get(template_source)


class WebServer(ConfigurableService, NetworkingService):
    def __init__(self):
        ConfigurableService.__init__(self)
        NetworkingService.__init__(self)
        self._site_template_name = str()
        self._static_base_path = str()
        self._ssl_certs_base_path = str()
        self._site_config_path_pattern = "sites-available/{}.conf"

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
    def static_base_path(self):
        return self._static_base_path

    @static_base_path.setter
    def static_base_path(self, value):
        self._static_base_path = value

    @static_base_path.deleter
    def static_base_path(self):
        del self._static_base_path

    @property
    def ssl_certs_base_path(self):
        return self._ssl_certs_base_path

    @ssl_certs_base_path.setter
    def ssl_certs_base_path(self, value):
        self._ssl_certs_base_path = value

    @ssl_certs_base_path.deleter
    def ssl_certs_base_path(self):
        del self._ssl_certs_base_path

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


class ApplicationServer:
    @property
    def interpreter(self):
        if not hasattr(self, "name") or not self.name or "-" not in self.name:
            return
        match = re.compile(r".+-(?P<name>[a-z]+)(?P<version>\d+)(-(?P<suffix>.+))*").match(self.name)
        name = match.group("name")
        version_major = match.group("version")[0]
        version_minor = match.group("version")[1:]
        suffix = match.group("suffix")
        Interpreter = collections.namedtuple("Interpreter", "name version_major version_minor suffix")
        return Interpreter(name, version_major, version_minor, suffix)


class DatabaseServer(ConfigurableService, NetworkingService, metaclass=abc.ABCMeta):
    def __init__(self):
        ConfigurableService.__init__(self)
        NetworkingService.__init__(self)

    @staticmethod
    @abc.abstractmethod
    def normalize_addrs(addrs_list):
        pass

    @abc.abstractmethod
    def get_user(self, name):
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
