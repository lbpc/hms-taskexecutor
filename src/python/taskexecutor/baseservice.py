import abc
import collections
import functools
import os
import pickle
import re
import time

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.httpsclient
import taskexecutor.utils


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
    _cache = dict()
    _cache_path = "/var/cache/te/config_templates.pkl"

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
        config = taskexecutor.constructor.get_conffile(config_type, self.construct_path(path))
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
        config = taskexecutor.constructor.get_conffile("templated", self.construct_path(path))
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
        if ConfigurableService._cache.get(template_source) \
                and ConfigurableService._cache[template_source]["timestamp"] + 10 > time.time():
            return ConfigurableService._cache[template_source]["value"]
        try:
            with taskexecutor.httpsclient.GitLabClient(**CONFIG.gitlab._asdict()) as gitlab:
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


class WebServer(ConfigurableService, NetworkingService):
    def __init__(self):
        ConfigurableService.__init__(self)
        NetworkingService.__init__(self)
        self._site_template_name = str()
        self._static_base_path = "/var/www/html"
        self._ssl_certs_base_path = "/usr/share/ssl-cert"
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

    def get_ssl_key_pair_files(self, basename):
        cert_file_path = os.path.join(self.ssl_certs_base_path, "{}.pem".format(basename))
        key_file_path = os.path.join(self.ssl_certs_base_path, "{}.key".format(basename))
        cert_file = taskexecutor.constructor.get_conffile("basic", cert_file_path)
        key_file = taskexecutor.constructor.get_conffile("basic", key_file_path)
        return cert_file, key_file


class ArchivableService(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_archive_stream(self, source, params={}):
        pass


class ApplicationServer(ArchivableService):
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

    def get_archive_stream(self, source, params={}):
        stdout, stderr = taskexecutor.utils.exec_command("tar czf - -C {0} {1}".format(params.get("basedir"), source),
                                                         return_raw_streams=True)
        return stdout, stderr


class DatabaseServer(ConfigurableService, NetworkingService, ArchivableService, metaclass=abc.ABCMeta):
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
