import abc
import collections
import os
import re

from taskexecutor.config import CONFIG
import taskexecutor.constructor
import taskexecutor.httpsclient


class ConfigValidationError(Exception):
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
    def is_concrete_config(name):
        return False if re.match(r"{.+}", name) else True

    def set_config_from_template_obj(self, template_obj):
        self.set_template_source(template_obj.name, template_obj.fileLink)
        if self.is_concrete_config(template_obj.name):
            self.add_concrete_config(template_obj.name)

    def get_abstract_config(self, template_name, rel_path, config_type="templated"):
        constructor = taskexecutor.constructor.Constructor()
        config = constructor.get_conffile(config_type, os.path.join(self.config_base_path, str(rel_path)))
        config.template = self.get_config_template(self.get_template_source(template_name))
        return config

    def get_template_source(self, name):
        return self._template_sources_map[name]

    def set_template_source(self, name, value):
        self._template_sources_map[name] = value

    def add_concrete_config(self, rel_path):
        constructor = taskexecutor.constructor.Constructor()
        config = constructor.get_conffile("templated", os.path.join(self.config_base_path, str(rel_path)))
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
        raise ConfigValidationError("No such config: {}".format(rel_path))

    def get_config_template(self, template_source):
        with taskexecutor.httpsclient.GitLabClient(**CONFIG.gitlab._asdict()) as gitlab:
            return gitlab.get(template_source)


class DatabaseServer(metaclass=abc.ABCMeta):
    DatabaseUserClass = collections.namedtuple("DatabaseUser", "name passowrdHash allowedIPAddresses")
    DatabaseClass = collections.namedtuple("Database", "name databaseUsers quotaUsed")

    @staticmethod
    @abc.abstractmethod
    def normalize_addrs(addrs_list):
        pass

    @abc.abstractmethod
    def get_user(self, name):
        pass

    @abc.abstractmethod
    def get_database(self, name, calculate_quota_used=False):
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


class QuotableService(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_quota_used(self, op_resource_ids):
        pass
