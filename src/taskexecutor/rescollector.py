import abc
import collections
import json
import os
import time
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.dbclient
import taskexecutor.httpsclient
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class ResCollector(metaclass=abc.ABCMeta):
    _cache = dict()

    def __init__(self, resource, service):
        super().__init__()
        self._resource = None
        self._service = None
        self._extra_services = None
        self.resource = resource
        self.service = service

    @property
    def resource(self):
        return self._resource

    @resource.setter
    def resource(self, value):
        self._resource = value

    @resource.deleter
    def resource(self):
        del self._resource

    @property
    def service(self):
        return self._service

    @service.setter
    def service(self, value):
        self._service = value

    @service.deleter
    def service(self):
        del self._service

    @property
    def extra_services(self):
        return self._extra_services

    @extra_services.setter
    def extra_services(self, value):
        self._extra_services = value

    @extra_services.deleter
    def extra_services(self):
        del self._extra_services

    @staticmethod
    def initialize_resource_cache(res_type):
        if res_type not in ResCollector._cache.keys():
            ResCollector._cache[res_type] = collections.defaultdict(dict)

    @staticmethod
    def check_cache(res_type, res_key, property_name, ttl):
        cached = ResCollector._cache[res_type][res_key] and \
                 property_name in ResCollector._cache[res_type][res_key].keys()
        expired = False
        if cached:
            expired = (ResCollector._cache[res_type][res_key][property_name]["timestamp"] + ttl) < time.time()
        return cached, expired

    @staticmethod
    def add_property_to_cache(res_type, res_key, property_name, value):
        ResCollector._cache[res_type][res_key][property_name] = {"value": value, "timestamp": time.time()}

    @staticmethod
    def get_property_from_cache(res_type, res_key, property_name):
        return ResCollector._cache[res_type][res_key][property_name]["value"]

    @abc.abstractmethod
    def get_property(self, property_name, cache_ttl=0):
        pass

    def get(self, cache_ttl=0):
        op_resource = dict()
        properties_list = vars(self.resource).keys()
        for property_name in properties_list:
            op_resource[property_name] = \
                self.get_property(property_name, cache_ttl) or getattr(self.resource, property_name)
        return collections.namedtuple("OpResource", op_resource.keys())(*op_resource.values())


class UnixAccountCollector(ResCollector):
    def __init__(self, resource, service):
        super().__init__(resource, service)
        self.initialize_resource_cache("unix-account")

    def get_property(self, property_name, cache_ttl=0):
        cached, expired = self.check_cache("unix-account", self.resource.uid, property_name, cache_ttl)
        if not cached or expired:
            if property_name == "quotaUsed":
                for uid, quota_used_bytes in self.service.get_quota().items():
                    self.add_property_to_cache("unix-account", uid, property_name, quota_used_bytes)
            else:
                return
        return self.get_property_from_cache("unix-account", self.resource.uid, property_name)


class MailboxCollector(ResCollector):
    def __init__(self, resource, service):
        super().__init__(resource, service)
        self.initialize_resource_cache("mailbox")

    def get_property(self, property_name, cache_ttl=0):
        cached, expired = self.check_cache("mailbox", self.resource.uid, property_name, cache_ttl)
        maildir_path = os.path.join(self.resource.mailSpool, self.resource.name)
        if not cached or expired:
            if property_name == "quotaUsed":
                maildir_size = self.service.get_maildir_size(maildir_path)
                self.add_property_to_cache("mailbox", maildir_path, property_name, maildir_size)
            else:
                return
        return self.get_property_from_cache("mailbox", maildir_path, property_name)


class DatabaseUserCollector(ResCollector):
    def __init__(self, resource, service):
        super().__init__(resource, service)
        self.initialize_resource_cache("database-user")

    def get_property(self, property_name, cache_ttl=0):
        cached, expired = self.check_cache("database-user", self.resource.name, property_name, cache_ttl)
        if not cached or expired:
            name, password_hash, addrs = self.service.get_user(self.resource.name)
            self.add_property_to_cache("database-user", self.resource.name, "name", name)
            self.add_property_to_cache("database-user", self.resource.name, "passowrdHash", password_hash)
            self.add_property_to_cache("database-user", self.resource.name, "allowedIPAddresses", addrs)
        if property_name not in ("name", "passowrdHash" "allowedIPAddresses"):
            return
        return self.get_property_from_cache("database-user", self.resource.name, property_name)


class DatabaseCollector(ResCollector):
    def __init__(self, resource, service):
        super().__init__(resource, service)
        self.initialize_resource_cache("database")

    def get_property(self, property_name, cache_ttl=0):
        cached, expired = self.check_cache("database", self.resource.name, property_name, cache_ttl)
        if not cached or expired:
            if property_name == "quotaUsed":
                database_size = self.service.get_database_size(self.resource.name)
                self.add_property_to_cache("database", self.resource.name, property_name, database_size)
            elif property_name == "name" or property_name == "databaseUsers":
                db_users = list()
                name, users = self.service.get_database(self.resource.name)
                self.add_property_to_cache("database", self.resource.name, "name", name)
                for user_name, password_hash, addrs in users:
                    OpDatabaseUser = collections.namedtuple("OpResource", "name passowrdHash allowedIPAddresses")
                    db_user = None
                    for user in self.resource.databaseUsers:
                        if user.name == user_name:
                            db_user = user
                    if not db_user:
                        db_user = OpDatabaseUser(name, password_hash, addrs)
                    user_collector = DatabaseUserCollector(db_user, self.service)
                    db_users.append(user_collector.get())
                self.add_property_to_cache("database", self.resource.name, "databaseUsers", db_users)
            else:
                return
        return self.get_property_from_cache("database", self.resource.name, property_name)


class Builder:
    def __new__(cls, res_type):
        if res_type == "unix-account":
            return UnixAccountCollector
        elif res_type == "database-user":
            return DatabaseUserCollector
        elif res_type == "database":
            return DatabaseCollector
        elif res_type == "mailbox":
            return MailboxCollector
        else:
            raise BuilderTypeError("Unknown resource type: {}".format(res_type))
