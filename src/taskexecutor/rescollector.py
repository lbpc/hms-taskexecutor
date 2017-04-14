import abc
import collections
import os
import time

from taskexecutor.logger import LOGGER
from taskexecutor.config import CONFIG

import taskexecutor.constructor
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
        self._ignored_properties = set()
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

    @abc.abstractmethod
    def get_property(self, property_name, cache_ttl=0):
        pass

    def get_cache_key(self, property_name, op_res_id):
        return "{0}.{1}.{2}".format(self.__class__.__name__, op_res_id, property_name)

    def check_cache(self, key, ttl):
        cached = key in ResCollector._cache.keys() and ResCollector._cache[key]
        expired = False
        if cached:
            expired = (ResCollector._cache[key]["timestamp"] + ttl) < time.time()
        return cached, expired

    def add_property_to_cache(self, key, value):
        ResCollector._cache[key] = {"value": value, "timestamp": time.time()}

    def get_property_from_cache(self, key):
        return ResCollector._cache[key]["value"]

    def ignore_property(self, property_name):
        self._ignored_properties.add(property_name)

    def get(self, cache_ttl=0):
        op_resource = dict()
        properties_set = set(vars(self.resource).keys())
        properties_set.difference_update(self._ignored_properties)
        start_collecting_time = time.time()
        for property_name in properties_set:
            op_resource[property_name] = self.get_property(property_name, cache_ttl=cache_ttl)
            cache_ttl += time.time() - start_collecting_time
        if not any(op_resource.values()):
            LOGGER.warning("No resource available, ID: {0}, name: {1}".format(getattr(self.resource, "id", None),
                                                                              self.resource.name))
            return
        return collections.namedtuple("OpResource", op_resource.keys())(*op_resource.values())


class UnixAccountCollector(ResCollector):
    @taskexecutor.utils.synchronized
    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.uid)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key)
        if property_name == "quotaUsed":
            uid_quota_used_mapping = self.service.get_quota()
            for uid, quota_used_bytes in uid_quota_used_mapping.items():
                LOGGER.debug("UID: {0} quota used: {1} bytes".format(uid, quota_used_bytes))
                self.add_property_to_cache(self.get_cache_key(property_name, uid), quota_used_bytes)
            return uid_quota_used_mapping.get(self.resource.uid)
        else:
            etc_passwd = taskexecutor.constructor.get_conffile("lines", "/etc/passwd")
            matched_lines = etc_passwd.get_lines("^{}:".format(self.resource.name))
            if len(matched_lines) != 1:
                LOGGER.warning("Cannot determine user {0}, "
                               "lines found in /etc/passwd: {1}".format(self.resource.name, matched_lines))
                return
            name, _, uid, _, _, home_dir, _ = matched_lines[0].split(":")
            self.add_property_to_cache(self.get_cache_key("name", self.resource.uid), name)
            self.add_property_to_cache(self.get_cache_key("uid", self.resource.uid), uid)
            self.add_property_to_cache(self.get_cache_key("homeDir", self.resource.uid), home_dir)
            return {"name": name,
                    "uid": uid,
                    "homeDir": home_dir}.get(property_name)


class MailboxCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        maildir_path = os.path.join(str(self.resource.mailSpool), str(self.resource.name))
        key = self.get_cache_key(property_name, maildir_path)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key)
        if property_name == "quotaUsed":
            maildir_size = self.service.get_maildir_size(maildir_path)
            if maildir_size:
                self.add_property_to_cache(key, maildir_size)
                return maildir_size


class DatabaseUserCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.name)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key)
        name, password_hash, addrs = self.service.get_user(self.resource.name)
        if name:
            self.add_property_to_cache(self.get_cache_key("name", self.resource.name), name)
            self.add_property_to_cache(self.get_cache_key("passwordHash", self.resource.name), password_hash)
            self.add_property_to_cache(self.get_cache_key("allowedIPAddresses", self.resource.name), addrs or [])
            return {"name": name,
                    "passwordHash": password_hash,
                    "allowedIPAddresses": addrs or []}.get(property_name)


class DatabaseCollector(ResCollector):
    @taskexecutor.utils.synchronized
    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.name)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key)
        if property_name == "quotaUsed" and not cache_ttl:
            database_size = self.service.get_database_size(self.resource.name)
            if isinstance(database_size, None):
                return
            self.add_property_to_cache(key, database_size)
        elif property_name == "quotaUsed":
            database_size_mapping = self.service.get_all_databases_size()
            for database_name, size in database_size_mapping.items():
                LOGGER.debug("Database: {0} Size: {1} bytes".format(database_name, size))
                self.add_property_to_cache(self.get_cache_key(property_name, database_name), size)
            return database_size_mapping.get(self.resource.name)
        elif property_name in ("name", "databaseUsers"):
            db_users = list()
            name, users = self.service.get_database(self.resource.name)
            if name:
                self.add_property_to_cache(self.get_cache_key("name", self.resource.name), name)
                for user_name, password_hash, addrs in users:
                    OpDatabaseUser = collections.namedtuple("OpDatabaseUser", "name passwordHash allowedIPAddresses")
                    db_user = next((user for user in self.resource.databaseUsers if user.name == user_name),
                                   OpDatabaseUser(name, password_hash, addrs))
                    collected_db_user = DatabaseUserCollector(db_user, self.service).get()
                    if collected_db_user:
                        db_users.append(collected_db_user)
                self.add_property_to_cache(self.get_cache_key("databaseUsers", self.resource.name), db_users)
                return {"name": name,
                        "databaseUsers": db_users}.get(property_name)


class WebsiteCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        if property_name == "serviceId":
            for service in CONFIG.localserver.services:
                if service.serviceTemplate.serviceType.name.startswith("WEBSITE_"):
                    app_server = taskexecutor.constructor.get_opservice(service)
                    config = app_server.get_website_config(self.resource.id)
                    if config.exists:
                        return service.id


class SslCertificateCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        return


class ServiceCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        if property_name == "name" and self.service:
            return "{0}@{1}".format(self.service.name, CONFIG.hostname)


class ResourceArchiveCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        return


class Builder:
    def __new__(cls, res_type):
        ResCollectorClass = {"unix-account": UnixAccountCollector,
                             "database-user": DatabaseUserCollector,
                             "database": DatabaseCollector,
                             "mailbox": MailboxCollector,
                             "website": WebsiteCollector,
                             "ssl-certificate": SslCertificateCollector,
                             "service": ServiceCollector,
                             "resource-archive": ResourceArchiveCollector}.get(res_type)
        if not ResCollectorClass:
            raise BuilderTypeError("Unknown resource type: {}".format(res_type))
        return ResCollectorClass
