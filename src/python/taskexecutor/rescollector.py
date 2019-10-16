import abc
import clamd
import collections
import os
import sys
import time

from taskexecutor.logger import LOGGER
from taskexecutor.config import CONFIG

import taskexecutor.constructor
import taskexecutor.watchdog
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
        LOGGER.debug("Cache size: {}".format(sys.getsizeof(ResCollector._cache)))
        LOGGER.debug("Probing cache for key: {}".format(key))
        LOGGER.debug("Cache record: {}".format(ResCollector._cache.get(key)))
        cached = key in ResCollector._cache.keys() and ResCollector._cache[key]
        expired = False
        if cached:
            expired = (ResCollector._cache[key]["timestamp"] + ttl) < time.time()
        return cached, expired

    def add_property_to_cache(self, key, value):
        LOGGER.debug("Pushing to cache: {}={}".format(key, value))
        ResCollector._cache[key] = {"value": value, "timestamp": time.time()}

    def get_property_from_cache(self, key):
        return ResCollector._cache[key]["value"]

    def ignore_property(self, property_name):
        self._ignored_properties.add(property_name)

    def get(self, cache_ttl=0):
        op_resource = dict()
        properties_set = set(self.resource._asdict().keys())
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
    def __init__(self, resource, service):
        super().__init__(resource, service)
        self._clamd = clamd.ClamdNetworkSocket(CONFIG.clamd.host, CONFIG.clamd.port)

    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.uid)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            LOGGER.debug("Cache hit for property: {}".format(property_name))
            return self.get_property_from_cache(key)
        etc_passwd = taskexecutor.constructor.get_conffile("lines", "/etc/passwd")
        matched_lines = etc_passwd.get_lines("^{}:".format(self.resource.name))
        if property_name == "quotaUsed":
            uid_quota_used_mapping = self.service.get_quota()
            for uid, quota_used_bytes in uid_quota_used_mapping.items():
                LOGGER.debug("UID: {0} quota used: {1} bytes".format(uid, quota_used_bytes))
                self.add_property_to_cache(self.get_cache_key(property_name, uid), quota_used_bytes)
            return uid_quota_used_mapping.get(self.resource.uid) or 0
        elif property_name == "cpuUsed":
            now = time.time()
            cpu_nanosec = self.service.get_cpuacct(self.resource.name)
            cpu_used = dict(at=now, nanoseconds=cpu_nanosec, percents=0)
            prev = self._cache.get(self.get_cache_key("cpuUsed", self.resource.uid))
            if prev:
                period = now - prev["timestamp"]
                delta_seconds = (cpu_nanosec - prev["value"]["nanoseconds"]) / 1000000000
                percents = delta_seconds / period * 100
                cpu_used["percents"] = percents
            self.add_property_to_cache(self.get_cache_key(property_name, self.resource.uid), cpu_used)
            return cpu_used
        elif property_name == "infectedFiles":
            infected_files = list()
            if self.resource.infected and cached:
                infected_files = self.get_property_from_cache(key)
            elif self.get_property("cpuUsed")["percents"] > CONFIG.unix_account.malscan_cpu_threshold:
                for path in taskexecutor.watchdog.ProcessWatchdog.get_workdirs_by_uid(self.resource.uid):
                    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
                    for file in files:
                        if os.path.exists(file) and \
                                        os.stat(file).st_mode & 0o0777 > 0o0 and \
                                        os.stat(file).st_uid == self.resource.uid and \
                                        os.path.getsize(file) < CONFIG.clamd.stream_max_length:
                            with open(file, "rb") as f:
                                try:
                                    scan_res = self._clamd.instream(f)
                                    file = file.encode("utf-8", "replace").decode("utf-8")
                                    LOGGER.debug("Malware scan result for {}: {}".format(file, scan_res))
                                    if "stream" in scan_res.keys() and scan_res["stream"][0] == "FOUND":
                                        infected_files.append(file)
                                except Exception as e:
                                    LOGGER.warning("Failed to scan {}: {}".format(file, e))
                self.add_property_to_cache(key, infected_files)
            return infected_files
        elif property_name in ("name", "uid", "homeDir", "crontab"):
            if len(matched_lines) != 1:
                LOGGER.warning("Cannot determine user {0}, "
                               "lines found in /etc/passwd: {1}".format(self.resource.name, matched_lines))
                return
            name, _, uid, _, _, home_dir, _ = matched_lines[0].split(":")
            CronTask = collections.namedtuple("CronTask", "switchedOn execTimeDescription execTime command")
            crontab = [CronTask(switchedOn=True,
                                execTimeDescription="",
                                execTime=" ".join(s.split(" ")[:5]),
                                command=" ".join(s.split(" ")[5:]))
                       for s in self.service.get_crontab(name).split("\n") if s]
            self.add_property_to_cache(self.get_cache_key("name", self.resource.uid), name)
            self.add_property_to_cache(self.get_cache_key("uid", self.resource.uid), int(uid))
            self.add_property_to_cache(self.get_cache_key("homeDir", self.resource.uid), home_dir)
            self.add_property_to_cache(self.get_cache_key("crontab", self.resource.uid), crontab)
            return {"name": name,
                    "uid": int(uid),
                    "homeDir": home_dir,
                    "crontab": crontab}.get(property_name)


class MailboxCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        maildir_path = self.service.get_maildir_path(self.resource.mailSpool, self.resource.name)
        maildir_size = None
        key = self.get_cache_key(property_name, maildir_path)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key)
        if property_name == "quotaUsed":
            if os.path.exists(os.path.join(maildir_path, "maildirsize")):
                maildir_size = self.service.get_maildir_size(self.resource.mailSpool, self.resource.name)
            else:
                maildir_size = self.service.get_real_maildir_size(self.resource.mailSpool, self.resource.name)
                self.service.create_maildirsize_file(self.resource.mailSpool, self.resource.name,
                                                     maildir_size, self.resource.uid)
            self.add_property_to_cache(key, maildir_size)
            return maildir_size or 0
        elif property_name in ("name", "mailSpool") and os.path.exists(maildir_path) and os.path.isdir(maildir_path):
            self.add_property_to_cache(self.get_cache_key("name", maildir_path), self.resource.name)
            self.add_property_to_cache(self.get_cache_key("mailSpool", maildir_path),
                                       self.service.normalize_spool(self.resource.mailSpool))
            return getattr(self.resource, property_name, None)


class DatabaseUserCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.name)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key) or 0
        if property_name in ("name", "passwordHash", "allowedIPAddresses"):
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
                LOGGER.warning("No database found: {}".format(self.resource.name))
                return
            self.add_property_to_cache(key, database_size)
        elif property_name == "quotaUsed":
            database_names = self.service.get_all_database_names()
            if self.resource.name not in database_names:
                LOGGER.warning("No database found: {}".format(self.resource.name))
                return
            database_size_mapping = self.service.get_all_databases_size()
            for database_name in database_names:
                size = database_size_mapping.get(database_name) or 0
                LOGGER.debug("Database: {0} Size: {1} bytes".format(database_name, size))
                self.add_property_to_cache(self.get_cache_key(property_name, database_name), size)
            return database_size_mapping.get(self.resource.name) or 0
        elif property_name in ("name", "databaseUsers"):
            db_users = list()
            name, users = self.service.get_database(self.resource.name)
            if name:
                self.add_property_to_cache(self.get_cache_key("name", self.resource.name), name)
                for user_name, password_hash, addrs in users:
                    OpDatabaseUser = collections.namedtuple("OpDatabaseUser", "name passwordHash allowedIPAddresses")
                    db_user = next((user for user in self.resource.databaseUsers if user.name == user_name),
                                   OpDatabaseUser(user_name, password_hash, addrs))
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
                if (hasattr(service, "serviceTemplate") and
                        service.serviceTemplate and
                        service.serviceTemplate.serviceType.name.startswith("WEBSITE_")) or \
                    (hasattr(service, "template") and
                     service.template and
                     service.template.resourceType == "WEBSITE" and
                     service.template.__class__.__name__ == "ApplicationServer"):
                    app_server = taskexecutor.constructor.get_opservice(service)
                    config = app_server.get_website_config(self.resource.id)
                    if config.exists:
                        return service.id


class RedirectCollector(ResCollector):
    def get_property(self, property_name, cache_ttl=0):
        if property_name == "serviceId":
            for service in CONFIG.localserver.services:
                if (hasattr(service, "serviceTemplate") and
                        service.serviceTemplate and
                        service.serviceTemplate.serviceType.name == "STAFF_NGINX") or \
                    (hasattr(service, "template") and
                     service.template and
                     service.template.__class__.__name__ == "HttpServer"):
                    app_server = taskexecutor.constructor.get_opservice(service)
                    config = app_server.get_website_config(self.resource.id)
                    if config.exists:
                        return service.id
            return


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
                             "resource-archive": ResourceArchiveCollector,
                             "redirect": RedirectCollector}.get(res_type)
        if not ResCollectorClass:
            raise BuilderTypeError("Unknown resource type: {}".format(res_type))
        return ResCollectorClass
