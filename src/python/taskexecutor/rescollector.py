import abc
import collections
import os
import sys
import time

import clamd

import taskexecutor.constructor as cnstr
import taskexecutor.builtinservice as bs
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.utils import synchronized, asdict
from taskexecutor.watchdog import ProcessWatchdog

__all__ = ['UnixAccountCollector', 'DatabaseUserCollector', 'DatabaseCollector', 'MailboxCollector', 'WebsiteCollector',
           'SslCertificateCollector', 'ServiceCollector', 'ResourceArchiveCollector', 'RedirectCollector']


class ResCollector(metaclass=abc.ABCMeta):
    _cache = dict()

    def __init__(self, resource, service):
        super().__init__()
        self.resource = resource
        self.service = service
        self.extra_services = None
        self._ignored_properties = set()

    @abc.abstractmethod
    def get_property(self, property_name, cache_ttl=0):
        pass

    def get_cache_key(self, property_name, op_res_id):
        return f'{self.__class__.__name__}.{op_res_id}.{property_name}'

    @property
    @abc.abstractmethod
    def necessary_properties(self):
        pass

    @staticmethod
    def check_cache(key, ttl):
        LOGGER.debug(f'Cache size: {sys.getsizeof(ResCollector._cache)}')
        LOGGER.debug(f'Probing cache for key: {key}')
        LOGGER.debug(f'Cache record: {ResCollector._cache.get(key)}')
        cached = ResCollector._cache.get(key)
        expired = False
        if cached:
            expired = (cached['timestamp'] + ttl) < time.time()
        return bool(cached), expired

    @staticmethod
    def add_property_to_cache(key, value):
        LOGGER.debug(f'Pushing to cache: {key}={value}')
        ResCollector._cache[key] = {'value': value, 'timestamp': time.time()}

    @staticmethod
    def get_property_from_cache(key):
        return ResCollector._cache[key]['value']

    def ignore_property(self, property_name):
        self._ignored_properties.add(property_name)

    def get(self, cache_ttl=0):
        op_resource = dict()
        properties_set = set(asdict(self.resource).keys())
        properties_set.difference_update(self._ignored_properties)
        start_collecting_time = time.time()
        for property_name in properties_set:
            op_resource[property_name] = self.get_property(property_name, cache_ttl=cache_ttl)
            cache_ttl += time.time() - start_collecting_time
        if not any(filter(lambda i: i[0] in self.necessary_properties and i[1] is not None, op_resource.items())):
            LOGGER.warning(f"No resource available, "
                           f"ID: {getattr(self.resource, 'id', None)}, "
                           f"name: {self.resource.name}")
            return
        return collections.namedtuple('OpResource', op_resource.keys())(*op_resource.values())


class UnixAccountCollector(ResCollector):
    def __init__(self, resource, service):
        super().__init__(resource, service)
        self._clamd = clamd.ClamdNetworkSocket(CONFIG.clamd.host, CONFIG.clamd.port)

    @property
    def necessary_properties(self):
        return ('name', 'uid')

    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.uid)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            LOGGER.debug(f'Cache hit for property: {property_name}')
            return self.get_property_from_cache(key)
        if property_name == 'quotaUsed':
            uid_quota_used_mapping = self.service.get_quota()
            for uid, quota_used_bytes in uid_quota_used_mapping.items():
                LOGGER.debug(f'UID: {uid} quota used: {quota_used_bytes} bytes')
                self.add_property_to_cache(self.get_cache_key(property_name, uid), quota_used_bytes)
            return uid_quota_used_mapping.get(self.resource.uid) or 0
        elif property_name == 'cpuUsed':
            now = time.time()
            cpu_nanosec = self.service.get_cpuacct(self.resource.name)
            cpu_used = dict(at=now, nanoseconds=cpu_nanosec, percents=0)
            prev = self._cache.get(self.get_cache_key('cpuUsed', self.resource.uid))
            if prev:
                period = now - prev['timestamp']
                delta_seconds = (cpu_nanosec - prev['value']['nanoseconds']) / 1000000000
                percents = delta_seconds / period * 100
                cpu_used['percents'] = percents
            self.add_property_to_cache(self.get_cache_key(property_name, self.resource.uid), cpu_used)
            return cpu_used
        elif property_name == 'infectedFiles':
            infected_files = list()
            if self.resource.infected and cached:
                infected_files = self.get_property_from_cache(key)
            elif self.get_property('cpuUsed')['percents'] > CONFIG.unix_account.malscan_cpu_threshold:
                for path in ProcessWatchdog.get_workdirs_by_uid(self.resource.uid):
                    files = [os.path.join(path, f) for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
                    for file in files:
                        if os.path.exists(file) and \
                                os.stat(file).st_mode & 0o0777 > 0o0 and \
                                os.stat(file).st_uid == self.resource.uid and \
                                os.path.getsize(file) < CONFIG.clamd.stream_max_length:
                            with open(file, 'rb') as f:
                                try:
                                    scan_res = self._clamd.instream(f)
                                    file = file.encode('utf-8', 'replace').decode('utf-8')
                                    LOGGER.debug(f'Malware scan result for {file}: {scan_res}')
                                    if 'stream' in scan_res and scan_res['stream'][0] == 'FOUND':
                                        infected_files.append(file)
                                except Exception as e:
                                    LOGGER.warning(f'Failed to scan {file}: {e}')
                self.add_property_to_cache(key, infected_files)
            return infected_files
        elif property_name == 'crontab':
            CronTask = collections.namedtuple('CronTask', 'switchedOn execTimeDescription execTime command')
            crontab = [CronTask(switchedOn=True,
                                execTimeDescription='',
                                execTime=' '.join(s.split(' ')[:5]),
                                command=' '.join(s.split(' ')[5:]))
                       for s in self.extra_services.cron.get_crontab(self.resource.name).split('\n') if s]
            self.add_property_to_cache(self.get_cache_key(property_name, self.resource.uid), crontab)
            return crontab
        elif property_name in ('name', 'uid', 'homeDir'):
            user = None
            try:
                user = self.service.get_user(self.resource.name)
            except bs.InconsistentData as e:
                LOGGER.warning(e)
            if not user: return
            self.add_property_to_cache(self.get_cache_key('name', self.resource.uid), user.name)
            self.add_property_to_cache(self.get_cache_key('uid', self.resource.uid), user.uid)
            self.add_property_to_cache(self.get_cache_key('homeDir', self.resource.uid), user.home)
            return {'name': user.name,
                    'uid': user.uid,
                    'homeDir': user.home}.get(property_name)


class MailboxCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ('name', 'mailSpool')

    def get_property(self, property_name, cache_ttl=0):
        maildir_path = self.service.get_maildir_path(self.resource.mailSpool, self.resource.name)
        maildir_size = None
        key = self.get_cache_key(property_name, maildir_path)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key)
        if property_name == 'quotaUsed':
            if os.path.exists(os.path.join(maildir_path, 'maildirsize')):
                maildir_size = self.service.get_maildir_size(self.resource.mailSpool, self.resource.name)
            else:
                maildir_size = self.service.get_real_maildir_size(self.resource.mailSpool, self.resource.name)
                self.service.create_maildirsize_file(self.resource.mailSpool, self.resource.name,
                                                     maildir_size, self.resource.uid)
            self.add_property_to_cache(key, maildir_size)
            return maildir_size or 0
        elif property_name in ('name', 'mailSpool') and os.path.exists(maildir_path) and os.path.isdir(maildir_path):
            self.add_property_to_cache(self.get_cache_key('name', maildir_path), self.resource.name)
            self.add_property_to_cache(self.get_cache_key('mailSpool', maildir_path),
                                       self.service.normalize_spool(self.resource.mailSpool))
            return getattr(self.resource, property_name, None)


class DatabaseUserCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ('name',)

    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.name)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key) or 0
        if property_name in ('name', 'passwordHash', 'allowedIPAddresses'):
            name, password_hash, addrs = self.service.get_user(self.resource.name)
            if name:
                self.add_property_to_cache(self.get_cache_key('name', self.resource.name), name)
                self.add_property_to_cache(self.get_cache_key('passwordHash', self.resource.name), password_hash)
                self.add_property_to_cache(self.get_cache_key('allowedIPAddresses', self.resource.name), addrs or [])
                return {'name': name,
                        'passwordHash': password_hash,
                        'allowedIPAddresses': addrs or []}.get(property_name)


class DatabaseCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ('name',)

    @synchronized
    def get_property(self, property_name, cache_ttl=0):
        key = self.get_cache_key(property_name, self.resource.name)
        cached, expired = self.check_cache(key, cache_ttl)
        if cached and not expired:
            return self.get_property_from_cache(key)
        if property_name == 'quotaUsed' and not cache_ttl:
            database_size = self.service.get_database_size(self.resource.name)
            if isinstance(database_size, None):
                LOGGER.warning(f'No database found: {self.resource.name}')
                return
            self.add_property_to_cache(key, database_size)
        elif property_name == 'quotaUsed':
            database_names = self.service.get_all_database_names()
            if self.resource.name not in database_names:
                LOGGER.warning(f'No database found: {self.resource.name}')
                return
            database_size_mapping = self.service.get_all_databases_size()
            for database_name in database_names:
                size = database_size_mapping.get(database_name) or 0
                LOGGER.debug(f'Database: {database_name} Size: {size} bytes')
                self.add_property_to_cache(self.get_cache_key(property_name, database_name), size)
            return database_size_mapping.get(self.resource.name) or 0
        elif property_name in ('name', 'databaseUsers'):
            db_users = list()
            name, users = self.service.get_database(self.resource.name)
            if name:
                self.add_property_to_cache(self.get_cache_key('name', self.resource.name), name)
                for user_name, password_hash, addrs in users:
                    OpDatabaseUser = collections.namedtuple('OpDatabaseUser', 'name passwordHash allowedIPAddresses')
                    db_user = next((user for user in self.resource.databaseUsers if user.name == user_name),
                                   OpDatabaseUser(user_name, password_hash, addrs))
                    collected_db_user = DatabaseUserCollector(db_user, self.service).get()
                    if collected_db_user:
                        db_users.append(collected_db_user)
                self.add_property_to_cache(self.get_cache_key('databaseUsers', self.resource.name), db_users)
                return {'name': name,
                        'databaseUsers': db_users}.get(property_name)


class WebsiteCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ('serviceId',)

    def get_property(self, property_name, cache_ttl=0):
        if property_name == 'serviceId':
            for service in cnstr.get_application_servers():
                for config in service.get_website_configs(self.resource):
                    if config.exists: return service.spec.id


class RedirectCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ('serviceId',)

    def get_property(self, property_name, cache_ttl=0):
        if property_name == 'serviceId':
            http_server = cnstr.get_http_proxy_service()
            if http_server and len(http_server.get_website_configs(self.resource)) > 0:
                return http_server.spec.id


class SslCertificateCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ()

    def get_property(self, property_name, cache_ttl=0):
        return


class ServiceCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ('name',)

    def get_property(self, property_name, cache_ttl=0):
        if property_name == 'name' and self.service:
            return f'{self.service.name}@{CONFIG.hostname}'


class ResourceArchiveCollector(ResCollector):
    @property
    def necessary_properties(self):
        return ()

    def get_property(self, property_name, cache_ttl=0):
        return
