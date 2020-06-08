import abc
import collections
import os
import shutil
import urllib.parse

import taskexecutor.constructor as cnstr
from taskexecutor.config import CONFIG
from taskexecutor.ftpclient import FTPClient
from taskexecutor.logger import LOGGER
from taskexecutor.opservice import ServiceStatus, HttpServer, ConfigurableService, Apache
from taskexecutor.utils import asdict, synchronized, to_snake_case, rgetattr
from taskexecutor.watchdog import ProcessWatchdog

__all__ = ['UnixAccountProcessor', 'DatabaseUserProcessor', 'DatabaseProcessor', 'MailboxProcessor', 'WebSiteProcessor',
           'SslCertificateProcessor', 'ServiceProcessor', 'ResourceArchiveProcessor', 'RedirectProcessor']


class ResourceValidationError(Exception):
    pass


class ResourceProcessingError(Exception):
    pass


class ResProcessor(metaclass=abc.ABCMeta):
    def __init__(self, resource, service, params):
        super().__init__()
        self.resource = resource
        self.service = service
        self.params = params
        self.extra_services = None
        self.op_resource = None

    @abc.abstractmethod
    def create(self):
        pass

    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def delete(self):
        pass

    def _process_data(self, src_uri, dst_uri, extra_postproc_args=None):
        extra_postproc_args = extra_postproc_args or {}
        datafetcher = cnstr.get_datafetcher(src_uri, dst_uri, self.params.get('dataSourceParams'))
        datafetcher.fetch()
        data_postprocessor_type = self.params.get('dataPostprocessorType')
        data_postprocessor_args = self.params.get('dataPostprocessorArgs', {})
        if data_postprocessor_type:
            data_postprocessor_args.update(extra_postproc_args)
            LOGGER.debug(f"Processing {self.resource.name} data with '{data_postprocessor_type}', "
                         f"arguments: {data_postprocessor_args}")
            postprocessor = cnstr.get_datapostprocessor(data_postprocessor_type, data_postprocessor_args)
            self.params['dataPostprocessorOutput'] = postprocessor.process()

    def __str__(self):
        return '{0}(resource=(name={1.name}, id={1.id}))'.format(self.__class__.__name__, self.resource)


class UnixAccountProcessor(ResProcessor):
    @synchronized
    def create(self):
        if self.op_resource:
            LOGGER.warning(f'User {self.resource.name} already exists, updating')
            self.update()
            return
        LOGGER.info(f'Adding user {self.resource.name} to system')
        shell = {True: self.service.default_shell, False: self.service.disabled_shell}[self.resource.switchedOn]
        self.service.create_user(self.resource.name,
                                 self.resource.uid,
                                 self.resource.homeDir,
                                 self.resource.passwordHash,
                                 shell,
                                 'Hosting account,,,,'
                                 'UnixAccount(id={0.id}, '
                                 'accountId={0.accountId}, '
                                 'writable={0.writable})'.format(self.resource),
                                 CONFIG.unix_account.groups)
        try:
            LOGGER.info('Setting quota for user {0.name}: {0.quota} bytes'.format(self.resource))
            self.service.set_quota(self.resource.uid, self.resource.quota)
        except Exception:
            LOGGER.error(f'Setting quota failed for user {self.resource.name}')
            self.service.delete_user(self.resource.name)
            raise
        if len(self.resource.crontab) > 0:
            if not self.extra_services.cron:
                raise ResourceProcessingError(f'Cannot create crontab for user {self.resource.name}, no cron service')
            self.extra_services.cron.create_crontab(self.resource.name,
                                                    filter(lambda t: t.switchedOn, self.resource.crontab))
        if getattr(self.resource, 'keyPair', None):
            LOGGER.info(f'Creating authorized_keys for user {self.resource.name}')
            self.service.create_authorized_keys(self.resource.keyPair.publicKey,
                                                self.resource.uid, self.resource.homeDir)
        if 'dataSourceParams' not in self.params:
            self.params['dataSourceParams'] = {}
        self.params['dataSourceParams']['ownerUid'] = self.params['dataSourceParams'].get('ownerUid', self.resource.uid)
        data_dest_uri = self.params.get('datadestinationUri', f'file://{self.resource.homeDir}')
        data_source_uri = self.params.get('datasourceUri', data_dest_uri)
        self._process_data(data_source_uri, data_dest_uri, {'dataType': 'directory', 'path': self.resource.homeDir})

    @synchronized
    def update(self):
        if self.op_resource:
            switched_on = self.resource.switchedOn and not self.params.get('forceSwitchOff')
            LOGGER.info('Modifying user {0.name}'.format(self.resource))
            if self.resource.uid != self.op_resource.uid:
                LOGGER.warning('UnixAccount {0} UID changed from {1} '
                               'to: {2}'.format(self.resource.name, self.op_resource.uid, self.resource.uid))
                self.service.change_uid(self.resource.name, self.resource.uid)
            self.service.set_shell(self.resource.name,
                                   {True: self.service.default_shell, False: None}[switched_on])
            if not self.extra_services.mta:
                raise ResourceProcessingError(f'Cannot update sendmail for user {self.resource.name}, no MTA service')
            if self.resource.sendmailAllowed:
                self.extra_services.mta.enable_sendmail(self.resource.uid)
            else:
                self.extra_services.mta.disable_sendmail(self.resource.uid)
            if not self.resource.writable:
                LOGGER.info('Disabling writes by setting quota=quotaUsed for user {0.name} '
                            '(quotaUsed={0.quotaUsed})'.format(self.resource))
                self.service.set_quota(self.resource.uid, self.resource.quotaUsed)
            else:
                LOGGER.info('Setting quota for user {0.name}: {0.quota} bytes'.format(self.resource))
                self.service.set_quota(self.resource.uid, self.resource.quota)
            if not 'dataSourceParams' in self.params.keys():
                self.params['dataSourceParams'] = {}
            self.params['dataSourceParams']['ownerUid'] = self.params['dataSourceParams'].get('ownerUid',
                                                                                              self.resource.uid)
            data_dest_uri = self.params.get('datadestinationUri', 'file://{}'.format(self.resource.homeDir))
            data_source_uri = self.params.get('datasourceUri', data_dest_uri)
            self._process_data(data_source_uri, data_dest_uri, {'dataType': 'directory', 'path': self.resource.homeDir})
            if hasattr(self.resource, 'keyPair') and self.resource.keyPair:
                LOGGER.info('Creating authorized_keys for user {0.name}'.format(self.resource))
                self.service.create_authorized_keys(self.resource.keyPair.publicKey,
                                                    self.resource.uid, self.resource.homeDir)
            if not self.extra_services.cron:
                raise ResourceProcessingError(f'Cannot process crontab for user {self.resource.name}, no cron service')
            if len(self.resource.crontab) > 0 and switched_on:
                self.extra_services.cron.create_crontab(self.resource.name,
                                                        [task for task in self.resource.crontab if task.switchedOn])
            else:
                self.extra_services.cron.delete_crontab(self.resource.name)
            self.service.set_comment(self.resource.name, 'Hosting account,,,,'
                                                         'UnixAccount(id={0.id}, '
                                                         'accountId={0.accountId}, '
                                                         'writable={0.writable})'.format(self.resource))
            if not self.resource.infected:
                ProcessWatchdog.get_uids_queue().put(-self.resource.uid)
            LOGGER.info("Creating 'logs' directory")
            os.makedirs(os.path.join(self.resource.homeDir, 'logs'), mode=0o755, exist_ok=True)
            if not switched_on:
                self.service.kill_user_processes(self.resource.name)
        else:
            LOGGER.warning(f'UnixAccount {self.resource.name} not found, creating')
            self.create()

    @synchronized
    def delete(self):
        self.service.kill_user_processes(self.resource.name)
        self.service.delete_user(self.resource.name)


class WebSiteProcessor(ResProcessor):
    @property
    def _without_reload(self):
        return self.params.get('required_for', [None])[0] == 'service' or \
               'appscat' in self.params.get('provider', [None])

    def _build_vhost_obj_list(self):
        vhosts = list()
        non_ssl_domains = list()
        res_dict = asdict(self.resource)
        for domain in (d for d in self.resource.domains if d.switchedOn):
            if domain.sslCertificate and domain.sslCertificate.switchedOn:
                res_dict['domains'] = [domain, ]
                vhosts.append(collections.namedtuple('VHost', res_dict.keys())(*res_dict.values()))
            else:
                domain_dict = asdict(domain)
                if 'sslCertificate' in domain_dict.keys():
                    del domain_dict['sslCertificate']
                non_ssl_domains.append(collections.namedtuple('Domain', domain_dict.keys())(*domain_dict.values()))
        if non_ssl_domains:
            res_dict['domains'] = non_ssl_domains
            vhosts.append(collections.namedtuple('VHost', res_dict.keys())(*res_dict.values()))
        return vhosts

    @synchronized
    def create(self):
        self.params.update(app_server_name=self.service.name,
                           subdomains_document_root='/'.join(str(self.resource.documentRoot).split('/')[:-1]))
        vhosts_list = self._build_vhost_obj_list()
        home_dir = os.path.normpath(str(self.resource.unixAccount.homeDir))
        document_root = os.path.normpath(str(self.resource.documentRoot))
        document_root_abs = os.path.join(home_dir, document_root)
        opcache_root = os.path.join('/opcache', self.resource.id)
        if os.path.exists(opcache_root):
            shutil.rmtree(opcache_root, ignore_errors=True)
        for directory in (os.path.join(home_dir, 'logs'), document_root_abs, opcache_root):
            if not os.path.islink(directory):
                os.makedirs(directory, mode=0o755, exist_ok=True)
            else:
                LOGGER.warning(f'{directory} is symbolic link')
        for directory in map(lambda d: os.path.join(home_dir, d),
                             ['/'.join(document_root.split('/')[0:i + 1])
                              for i, d in enumerate(document_root.split('/'))]):
            if os.path.exists(directory):
                os.chown(directory, self.resource.unixAccount.uid, self.resource.unixAccount.uid)
            else:
                LOGGER.warning(f'{directory} does not exist')
        os.chown(opcache_root, self.resource.unixAccount.uid, self.resource.unixAccount.uid)
        services = []
        if self.params.get('oldHttpProxyIp') != self.extra_services.http_proxy.socket.http.address:
            services.append(self.service)
        services.append(self.extra_services.http_proxy)
        LOGGER.debug('Configuring services: {}'.format(', '.join((s.name for s in services))))
        for service in services:
            configs = service.get_website_configs(self.resource)
            for each in configs:
                each.render_template(service=service, vhosts=vhosts_list, params=self.params)
                each.write()
            if not self._without_reload:
                try:
                    service.reload()
                except:
                    for each in configs: each.revert()
                    raise
            for each in configs: each.confirm()
        data_dest_uri = self.params.get('datadestinationUri', f'file://{document_root_abs}')
        data_source_uri = self.params.get('datasourceUri') or data_dest_uri
        if 'dataSourceParams' not in self.params: self.params['dataSourceParams'] = {}
        self.params['dataSourceParams']['ownerUid'] = self.params['dataSourceParams'].get('ownerUid',
                                                                                          self.resource.unixAccount.uid)
        given_postproc_args = self.params.get('dataPostprocessorArgs', {})
        cwd = given_postproc_args.get('cwd', document_root_abs)
        if self.params.get('extendedAction') in ('SHELL', 'INSTALL'): cwd = self.resource.unixAccount.homeDir   # XXX
        env = given_postproc_args.get('env', {})
        command = given_postproc_args.get('command')
        env['DOCUMENT_ROOT'] = document_root_abs
        domain = next((d for d in self.resource.domains if d.name == env.get('DOMAIN_NAME')), self.resource.domains[0])
        env['DOMAIN_NAME'] = domain.name.encode('idna').decode()
        env['PROTOCOL'] = 'https' if domain.sslCertificate and domain.sslCertificate.switchedOn else 'http'
        postproc_args = dict(cwd=cwd,
                             hosts={env['DOMAIN_NAME']: self.extra_services.http_proxy.socket.http.address},
                             uid=self.resource.unixAccount.uid,
                             dataType='directory',
                             env=env,
                             command=command)
        self._process_data(data_source_uri, data_dest_uri, postproc_args)

    @synchronized
    def update(self):
        if not self.resource.switchedOn:
            for service in (self.service, self.extra_services.http_proxy):
                for each in service.get_website_configs(self.resource): each.delete()
                if not self._without_reload:
                    service.reload()
        else:
            self.create()
            if self.extra_services.old_app_server and (self.extra_services.old_app_server.name != self.service.name or
                                                       type(self.extra_services.old_app_server) != type(self.service)):
                LOGGER.info(f'Removing config from old application server {self.extra_services.old_app_server.name}')
                for each in self.extra_services.old_app_server.get_website_configs(self.resource): each.delete()
                if not self._without_reload:
                    self.extra_services.old_app_server.reload()

    @synchronized
    def delete(self):
        shutil.rmtree(os.path.join('/opcache', self.resource.id), ignore_errors=True)
        for service in (self.extra_services.http_proxy, self.service):
            for each in service.get_website_configs(self.resource): each.delete()
            service.reload()


class SslCertificateProcessor(ResProcessor):
    @synchronized
    def create(self):
        cert_file, key_file = self.service.get_ssl_key_pair_files(self.resource.name)
        cert_file.body = self.resource.cert + self.resource.chain or ''
        key_file.body = self.resource.key
        cert_file.save()
        key_file.save()

    def update(self):
        self.create()

    def delete(self):
        pass


class MailboxProcessor(ResProcessor):
    def create(self):
        self.service.create_maildir(self.resource.mailSpool, self.resource.name, self.resource.uid)

    def update(self):
        if not self.op_resource:
            self.create()

    def delete(self):
        self.service.delete_maildir(self.resource.mailSpool, self.resource.name)


class DatabaseUserProcessor(ResProcessor):
    def _apply_restrictions(self):
        if self.resource.maxCpuTimePerSecond and float(self.resource.maxCpuTimePerSecond) > 0:
            LOGGER.info('{0.name} should be restricted to use no more than '
                        '{0.maxCpuTimePerSecond} CPU seconds per wall clock second'.format(self.resource))
            self.service.restrict_user_cpu(self.resource.name, self.resource.maxCpuTimePerSecond)
        else:
            self.service.unrestrict_user_cpu(self.resource.name)

    def _apply_customizations(self):
        vars = getattr(self.resource, 'sessionVariables', {})
        if not isinstance(vars, dict):
            vars = asdict(vars)
        if len(set(vars.keys()).intersection({'queryCacheType', 'characterSetClient', 'characterSetConnection',
                                              'characterSetResults', 'collationConnection', 'innodbStrictMode'})) > 0:
            vars = {to_snake_case(k): v for k, v in vars.items()}
            addrs_set = set(self.service.normalize_addrs(self.resource.allowedIPAddresses))
            LOGGER.info('Presetting session variables for user {0} with addresses {1}: {2}'.format(
                self.resource.name,
                addrs_set,
                ', '.join(('{}={}'.format(k, v) for k, v in vars.items()))
            ))
            self.service.preset_user_session_vars(self.resource.name, list(addrs_set), vars)

    def create(self):
        db_type = self.service.__class__.__name__
        if not self.op_resource:
            always_allowed_addrs = rgetattr(CONFIG, 'database.default_allowed_networks', [])
            addrs_set = set(self.service.normalize_addrs(self.resource.allowedIPAddresses + always_allowed_addrs))
            LOGGER.info(f'Creating {db_type} user {self.resource.name} with addresses {addrs_set}')
            self.service.create_user(self.resource.name, self.resource.passwordHash, list(addrs_set))
            self.service.set_initial_permissions(self.resource.name, list(addrs_set))
            self._apply_restrictions()
            self._apply_customizations()
        else:
            LOGGER.warning(f'{db_type} user {self.resource.name} already exists, updating')
            self.update()

    def update(self):
        db_type = self.service.__class__.__name__
        if not self.resource.switchedOn or self.params.get('forceSwitchOff'):
            LOGGER.info(f'User {self.resource.name} is switched off, deleting')
            self.delete()
            return
        if self.op_resource:
            always_allowed_addrs = rgetattr(CONFIG, 'database.default_allowed_networks', [])
            current_addrs_set = set(self.service.normalize_addrs(self.op_resource.allowedIPAddresses))
            staging_addrs_set = set(self.service.normalize_addrs(self.resource.allowedIPAddresses +
                                                                 always_allowed_addrs))
            LOGGER.info(f'Updating {db_type} user {self.resource.name}')
            self.service.drop_user(self.resource.name, list(current_addrs_set.difference(staging_addrs_set)))
            self.service.create_user(self.resource.name, self.resource.passwordHash,
                                     list(staging_addrs_set.difference(current_addrs_set)))
            self.service.set_password(self.resource.name, self.resource.passwordHash,
                                      list(current_addrs_set.intersection(staging_addrs_set)))
            self.service.set_initial_permissions(self.resource.name, list(staging_addrs_set))
            self._apply_restrictions()
            self._apply_customizations()
        else:
            LOGGER.warning(f'{db_type} user {self.resource.name} not found, creating')
            self.create()

    def delete(self):
        db_type = self.service.__class__.__name__
        if self.op_resource:
            LOGGER.info(f'Dropping {db_type} user {self.resource.name}')
            self.service.drop_user(self.resource.name, self.op_resource.allowedIPAddresses)
        else:
            LOGGER.warning(f'{db_type} user {self.resource.name} not found')


class DatabaseProcessor(ResProcessor):
    def create(self):
        db_type = self.service.__class__.__name__
        if not self.op_resource:
            LOGGER.info(f'Creating {db_type} database {self.resource.name}')
            self.service.create_database(self.resource.name)
            collector = cnstr.get_rescollector('database', self.resource)
            collector.ignore_property('quotaUsed')
            self.op_resource = collector.get()
        self.update()

    def update(self):
        db_type = self.service.__class__.__name__
        if 'dataSourceParams' in self.params and self.params['dataSourceParams'].get('deleteExtraneous', False):
            LOGGER.info(f'Data cleanup requested, dropping {db_type} database {self.resource.name}')
            self.service.drop_database(self.resource.name)
            self.op_resource = None
            self.create()
            return
        if self.op_resource:
            deleted_user = getattr(self.params.get('delete'), 'name', None)
            staging = set(u.name for u in self.resource.databaseUsers if u.switchedOn and u.name != deleted_user)
            current = set(u.name for u in self.op_resource.databaseUsers)
            new_users = filter(lambda u: u.name in staging.difference(current), self.resource.databaseUsers)
            old_users = filter(lambda u: u.name in current.difference(staging), self.op_resource.databaseUsers)
            spare_users = filter(lambda u: u.name in current.intersection(staging), self.resource.databaseUsers)
            always_allowed_addrs = rgetattr(CONFIG, 'database.default_allowed_networks', [])
            if self.resource.writable:
                for user in new_users:
                    LOGGER.info(f'Granting access on {db_type} database {self.resource.name} to user {user.name}')
                    addrs = set(self.service.normalize_addrs(user.allowedIPAddresses + always_allowed_addrs))
                    self.service.allow_database_access(self.resource.name, user.name, list(addrs))
                for user in spare_users:
                    current_user = next(filter(lambda u: u.name == user.name, self.op_resource.databaseUsers))
                    current_addrs = set(current_user.allowedIPAddresses)
                    staging_addrs = set(user.allowedIPAddresses)
                    new_addrs = self.service.normalize_addrs(list(staging_addrs.difference(current_addrs)) +
                                                             always_allowed_addrs)
                    old_addrs = list(current_addrs.difference(staging_addrs))
                    if new_addrs:
                        LOGGER.info(f'Granting access on {db_type} database {self.resource.name} '
                                    f'to user {user.name} with addresses: {new_addrs}')
                        self.service.allow_database_access(self.resource.name, user.name, new_addrs)
                    if old_addrs:
                        LOGGER.info(f'Revoking access on {db_type} database {self.resource.name} '
                                    f'from user {user.name} with addresses: {old_addrs}')
                        self.service.deny_database_access(self.resource.name, user.name, old_addrs)
                for user in old_users:
                    LOGGER.info(f'Revoking access on {db_type} database {self.resource.name} from user {user.name}')
                    addrs = set(self.service.normalize_addrs(user.allowedIPAddresses + always_allowed_addrs))
                    self.service.deny_database_access(self.resource.name, user.name, list(addrs))
            else:
                for user in new_users:
                    LOGGER.info(f'Granting READ access on {db_type} database {self.resource.name} to user {user.name}')
                    addrs = set(self.service.normalize_addrs(user.allowedIPAddresses + always_allowed_addrs))
                    self.service.allow_database_reads(self.resource.name, user.name, list(addrs))
                for user in spare_users:
                    current_user = next(filter(lambda u: u.name == user.name, self.op_resource.databaseUsers))
                    current_addrs = set(current_user.allowedIPAddresses)
                    staging_addrs = set(self.service.normalize_addrs(user.allowedIPAddresses + always_allowed_addrs))
                    new_addrs = self.service.normalize_addrs(list(staging_addrs.difference(current_addrs)))
                    spare_addrs = list(current_addrs.intersection(staging_addrs))
                    old_addrs = list(current_addrs.difference(staging_addrs))
                    if new_addrs:
                        LOGGER.info(f'Granting READ access on {db_type} database {self.resource.name} '
                                    f'to user {user.name} with addresses: {new_addrs}')
                        self.service.allow_database_reads(self.resource.name, user.name, new_addrs)
                    if spare_addrs:
                        LOGGER.info(f'Revoking WRITE access on {db_type} database {self.resource.name} '
                                    f'from user {user.name} with addresses: {spare_addrs}')
                        self.service.deny_database_writes(self.resource.name, user.name, spare_addrs)
                    if old_addrs:
                        LOGGER.info(f'Revoking access on {db_type} database {self.resource.name} '
                                    f'from user {user.name} with addresses: {old_addrs}')
                        self.service.deny_database_access(self.resource.name, user.name, old_addrs)
                for user in old_users:
                    LOGGER.info(f'Revoking access on {db_type} database {self.resource.name} from user {user.name}')
                    addrs = set(self.service.normalize_addrs(user.allowedIPAddresses + always_allowed_addrs))
                    self.service.deny_database_access(self.resource.name, user.name, list(addrs))
            data_dest_uri = self.params.get('datadestinationUri', f'mysql://{CONFIG.hostname}/{self.resource.name}')
            data_source_uri = self.params.get('datasourceUri', data_dest_uri)
            self._process_data(data_source_uri, data_dest_uri, dict(name=self.resource.name,
                                                                    dataType='database',
                                                                    dbServer=self.service))
        else:
            LOGGER.warning(f'{db_type} database {self.resource.name} not found, creating')
            self.create()

    def delete(self):
        db_type = self.service.__class__.__name__
        if self.op_resource:
            for user in self.op_resource.databaseUsers:
                LOGGER.info(f'Revoking access on {db_type} database {self.resource.name} from user {user.name}')
                self.service.deny_database_access(self.resource.name, user.name, user.allowedIPAddresses)
            LOGGER.info(f'Dropping {db_type} database {self.resource.name}')
            self.service.drop_database(self.resource.name)
        else:
            LOGGER.warning(f'{db_type} database {self.resource.name} not found')


class ServiceProcessor(ResProcessor):
    def create(self):
        self.update()

    def update(self):
        self.params.update(hostname=CONFIG.hostname)
        if isinstance(self.service, HttpServer):
            self.params['app_servers'] = cnstr.get_application_servers()
        elif isinstance(self.service, Apache):
            self.params.update(admin_networks=CONFIG.apache.admin_networks)
        if isinstance(self.service, ConfigurableService):
            configs = self.service.get_configs_in_context(self.service)
        else:
            configs = []
        for each in configs:
            each.render_template(service=self.service, params=self.params)
            each.write()
        try:
            status = self.service.status()
            if self.resource.switchedOn and status is ServiceStatus.UP:
                self.service.reload()
            elif self.resource.switchedOn:
                LOGGER.warning(f'{self.service.name} is down, starting it')
                self.service.start()
            elif status is ServiceStatus.UP:
                self.service.stop()
        except:
            for each in configs: each.revert()
            raise
        for each in configs: each.confirm()

    def delete(self):
        pass


class ResourceArchiveProcessor(ResProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        self._archive_storage = FTPClient(**asdict(CONFIG.ftp))
        self._archive_filename = urllib.parse.urlparse(self.resource.fileLink).path.lstrip('/')

    def create(self):
        if self.resource.resourceType == 'WEBSITE':
            arch_src = self.resource.resource.documentRoot
            params = {'basedir': self.resource.resource.unixAccount.homeDir}
        elif self.resource.resourceType == 'DATABASE':
            arch_src = self.resource.resource.name
            params = None
        else:
            raise ResourceValidationError(f'Unknown resource type: {self.resource.resourceType}')
        LOGGER.info(f'Archiving {self.resource.resourceType.lower()} {arch_src}')
        data_stream, error_stream = self.service.get_archive_stream(arch_src, params=params)
        LOGGER.info(f'Uploading {arch_src} archive to {self._archive_storage.host} as {self._archive_filename}')
        self._archive_storage.upload(data_stream, self._archive_filename)
        error = error_stream.read().decode('UTF-8')
        if error:
            raise ResourceProcessingError(f'Failed to archive {self.resource.resourceType.lower()} {arch_src}: {error}')

    def update(self):
        pass

    def delete(self):
        LOGGER.info(f'Deleting {self._archive_filename} file at {self._archive_storage.host}')
        self._archive_storage.delete(self._archive_filename)


class RedirectProcessor(ResProcessor):
    @property
    def _without_reload(self):
        return self.params.get('required_for', [None])[0] == 'service'

    @synchronized
    def create(self):
        res_dict = asdict(self.resource)
        res_dict['domains'] = [res_dict.get('domain')]
        del res_dict['domain']
        vhost = collections.namedtuple('VHost', res_dict.keys())(*res_dict.values())
        configs = self.service.get_website_configs(self.resource)
        for each in configs:
            each.render_template(service=self.service, vhosts=[vhost], params=self.params)
            each.write()
        if not self._without_reload:
            try:
                self.service.reload()
            except:
                for each in configs: each.revert()
                raise
        for each in configs: each.confirm()

    def update(self):
        if self.resource.switchedOn:
            self.create()
        else:
            self.delete()

    @synchronized
    def delete(self):
        for each in self.service.get_website_configs(self.resource): each.delete()
        self.service.reload()
