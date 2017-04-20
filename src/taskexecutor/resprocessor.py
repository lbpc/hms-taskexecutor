import os
import pytransliter
import sys
import abc
import collections
import time
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.ftpclient
import taskexecutor.httpsclient
import taskexecutor.opservice
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class ResourceValidationError(Exception):
    pass


class ResourceProcessingError(Exception):
    pass


class ResProcessor(metaclass=abc.ABCMeta):
    def __init__(self, resource, service, params):
        super().__init__()
        self._resource = None
        self._service = None
        self._params = dict()
        self._extra_services = None
        self._op_resource = None
        self.resource = resource
        self.service = service
        self.params = params
        if isinstance(self.service, taskexecutor.opservice.OpService):
            while self.service.status() is not taskexecutor.opservice.UP:
                LOGGER.warning("{} is down, waiting for it".format(self.service.name))
                time.sleep(1)

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
    def params(self):
        return self._params

    @params.setter
    def params(self, value):
        self._params = value

    @params.deleter
    def params(self):
        del self._params

    @property
    def extra_services(self):
        return self._extra_services

    @extra_services.setter
    def extra_services(self, value):
        self._extra_services = value

    @extra_services.deleter
    def extra_services(self):
        del self._extra_services

    @property
    def op_resource(self):
        return self._op_resource

    @op_resource.setter
    def op_resource(self, value):
        self._op_resource = value

    @op_resource.deleter
    def op_resource(self):
        del self._op_resource

    @abc.abstractmethod
    def create(self):
        pass

    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def delete(self):
        pass

    def __str__(self):
        return "{0}(resource=(name={1.name}, id={1.id}))".format(self.__class__.__name__, self.resource)


class UnixAccountProcessor(ResProcessor):
    def create(self):
        LOGGER.info("Adding user {0.name} to system".format(self.resource))
        self.service.create_user(self.resource.name,
                                 self.resource.uid,
                                 self.resource.homeDir,
                                 self.resource.passwordHash,
                                 "Hosting account HMS id {}".format(self.resource.id))
        try:
            LOGGER.info("Setting quota for user {0.name}".format(self.resource))
            self.service.set_quota(self.resource.uid, self.resource.quota)
        except Exception:
            LOGGER.error("Setting quota failed "
                         "for user {0.name}".format(self.resource))
            self.service.delete_user(self.resource.name)
            raise
        if len(self.resource.crontab) > 0:
            self.service.create_crontab(self.resource.name, [task for task in self.resource.crontab if task.switchedOn])
        if hasattr(self.resource, "keyPair") and self.resource.keyPair:
            LOGGER.info("Creating authorized_keys for user {0.name}".format(self.resource))
            self.service.create_authorized_keys(self.resource.keyPair.publicKey,
                                                self.resource.uid, self.resource.homeDir)

    def update(self):
        if self.op_resource:
            LOGGER.info("Modifying user {0.name}".format(self.resource))
            if self.resource.uid != self.op_resource.uid:
                LOGGER.warning("UnixAccount {0} has wrong UID {1}, "
                               "expected: {2}".format(self.resource.name, self.op_resource.uid, self.resource.uid))
            self.service.set_shell(self.resource.name,
                                   {True: self.service.default_shell, False: None}[self.resource.switchedOn])
            if self.resource.sendmailAllowed:
                self.service.enable_sendmail(self.resource.uid)
            else:
                self.service.disable_sendmail(self.resource.uid)
            if not self.resource.writable:
                LOGGER.info("Disabling writes by setting quota=quotaUsed for user {0.name}".format(self.resource))
                self.service.set_quota(self.resource.uid, self.resource.quotaUsed)
            else:
                LOGGER.info("Setting quota for user {0.name}".format(self.resource))
                self.service.set_quota(self.resource.uid, self.resource.quota)
            if hasattr(self.resource, "keyPair") and self.resource.keyPair:
                LOGGER.info("Creating authorized_keys for user {0.name}".format(self.resource))
                self.service.create_authorized_keys(self.resource.keyPair.publicKey,
                                                    self.resource.uid, self.resource.homeDir)
            if len(self.resource.crontab) > 0 and self.resource.switchedOn:
                self.service.create_crontab(self.resource.name,
                                            [task for task in self.resource.crontab if task.switchedOn])
            else:
                self.service.delete_crontab(self.resource.name)
        else:
            LOGGER.warning("UnixAccount {0} not found, creating".format(self.resource.name))
            self.create()

    def delete(self):
        self.service.kill_user_processes(self.resource.name)
        self.service.delete_user(self.resource.name)


class WebSiteProcessor(ResProcessor):
    def _build_vhost_obj_list(self):
        vhosts = list()
        non_ssl_domains = list()
        res_dict = self.resource._asdict()
        for domain in self.resource.domains:
            if domain.sslCertificate and domain.sslCertificate.switchedOn:
                res_dict["domains"] = [domain, ]
                vhosts.append(
                        collections.namedtuple("VHost", res_dict.keys())(*res_dict.values()))
            else:
                domain_dict = domain._asdict()
                if "sslCertificate" in domain_dict.keys():
                    del domain_dict["sslCertificate"]
                non_ssl_domains.append(collections.namedtuple("Domain", domain_dict.keys())(*domain_dict.values()))
        if non_ssl_domains:
            res_dict["domains"] = non_ssl_domains
            vhosts.append(collections.namedtuple("VHost", res_dict.keys())(*res_dict.values()))
        return vhosts

    @taskexecutor.utils.synchronized
    def create(self):
        self.params.update(app_server_name=self.service.name,
                           error_pages=[(code, "http_{}.html".format(code)) for code in (403, 404, 502, 503, 504)],
                           anti_ddos_location=CONFIG.nginx.anti_ddos_location,
                           anti_ddos_set_cookie_file=CONFIG.nginx.anti_ddos_set_cookie_file,
                           anti_ddos_check_cookie_file=CONFIG.nginx.anti_ddos_check_cookie_file,
                           subdomains_document_root="/".join(self.resource.documentRoot.split("/")[:-1]))
        vhosts_list = self._build_vhost_obj_list()
        home_dir = os.path.normpath(str(self.resource.unixAccount.homeDir))
        document_root = os.path.normpath(str(self.resource.documentRoot))
        for directory in (os.path.join(home_dir, "logs"), os.path.join(home_dir, document_root)):
            os.makedirs(directory, mode=0o755, exist_ok=True)
        for directory in ["/".join(document_root.split("/")[0:i + 1]) for i, d in enumerate(document_root.split("/"))]:
            os.chown(os.path.join(home_dir, directory), self.resource.unixAccount.uid, self.resource.unixAccount.uid)
        for service in (self.service, self.extra_services.http_proxy):
            config = service.get_website_config(self.resource.id)
            config.render_template(service=service, vhosts=vhosts_list, params=self.params)
            config.write()
            if self.resource.switchedOn and not config.is_enabled:
                config.enable()
            if self.params.get("required_for", [None])[0] != "service":
                try:
                    service.reload()
                except:
                    config.revert()
                    raise
            config.confirm()

    @taskexecutor.utils.synchronized
    def update(self):
        if not self.resource.switchedOn:
            for service in (self.service, self.extra_services.http_proxy):
                config = service.get_website_config(self.resource.id)
                if config.is_enabled:
                    config.disable()
                    config.save()
                    service.reload()
        else:
            self.create()

    @taskexecutor.utils.synchronized
    def delete(self):
        for service in (self.extra_services.http_proxy, self.service):
            config = service.get_website_config(self.resource.id)
            if not os.path.exists(config.file_path):
                LOGGER.warning("{} does not exist".format(config.file_path))
                continue
            if config.is_enabled:
                config.disable()
            config.delete()
            service.reload()


# HACK: the only purpose of this class is to process
# WebSite resources at baton.intr
# should be removed when this server is gone
class WebSiteProcessorFreeBsd(WebSiteProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        res_dict = self.resource._asdict()
        res_dict["documentRoot"] = pytransliter.translit(res_dict["documentRoot"], "ru")
        self.resource = collections.namedtuple("ApiObject", res_dict.keys())(*res_dict.values())


class SslCertificateProcessor(ResProcessor):
    @taskexecutor.utils.synchronized
    def create(self):
        cert_file, key_file = self.service.get_ssl_key_pair_files(self.resource.name)
        cert_file.body = self.resource.cert
        key_file.body = self.resource.key
        cert_file.save()
        key_file.save()

    def update(self):
        self.create()

    def delete(self):
        cert_file, key_file = self.service.get_ssl_key_pair_files(self.resource.name)
        if cert_file.exists:
            cert_file.delete()
        if key_file.exists:
            key_file.delete()


class MailboxProcessor(ResProcessor):
    def create(self):
        self.service.create_maildir(self.resource.mailSpool, self.resource.name, self.resource.uid)

    def update(self):
        pass

    def delete(self):
        self.service.delete_maildir(self.resource.mailSpool, self.resource.name)


class DatabaseUserProcessor(ResProcessor):
    def create(self):
        if not self.op_resource:
            addrs_set = set(self.service.normalize_addrs(self.resource.allowedIPAddresses))
            LOGGER.info("Creating {0} user {1} with addresses {2}".format(self.service.__class__.__name__,
                                                                          self.resource.name,
                                                                          addrs_set))
            self.service.create_user(self.resource.name, self.resource.passwordHash, list(addrs_set))
        else:
            LOGGER.warning("{0} user {1} already exists, updating".format(self.service.__class__.__name__,
                                                                          self.resource.name))
            self.update()

    def update(self):
        if not self.resource.switchedOn:
            self.delete()
            return
        if self.op_resource:
            current_addrs_set = set(self.service.normalize_addrs(self.op_resource.allowedIPAddresses))
            staging_addrs_set = set(self.service.normalize_addrs(self.resource.allowedIPAddresses))
            LOGGER.info("Updating {0} user {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_user(self.resource.name, list(current_addrs_set.difference(staging_addrs_set)))
            self.service.create_user(self.resource.name, self.resource.passwordHash,
                                     list(staging_addrs_set.difference(current_addrs_set)))
            self.service.set_password(self.resource.name, self.resource.passwordHash,
                                      list(current_addrs_set.intersection(staging_addrs_set)))
        else:
            LOGGER.warning("{0} user {1} not found, creating".format(self.service.__class__.__name__,
                                                                     self.resource.name))
            self.create()

    def delete(self):
        if self.op_resource:
            LOGGER.info("Dropping {0} user {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_user(self.resource.name, self.op_resource.allowedIPAddresses)
        else:
            LOGGER.warning("{0} user {1} not found".format(self.service.__class__.__name__, self.resource.name))


class DatabaseProcessor(ResProcessor):
    def create(self):
        if not self.op_resource:
            LOGGER.info("Creating {0} database {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.create_database(self.resource.name)
            for user in self.resource.databaseUsers:
                addrs_set = set(self.service.normalize_addrs(user.allowedIPAddresses))
                LOGGER.info("Granting access on {0} database {1} to user {2} "
                            "with addresses {3}".format(self.service.__class__.__name__, self.resource.name,
                                                        user.name, addrs_set))
                self.service.allow_database_access(self.resource.name, user.name, list(addrs_set))
        else:
            LOGGER.warning("{0} database {1} already exists, updating".format(self.service.__class__.__name__,
                                                                              self.resource.name))
            self.update()

    def update(self):
        database_users = self.resource.databaseUsers
        if self.params.get("delete"):
            database_users.remove(self.params["delete"])
        if self.op_resource:
            current_usernames_set = set((user.name for user in self.op_resource.databaseUsers))
            staging_usernames_set = set((user.name for user in database_users))
            new_users_list = [user for user in database_users
                              if user.name in staging_usernames_set.difference(current_usernames_set)]
            old_users_list = [user for user in self.op_resource.databaseUsers
                              if user.name in current_usernames_set.difference(staging_usernames_set)]
            spare_users_list = [user for user in database_users
                                if user.name in current_usernames_set.intersection(staging_usernames_set)]
            if self.resource.writable:
                for user in new_users_list:
                    LOGGER.info("Granting access on {0} database {1} to "
                                "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                    addrs_set = set(self.service.normalize_addrs(user.allowedIPAddresses))
                    self.service.allow_database_access(self.resource.name, user.name, list(addrs_set))
                for user in spare_users_list:
                    LOGGER.info("Granting access on {0} database {1} to "
                                "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                    current_user = taskexecutor.constructor.get_rescollector("database-user", user).get()
                    current_addrs_set = set(current_user.allowedIPAddresses)
                    staging_addrs_set = set(user.allowedIPAddresses)
                    addrs_set = self.service.normalize_addrs(list(staging_addrs_set.difference(current_addrs_set)))
                    self.service.allow_database_access(self.resource.name, user.name, list(addrs_set))
                for user in old_users_list:
                    LOGGER.info("Revoking access on {0} database {1} from "
                                "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                    addrs_set = set(self.service.normalize_addrs(user.allowedIPAddresses))
                    self.service.deny_database_access(self.resource.name, user.name, list(addrs_set))
            else:
                for user in new_users_list:
                    LOGGER.info("Granting READ access on {0} database {1} to "
                                "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                    addrs_set = set(self.service.normalize_addrs(user.allowedIPAddresses))
                    self.service.allow_database_reads(self.resource.name, user.name, list(addrs_set))
                for user in spare_users_list:
                    LOGGER.info("Revoking WRITE access on {0} database {1} from "
                                "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                    addrs_set = set(self.service.normalize_addrs(user.allowedIPAddresses))
                    self.service.deny_database_writes(self.resource.name, user.name, list(addrs_set))
                for user in old_users_list:
                    LOGGER.info("Revoking access on {0} database {1} from "
                                "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                    addrs_set = set(self.service.normalize_addrs(user.allowedIPAddresses))
                    self.service.deny_database_access(self.resource.name, user.name, list(addrs_set))
        else:
            LOGGER.warning("{0} database {1} not found, creating".format(self.service.__class__.__name__,
                                                                         self.resource.name))
            self.create()

    def delete(self):
        if self.op_resource:
            for user in self.op_resource.databaseUsers:
                LOGGER.info("Revoking access on {0} database {1} from "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                self.service.deny_database_access(self.resource.name, user.name, user.allowedIPAddresses)
            LOGGER.info("Dropping {0} database {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_database(self.resource.name)
        else:
            LOGGER.warning("{0} database {1} not found".format(self.service.__class__.__name__, self.resource.name))


class ServiceProcessor(ResProcessor):
    def _create_error_pages(self):
        self.params.update(error_pages=list())
        for code in (403, 404, 502, 503, 504):
            self.params["error_pages"].append((code, "http_{}.html".format(code)))
            error_page_path = os.path.join(self.service.static_base_path, "http_{}.html".format(code))
            error_page = self.service.get_abstract_config("@HTTPErrorPage", error_page_path)
            error_page.render_template(code=code)
            error_page.save()

    def create(self):
        self.update()

    def update(self):
        self.params.update(hostname=CONFIG.hostname)
        if isinstance(self.service, taskexecutor.opservice.Nginx):
            self._create_error_pages()
            self.params.update(app_servers=taskexecutor.constructor.get_all_opservices_by_res_type("website"))
        configs = self.service.get_concrete_configs_set()
        if isinstance(self.service, taskexecutor.opservice.Apache) and self.service.interpreter.name != "php":
            configs = [c for c in configs if os.path.basename(c.file_path) != "php.ini"]
        for config in configs:
            config.render_template(service=self.service, params=self.params)
            config.write()
        try:
            self.service.reload()
        except:
            for config in configs:
                config.revert()
            raise
        for config in configs:
            config.confirm()

    def delete(self):
        pass


class ResourceArchiveProcessor(ResProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        self._archive_storage = taskexecutor.ftpclient.FTPClient(**CONFIG.ftp._asdict())
        self._archive_filename = urllib.parse.urlparse(self.resource.fileLink).path.lstrip("/")

    def create(self):
        if self.resource.resourceType == "WEBSITE":
            archive_source = self.resource.resource.documentRoot
            params = {"basedir": self.resource.resource.unixAccount.homeDir}
        elif self.resource.resourceType == "DATABASE":
            archive_source = self.resource.resource.name
            params = None
        else:
            raise ResourceValidationError("Unknown resource type: {}".format(self.resource.resourceType))
        LOGGER.info("Archiving {0} {1}".format(self.resource.resourceType.lower(), archive_source))
        data_stream, error_stream = self.service.get_archive_stream(archive_source, params=params)
        LOGGER.info("Uploading {0} archive "
                    "to {1} as {2}".format(archive_source, self._archive_storage.host, self._archive_filename))
        self._archive_storage.upload(data_stream, self._archive_filename)
        error = error_stream.read().decode("UTF-8")
        if error:
            raise ResourceProcessingError("Failed to archive {0} {1}: "
                                          "{2}".format(self.resource.resourceType.lower(), archive_source, error))

    def update(self):
        self.create()

    def delete(self):
        LOGGER.info("Deleting {0} file at {1}".format(self._archive_filename, self._archive_storage.host))
        self._archive_storage.delete(self._archive_filename)


class Builder:
    def __new__(cls, res_type):
        ResProcessorClass = {"service": ServiceProcessor,
                             "unix-account": UnixAccountProcessor,
                             "database-user": DatabaseUserProcessor,
                             "database": DatabaseProcessor,
                             "website": WebSiteProcessor if sys.platform != "freebsd9" else WebSiteProcessorFreeBsd,
                             "ssl-certificate": SslCertificateProcessor,
                             "mailbox": MailboxProcessor,
                             "resource-archive": ResourceArchiveProcessor}.get(res_type)
        if not ResProcessorClass:
            raise BuilderTypeError("Unknown resource type: {}".format(res_type))
        return ResProcessorClass
