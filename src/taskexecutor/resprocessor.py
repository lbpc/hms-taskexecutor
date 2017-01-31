import os
import pytransliter
import abc
import collections

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.httpsclient
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class ResProcessor(metaclass=abc.ABCMeta):
    def __init__(self, resource, service, params):
        super().__init__()
        self._resource = None
        self._service = None
        self._params = dict()
        self._extra_services = None
        self.resource = resource
        self.service = service
        self.params = params

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

    @abc.abstractmethod
    def create(self):
        pass

    @abc.abstractmethod
    def update(self):
        pass

    @abc.abstractmethod
    def delete(self):
        pass


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
            self.service.set_quota(self.resource.quota)
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
        LOGGER.info("Modifying user {0.name}".format(self.resource))
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
        if len(self.resource.crontab) > 0:
            self.service.create_crontab(self.resource.name, [task for task in self.resource.crontab if task.switchedOn])
        else:
            self.service.delete_crontab(self.resource.name)

    def delete(self):
        self.service.kill_user_processes(self.resource.name)
        self.service.delete_user(self.resource.name)


class WebSiteProcessor(ResProcessor):
    def _build_vhost_obj_list(self):
        vhosts = list()
        non_ssl_domains = list()
        res_dict = self.resource._asdict()
        for domain in self.resource.domains:
            if domain.sslCertificate:
                res_dict["domains"] = [domain, ]
                vhosts.append(
                        collections.namedtuple("VHost", res_dict.keys())(*res_dict.values()))
            else:
                non_ssl_domains.append(domain)
        res_dict["domains"] = non_ssl_domains
        vhosts.append(collections.namedtuple("VHost", res_dict.keys())(*res_dict.values()))
        return vhosts

    @taskexecutor.utils.synchronized
    def create(self):
        self.params.update({
            "app_server_name": self.service.name,
            "error_pages": [(code, "http_{}.html".format(code)) for code in (403, 404, 502, 503, 504)],
            "static_base": CONFIG.nginx.static_base_path,
            "ssl_path": CONFIG.nginx.ssl_certs_path,
            "anti_ddos_location": CONFIG.nginx.anti_ddos_location,
            "anti_ddos_set_cookie_file": CONFIG.nginx.anti_ddos_set_cookie_file,
            "anti_ddos_check_cookie_file": CONFIG.nginx.anti_ddos_check_cookie_file,
            "subdomains_document_root": "/".join(self.resource.documentRoot.split("/")[:-1])
        })
        vhosts_list = self._build_vhost_obj_list()
        home_dir = os.path.normpath(str(self.resource.unixAccount.homeDir))
        document_root = os.path.normpath(str(self.resource.documentRoot))
        for directory in (os.path.join(home_dir, "logs"), os.path.join(home_dir, document_root)):
            os.makedirs(directory, mode=0o755, exist_ok=True)
        for directory in ["/".join(document_root.split("/")[0:i + 1]) for i, d in enumerate(document_root.split("/"))]:
            os.chown(os.path.join(home_dir, directory), self.resource.unixAccount.uid, self.resource.unixAccount.uid)
        for service in (self.service, self.extra_services.http_proxy):
            config = service.get_website_config(self.resource.id)
            config.render_template(socket=service.socket, vhosts=vhosts_list, params=self.params)
            config.write()
            if self.resource.switchedOn and not config.is_enabled:
                config.enable()
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
                    config.write()
                    config.confirm()
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
class WebSiteProcessorBaton(WebSiteProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        res_dict = self.resource._asdict()
        res_dict["documentRoot"] = pytransliter.translit(res_dict["documentRoot"], "ru")
        self.resource = collections.namedtuple("ApiObject", res_dict.keys())(*res_dict.values())


class SSLCertificateProcessor(ResProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        cert_file_path = os.path.join(CONFIG.nginx.ssl_certs_path, "{0.name}.pem".format(self.resource))
        key_file_path = os.path.join(CONFIG.nginx.ssl_certs_path, "{0.name}.key".format(self.resource))
        constructor = taskexecutor.constructor.Constructor()
        self._cert_file = constructor.get_conffile("basic", cert_file_path)
        self._key_file = constructor.get_conffile("basic", key_file_path)

    @taskexecutor.utils.synchronized
    def create(self):
        self._cert_file.body = self.resource.certificate
        self._key_file.body = self.resource.key
        self._cert_file.save()
        self._key_file.save()

    def update(self):
        self.create(self)

    def delete(self):
        self._cert_file.delete()
        self._key_file.delete()


class MailboxProcessor(ResProcessor):
    def create(self):
        self.service.create_maildir(self.resource.mailSpool, self.resource.name, self.resource.uid)

    def update(self):
        pass

    def delete(self):
        self.service.delete_maildir(self.resource.mailSpool, self.resource.name)


class DatabaseUserProcessor(ResProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        self._current_user = self.service.get_user(self.resource.name)

    def create(self):
        if not self._current_user:
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
        if self._current_user:
            current_addrs_set = set(self.service.normalize_addrs(self._current_user.allowedIPAddresses))
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
        if self._current_user:
            LOGGER.info("Dropping {0} user {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_user(self.resource.name, self._current_user.allowedIPAddresses)
        else:
            LOGGER.warning("{0} user {1} not found".format(self.service.__class__.__name__, self.resource.name))


class DatabaseProcessor(ResProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        self._current_database = self.service.get_database(self.resource.name)

    def create(self):
        if not self._current_database:
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
        if self._current_database:
            current_usernames_set = set([user.name for user in self._current_database.databaseUsers])
            staging_usernames_set = set([user.name for user in self.resource.databaseUsers])
            new_users_list = [user for user in self.resource.databaseUsers
                              if user.name in staging_usernames_set.difference(current_usernames_set)]
            old_users_list = [user for user in self._current_database.databaseUsers
                              if user.name in current_usernames_set.difference(staging_usernames_set)]
            spare_users_list = [user for user in self.resource.databaseUsers
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
                    current_user = self.service.get_user(user.name)
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
        if self._current_database:
            for user in self._current_database.databaseUsers:
                LOGGER.info("Revoking access on {0} database {1} from "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                self.service.deny_database_access(self.resource.name, user.name, user.allowedIPAddresses)
            LOGGER.info("Dropping {0} database {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_database(self.resource.name)
        else:
            LOGGER.warning("{0} database {1} not found".format(self.service.__class__.__name__, self.resource.name))


class ServiceProcessor(ResProcessor):
    def __init__(self, resource, service, params):
        super().__init__(resource, service, params)
        self.service = taskexecutor.constructor.Constructor().get_opservice(
            self.resource.serviceType.name
        )

    def create(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass


class Builder:
    def __new__(cls, res_type):
        if res_type == "service":
            return ServiceProcessor
        elif res_type == "unix-account":
            return UnixAccountProcessor
        elif res_type == "database-user":
            return DatabaseUserProcessor
        elif res_type == "database":
            return DatabaseProcessor
        elif res_type == "website" and CONFIG.hostname == "baton":
            return WebSiteProcessorBaton
        elif res_type == "website":
            return WebSiteProcessor
        elif res_type == "sslcertificate":
            return SSLCertificateProcessor
        elif res_type == "mailbox":
            return MailboxProcessor
        else:
            raise BuilderTypeError("Unknown resource type: {}".format(res_type))
