import os
import shutil
import pytransliter
import abc
import collections

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.httpsclient
import taskexecutor.utils

__all__ = ["Builder", "DatabaseUserProcessor", "WebSiteProcessor"]


class ResProcessor(metaclass=abc.ABCMeta):
    def __init__(self, resource, params):
        super().__init__()
        self._resource = None
        self._service = None
        self._params = dict()
        self._extra_services = None
        self.resource = resource
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
    def __init__(self, resource, params):
        super().__init__(resource, params)

    def _adduser(self):
        LOGGER.info("Adding user {0.name} to system".format(self.resource))
        taskexecutor.utils.exec_command("useradd "
                                        "--comment 'Hosting account HMS ID {0.id}' "
                                        "--uid {0.uid} "
                                        "--home {0.homeDir} "
                                        "--password '{0.passwordHash}' "
                                        "--create-home "
                                        "--shell /bin/bash "
                                        "{0.name}".format(self.resource))

    def _setquota(self, quota=None):
        LOGGER.info("Setting quota for user {0.name}".format(self.resource))
        if not quota:
            quota = self.resource.quota
        taskexecutor.utils.exec_command("setquota "
                                        "-g {0} 0 {1} "
                                        "0 0 /home".format(self.resource.uid, int(quota / 1024)))

    def _userdel(self):
        LOGGER.info("Deleting user {0.name}".format(self.resource))
        taskexecutor.utils.exec_command("userdel "
                                        "--force "
                                        "--remove "
                                        "{0.name}".format(self.resource))

    def _killprocs(self):
        LOGGER.info("Killing user {0.name}'s processes, "
                    "if any".format(self.resource))
        taskexecutor.utils.exec_command("killall -9 -u {0.name} || true".format(self.resource))

    def _create_authorized_keys(self):
        if not os.path.exists("{0.homeDir}/.ssh".format(self.resource)):
            os.mkdir("{0.homeDir}/.ssh".format(self.resource), mode=0o700)
        constructor = taskexecutor.constructor.Constructor()
        authorized_keys = constructor.get_conffile("basic",
                                                   "{0.homeDir}/.ssh/authorized_keys".format(self.resource),
                                                   owner_uid=self.resource.uid,
                                                   mode=0o400)
        authorized_keys.body = self.resource.keyPair.publicKey
        authorized_keys.save()

    def _create_crontab(self):
        crontab_string = str()
        for entry in self.resource.crontab:
            if entry.switchedOn:
                crontab_string = ("{0}"
                                  "#{1.name}\n"
                                  "#{1.execTimeDescription}\n"
                                  "{1.execTime} {1.command}\n").format(crontab_string, entry)
        LOGGER.info("Installing '{0}' crontab for {1}".format(crontab_string, self.resource.name))
        taskexecutor.utils.exec_command("crontab -u {} -".format(self.resource.name), pass_to_stdin=crontab_string)

    def _delete_crontab_if_present(self):
        if os.path.exists("/var/spool/cron/crontabs/{}".format(self.resource.name)):
            LOGGER.info("Deleting {} crontab".format(self.resource.name))
            taskexecutor.utils.exec_command("crontab -u {} -r".format(self.resource.name))

    def create(self):
        self._adduser()
        try:
            self._setquota()
        except Exception:
            LOGGER.error("Setting quota failed "
                         "for user {0.name}".format(self.resource))
            self._userdel()
            raise
        if len(self.resource.crontab) > 0:
            self._create_crontab()
        if hasattr(self.resource, "keyPair") and self.resource.keyPair:
            self._create_authorized_keys()

    def update(self):
        LOGGER.info("Modifying user {0.name}".format(self.resource))
        if not self.resource.writable:
            self._setquota(self.resource.quotaUsed)
        else:
            self._setquota()
        if hasattr(self.resource, "keyPair") and self.resource.keyPair:
            self._create_authorized_keys()
        if len(self.resource.crontab) > 0:
            self._create_crontab()
        else:
            self._delete_crontab_if_present()

    def delete(self):
        self._killprocs()
        self._userdel()


# HACK: the only purpose of this class is to process
# UnixAccount resources at baton.intr
# should be removed when this server is gone
class UnixAccountProcessorBaton(UnixAccountProcessor):
    def _update_jailed_ssh(self, action):
        jailed_ssh_config = taskexecutor.constructor.Constructor().get_conffile(
                "lines", "/usr/jail/usr/local/etc/ssh/sshd_clients_config"
        )
        allow_users = jailed_ssh_config.get_lines("^AllowUsers", count=1).split(' ')
        getattr(allow_users, action)(self.resource.name)
        jailed_ssh_config.replace_line("^AllowUsers", " ".join(allow_users))
        jailed_ssh_config.save()
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                                        "pgrep sshd | xargs kill -HUP",
                                        shell="/usr/local/bin/bash")

    def _adduser(self):
        LOGGER.info("Adding user {0.name} to system".format(self.resource))
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                                        "pw useradd {0.name} "
                                        "-u {0.uid} "
                                        "-d {0.homeDir} "
                                        "-h - "
                                        "-s /usr/local/bin/bash "
                                        "-c 'Hosting account'".format(self.resource, CONFIG),
                                        shell="/usr/local/bin/bash")
        self._update_jailed_ssh("append")

    def _setquota(self, quota=None):
        LOGGER.info("Setting quota for user {0.name}".format(self.resource))
        if not quota:
            quota = self.resource.quota
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{2.hostname}-a/ {print $12}') "
                                        "edquota "
                                        "-g "
                                        "-e /home:0:{1} "
                                        "{0}".format(self.resource.uid, int(quota / 1024), CONFIG),
                                        shell="/usr/local/bin/bash")

    def _userdel(self):
        LOGGER.info("Deleting user {0.name}".format(self.resource))
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                                        "pw userdel {0.name} -r".format(self.resource, CONFIG),
                                        shell="/usr/local/bin/bash")
        self._update_jailed_ssh("remove")

    def _killprocs(self):
        LOGGER.info("Killing user {0.name}'s processes, if any".format(self.resource))
        taskexecutor.utils.exec_command("killall -9 -u {0.uid} || true".format(self.resource),
                                        shell="/usr/local/bin/bash")

    def _create_crontab(self):
        crontab_string = str()
        for entry in self.resource.crontab:
            if entry.switchedOn:
                crontab_string = ("{0}"
                                  "{1.execTime} {1.command}\n").format(crontab_string, entry)
        LOGGER.info("Installing '{0}' crontab for {1}".format(crontab_string, self.resource.name))
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                                        "crontab -u {0.name} -".format(self.resource, CONFIG),
                                        pass_to_stdin=crontab_string)

    def _delete_crontab_if_present(self):
        if os.path.exists("/usr/jail/var/cron/tabs/{}".format(self.resource.name)):
            LOGGER.info("Deleting {} crontab".format(self.resource.name))
            taskexecutor.utils.exec_command("jexec "
                                            "$(jls -ns | "
                                            "awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                                            "crontab -u {0.name} -r".format(self.resource, CONFIG))


class WebSiteProcessor(ResProcessor):
    def _build_vhost_obj_list(self):
        vhosts = list()
        non_ssl_domains = list()
        res_dict = self.resource._asdict()
        for domain in self.resource.domains:
            # FIXME: there is no real need in attribute presence check
            if hasattr(domain, "sslCertificate") and domain.sslCertificate:
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
        home_dir = os.path.normpath(self.resource.unixAccount.homeDir)
        document_root = os.path.normpath(self.resource.documentRoot)
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
    def __init__(self, resource, params):
        super().__init__(resource, params)
        res_dict = self.resource._asdict()
        res_dict["documentRoot"] = pytransliter.translit(res_dict["documentRoot"], "ru")
        self.resource = collections.namedtuple("ApiObject", res_dict.keys())(*res_dict.values())


class SSLCertificateProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        cert_file_path = os.path.join(CONFIG.nginx.ssl_certs_path, "{1.name}.pem".format(self.resource))
        key_file_path = os.path.join(CONFIG.nginx.ssl_certs_path, "{1.name}.key".format(self.resource))
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
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._is_last = False
        if self.resource.antiSpamEnabled:
            self._avp_domains = taskexecutor.constructor.Constructor().get_conffile(
                "lines", "/etc/exim4/etc/avp_domains{}".format(self.resource.popServer.id)
            )

    @taskexecutor.utils.synchronized
    def create(self):
        if self.resource.antiSpamEnabled and not self._avp_domains.has_line(self.resource.domain.name):
            self._avp_domains.add_line(self.resource.domain.name)
            self._avp_domains.save()
        else:
            LOGGER.info("{0.name}@{0.domain.name} is not spam protected".format(self.resource))

    def update(self):
        self.create()

    @taskexecutor.utils.synchronized
    def delete(self):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            mailboxes_remaining = api.Mailbox(query={"domain": self.resource.domain.name}).get()
        if len(mailboxes_remaining) == 1:
            LOGGER.info("{0.name}@{0.domain} is the last mailbox in {0.domain}".format(self.resource))
            self._is_last = True
        if self.resource.antiSpamEnabled and self._is_last:
            self._avp_domains.remove_line(self.resource.domain.name)
            self._avp_domains.save()


class MailboxAtPopperProcessor(MailboxProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)

    def create(self):
        if not os.path.isdir(self.resource.mailSpool):
            LOGGER.info("Creating directory {}".format(self.resource.mailSpool))
            os.mkdir(self.resource.mailSpool)
        else:
            LOGGER.info("Mail spool directory {} "
                        "already exists".format(self.resource.mailSpool))
        LOGGER.info("Setting owner {0.unixAccount.uid} "
                    "for {0.mailSpool}".format(self.resource))
        os.chown(self.resource.mailSpool,
                 self.resource.unixAccount.uid,
                 self.resource.unixAccount.uid)

    def delete(self):
        super().delete()
        LOGGER.info("Removing {0.mailSpool]}/{0.name} recursively".format(self.resource))
        shutil.rmtree("{0.mailSpool]}/{0.name}".format(self.resource))
        if self._is_last:
            LOGGER.info("{0.mailSpool}/{0.name} was the last maildir, removing spool itself".format(self.resource))
            os.rmdir(self.resource.mailSpool)


class MailboxAtMxProcessor(MailboxProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._relay_domains = taskexecutor.constructor.Constructor().get_conffile(
                "lines",
                "/etc/exim4/etc/relay_domains{}".format(self.resource.popServer.id)
        )

    def create(self):
        super().create()
        if not self._relay_domains.has_line(self.resource.domain.name):
            self._relay_domains.add_line(self.resource.domain.name)
            self._relay_domains.save()
        else:
            LOGGER.info("{0} already exists in {1}, nothing to do".format(self.resource.domain.name,
                                                                          self._relay_domains.file_path))

    def delete(self):
        super().delete()
        if self._is_last:
            self._relay_domains.remove_line(self.resource.domain.name)
            self._relay_domains.save()


class DatabaseUserProcessor(ResProcessor):
    def create(self):
        addrs_set = set(self.service.normalize_addrs(self.resource.allowedAddressList))
        LOGGER.info("Creating {0} user {1} with addresses {2}".format(self.service.__class__.__name__,
                                                                      self.resource.name,
                                                                      addrs_set))
        self.service.create_user(self.resource.name, self.resource.passwordHash, list(addrs_set))

    def update(self):
        current_user = self.service.get_user(self.resource.name)
        if not current_user:
            LOGGER.warning("{0} user {1} not found, creating".format(self.service.__class__.__name__,
                                                                     self.resource.name))
            self.create()
            current_user = self.resource
        current_addrs_set = set(self.service.normalize_addrs(current_user.allowedAddressList))
        staging_addrs_set = set(self.service.normalize_addrs(self.resource.allowedAddressList))
        LOGGER.info("Updating {0} user {1}".format(self.service.__class__.__name__, self.resource.name))
        self.service.drop_user(self.resource.name, list(current_addrs_set.difference(staging_addrs_set)))
        self.service.create_user(self.resource.name, self.resource.passwordHash,
                                 list(staging_addrs_set.difference(current_addrs_set)))
        self.service.set_password(self.resource.name, self.resource.passwordHash,
                                  list(current_addrs_set.intersection(staging_addrs_set)))

    def delete(self):
        current_user = self.service.get_user(self.resource.name)
        if current_user:
            LOGGER.info("Dropping {0} user {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_user(self.resource.name, current_user.allowedAddressList)
        else:
            LOGGER.warning("{0} user {1} not found".format(self.service.__class__.__name__, self.resource.name))


class DatabaseProcessor(ResProcessor):
    def create(self):
        LOGGER.info("Creating {0} database {1}".format(self.service.__class__.__name__, self.resource.name))
        self.service.create_database(self.resource.name)
        for user in self.resource.databaseUsers:
            addrs_set = set(self.service.normalize_addrs(user.allowedAddressList))
            LOGGER.info("Granting access on {0} database {1} to user {2} "
                        "with addresses {3}".format(self.service.__class__.__name__, self.resource.name,
                                                    user.name, addrs_set))
            self.service.allow_database_access(self.resource.name, user.name, list(addrs_set))

    def update(self):
        current_database = self.service.get_database(self.resource.name)
        if not current_database:
            LOGGER.warning("{0} database {1} not found, creating".format(self.service.__class__.__name__,
                                                                         self.resource.name))
            self.create()
            current_database = self.resource
        current_usernames_set = set([user.name for user in current_database.databaseUsers])
        staging_usernames_set = set([user.name for user in self.resource.databaseUsers])
        new_users_list = [user for user in self.resource.databaseUsers
                          if user.name in staging_usernames_set.difference(current_usernames_set)]
        old_users_list = [user for user in current_database.databaseUsers
                          if user.name in current_usernames_set.difference(staging_usernames_set)]
        spare_users_list = [user for user in self.resource.databaseUsers
                            if user.name in current_usernames_set.intersection(staging_usernames_set)]
        if not self.resource.writable:
            for user in new_users_list:
                LOGGER.info("Granting READ access on {0} database {1} to "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                self.service.allow_database_reads(self.resource.name, user.name, user.allowedAddressList)
            for user in spare_users_list:
                LOGGER.info("Revoking WRITE access on {0} database {1} from "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                self.service.deny_database_writes(self.resource.name, user.name, user.allowedAddressList)
            for user in old_users_list:
                LOGGER.info("Revoking access on {0} database {1} from "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                self.service.deny_database_access(self.resource.name, user.name, user.allowedAddressList)
        else:
            for user in spare_users_list + new_users_list:
                LOGGER.info("Granting access on {0} database {1} to "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                addrs_set = set(self.service.normalize_addrs(user.allowedAddressList))
                self.service.allow_database_access(self.resource.name, user.name, list(addrs_set))
            for user in old_users_list:
                LOGGER.info("Revoking access on {0} database {1} from "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                self.service.deny_database_access(self.resource.name, user.name, user.allowedAddressList)

    def delete(self):
        current_database = self.service.get_database(self.resource.name)
        if current_database:
            for user in current_database.databaseUsers:
                LOGGER.info("Revoking access on {0} database {1} from "
                            "user {2}".format(self.service.__class__.__name__, self.resource.name, user.name))
                self.service.deny_database_access(self.resource.name, user.name, user.allowedAddressList)
            LOGGER.info("Dropping {0} database {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_database(self.resource.name)
        else:
            LOGGER.warning("{0} database {1} not found".format(self.service.__class__.__name__, self.resource.name))


# TODO: reimplement using taskexecutor.constructor
class ServiceProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self.service = taskexecutor.constructor.Constructor().get_opservice(
            self.resource.serviceType.name,
            template_obj_list=self.resource.serviceTemplates.configTemplates,
            socket_obj_list=self.resource.serviceSockets
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
        elif res_type == "unix-account" and CONFIG.hostname == "baton":
            return UnixAccountProcessorBaton
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
            raise ValueError("Unknown resource type: {}".format(res_type))
