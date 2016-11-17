import os
import re
import shutil
from pytransliter import translit
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from taskexecutor.config import CONFIG
from taskexecutor.opservice import OpServiceBuilder
from taskexecutor.httpsclient import ApiClient
from taskexecutor.utils import ConfigFile, exec_command, synchronized
from taskexecutor.logger import LOGGER


class ResProcessor(metaclass=ABCMeta):
    def __init__(self, resource, params):
        self._resource = object()
        self._params = dict()
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
    def params(self):
        return self._params

    @params.setter
    def params(self, value):
        self._params = value

    @params.deleter
    def params(self):
        del self._params

    @abstractmethod
    def create(self):
        pass

    @abstractmethod
    def update(self):
        pass

    @abstractmethod
    def delete(self):
        pass


class UnixAccountProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)

    def _adduser(self):
        LOGGER.info("Adding user {0.name} to system".format(self.resource))
        exec_command("useradd "
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
        exec_command("setquota "
                     "-g {0} 0 {1} "
                     "0 0 /home".format(self.resource.uid, int(quota / 1024)))

    def _userdel(self):
        LOGGER.info("Deleting user {0.name}".format(self.resource))
        exec_command("userdel "
                     "--force "
                     "--remove "
                     "{0.name}".format(self.resource))

    def _killprocs(self):
        LOGGER.info("Killing user {0.name}'s processes, "
                    "if any".format(self.resource))
        exec_command("killall -9 -u {0.name} || true".format(self.resource))

    def _create_authorized_keys(self):
        if not os.path.exists("{0.homeDir}/.ssh".format(self.resource)):
            os.mkdir("{0.homeDir}/.ssh".format(self.resource), mode=0o700)
        authorized_keys = ConfigFile(
            "{0.homeDir}/.ssh/authorized_keys".format(self.resource),
            owner_uid=self.resource.uid,
            mode=0o400
        )
        authorized_keys.body = self.resource.keyPair.publicKey
        authorized_keys.save()

    def _create_crontab(self):
        crontab_string = str()
        for entry in self.resource.crontab:
            if entry.switchedOn:
                crontab_string = ("{0}"
                                  "#{1.name}\n"
                                  "#{1.execTimeDescription}\n"
                                  "{1.execTime} {1.command}\n").format(
                        crontab_string, entry)
        LOGGER.info("Installing '{0}' crontab for {1}".format(
                crontab_string, self.resource.name))
        exec_command("crontab -u {} -".format(self.resource.name),
                     pass_to_stdin=crontab_string)

    def _delete_crontab_if_present(self):
        if os.path.exists(
                "/var/spool/cron/crontabs/{}".format(self.resource.name)
        ):
            LOGGER.info("Deleting {} crontab".format(self.resource.name))
            exec_command("crontab -u {} -r".format(self.resource.name))

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
        jailed_ssh_config = ConfigFile(
                "/usr/jail/usr/local/etc/ssh/sshd_clients_config")
        allow_users = jailed_ssh_config.get_lines("^AllowUsers",
                                                  count=1).split(' ')
        getattr(allow_users, action)(self.resource.name)
        jailed_ssh_config.replace_line("^AllowUsers", " ".join(allow_users))
        jailed_ssh_config.save()
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                     "pgrep sshd | xargs kill -HUP",
                     shell="/usr/local/bin/bash")

    def _adduser(self):
        LOGGER.info("Adding user {0.name} to system".format(self.resource))
        exec_command("jexec "
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
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{2.hostname}-a/ {print $12}') "
                     "edquota "
                     "-g "
                     "-e /home:0:{1} "
                     "{0}".format(self.resource.uid, int(quota / 1024), CONFIG),
                     shell="/usr/local/bin/bash")

    def _userdel(self):
        LOGGER.info("Deleting user {0.name}".format(self.resource))
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                     "pw userdel {0.name} -r".format(self.resource, CONFIG),
                     shell="/usr/local/bin/bash")
        self._update_jailed_ssh("remove")

    def _killprocs(self):
        LOGGER.info("Killing user {0.name}'s processes, "
                    "if any".format(self.resource))
        exec_command("killall -9 -u {0.uid} || true".format(self.resource),
                     shell="/usr/local/bin/bash")

    def _create_crontab(self):
        crontab_string = str()
        for entry in self.resource.crontab:
            if entry.switchedOn:
                crontab_string = ("{0}"
                                  "{1.execTime} {1.command}\n").format(
                        crontab_string, entry)
        LOGGER.info("Installing '{0}' crontab for {1}".format(
                crontab_string, self.resource.name))
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                     "crontab -u {0.name} -".format(self.resource, CONFIG),
                     pass_to_stdin=crontab_string)

    def _delete_crontab_if_present(self):
        if os.path.exists(
                "/usr/jail/var/cron/tabs/{}".format(self.resource.name)
        ):
            LOGGER.info("Deleting {} crontab".format(self.resource.name))
            exec_command("jexec "
                         "$(jls -ns | "
                         "awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                         "crontab -u {0.name} -r".format(self.resource, CONFIG))


class WebSiteProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._nginx = OpServiceBuilder(self._get_nginx_service_obj())
        self._apache = OpServiceBuilder(self._get_apache_service_obj())
        self._fill_params()

    def _get_apache_service_obj(self):
        with ApiClient(**CONFIG.apigw) as api:
            return api.Service(self.resource.serviceId).get()

    def _get_nginx_service_obj(self):
        for service in CONFIG.localserver.services:
            if service.serviceType.name == "STAFF_NGINX":
                return service
        raise AttributeError("Local server has no nginx service")

    def _build_vhost_obj_list(self):
        vhosts = list()
        non_ssl_domains = list()
        res_dict = self.resource._asdict()
        for domain in self.resource.domains:
            # FIXME: there is no real need in attribute presence check
            if hasattr(domain, "sslCertificate") and domain.sslCertificate:
                res_dict["domains"] = [domain, ]
                vhosts.append(
                        namedtuple("VHost", res_dict.keys())(
                                *res_dict.values())
                )
            else:
                non_ssl_domains.append(domain)
        res_dict["domains"] = non_ssl_domains
        vhosts.append(
                namedtuple("VHost", res_dict.keys())(*res_dict.values())
        )
        return vhosts

    def _fill_params(self):
        self.params["apache_name"] = self._apache.name
        self.params["error_pages"] = \
            [(code, "http_{}.html".format(code)) for code in
             (403, 404, 502, 503, 504)]
        self.params["static_base"] = CONFIG.nginx.static_base_path
        self.params["ssl_path"] = CONFIG.nginx.ssl_certs_path
        self.params["anti_ddos_location"] = CONFIG.nginx.anti_ddos_location
        self.params["anti_ddos_set_cookie_file"] = \
            CONFIG.nginx.anti_ddos_set_cookie_file
        self.params["anti_ddos_check_cookie_file"] = \
            CONFIG.nginx.anti_ddos_check_cookie_file
        self.params["subdomains_document_root"] = \
            "/".join(self.resource.documentRoot.split("/")[:-1])

    def _create_logs_directory(self):
        logs_dir = "{}/logs".format(self.resource.unixAccount.homeDir)
        if not os.path.exists(logs_dir):
            LOGGER.info(
                    "{} directory does not exist, creating".format(logs_dir)
            )
            os.makedirs(logs_dir, mode=0o755)

    def _create_document_root(self):
        docroot_abs = os.path.join(self.resource.unixAccount.homeDir,
                                   self.resource.documentRoot)
        if not os.path.exists(docroot_abs):
            LOGGER.info("{} directory does not exist, "
                        "creating".format(docroot_abs))
            os.makedirs(docroot_abs, mode=0o755)
            chown_path = self.resource.unixAccount.homeDir
            for directory in docroot_abs.split("/")[1:]:
                chown_path += "/{}".format(directory)
                os.chown(chown_path,
                         self.resource.unixAccount.uid,
                         self.resource.unixAccount.uid)

    @synchronized
    def create(self):
        vhosts_list = self._build_vhost_obj_list()
        self._create_logs_directory()
        self._create_document_root()
        for service in (self._apache, self._nginx):
            config = service.get_website_config(self.resource.id)
            config.render_template(socket=service.socket,
                                   vhosts=vhosts_list,
                                   params=self.params)
            config.write()
            if self.resource.switchedOn and not config.is_enabled:
                config.enable()
            try:
                service.reload()
            except:
                config.revert()
                raise
            config.confirm()

    @synchronized
    def update(self):
        if not self.resource.switchedOn:
            for service in (self._apache, self._nginx):
                config = service.get_website_config(self.resource.id)
                if config.is_enabled:
                    config.disable()
                    config.write()
                    config.confirm()
                    service.reload()
        else:
            self.create()

    @synchronized
    def delete(self):
        for service in (self._nginx, self._apache):
            config = service.get_website_config(self.resource.id)
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
        res_dict["documentRoot"] = translit(res_dict["documentRoot"], "ru")
        self.resource = namedtuple("ApiObject",
                                   res_dict.keys())(*res_dict.values())


class SSLCertificateProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._cert_file = ConfigFile(
                "{0}/{1.name}.pem".format(CONFIG.paths.ssl_certs, self.resource)
        )
        self._key_file = ConfigFile(
                "{0}/{1.name}.key".format(CONFIG.paths.ssl_certs, self.resource)
        )

    @synchronized
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
            self._avp_domains = ConfigFile(
                    "/etc/exim4/etc/avp_domains{}".format(
                            self.resource.popServer.id))

    @synchronized
    def create(self):
        if self.resource.antiSpamEnabled and not self._avp_domains.has_line(
                self.resource.domain.name):
            self._avp_domains.add_line(self.resource.domain.name)
            self._avp_domains.save()
        else:
            LOGGER.info("{0.name}@{0.domain.name} "
                        "is not spam protected".format(self.resource))

    def update(self):
        self.create()

    @synchronized
    def delete(self):
        with ApiClient(**CONFIG.apigw) as api:
            mailboxes_remaining = \
                api.Mailbox(query={"domain": self.resource.domain.name}).get()
        if len(mailboxes_remaining) == 1:
            LOGGER.info("{0.name}@{0.domain} is the last mailbox "
                        "in {0.domain}".format(self.resource))
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
        LOGGER.info("Removing {0.mailSpool]}/{0.name} "
                    "recursively".format(self.resource))
        shutil.rmtree("{0.mailSpool]}/{0.name}".format(self.resource))
        if self._is_last:
            LOGGER.info("{0.mailSpool}/{0.name} was the last maildir, "
                        "removing spool itself".format(self.resource))
            os.rmdir(self.resource.mailSpool)


class MailboxAtMxProcessor(MailboxProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._relay_domains = ConfigFile(
                "/etc/exim4/etc/relay_domains{}".format(
                    self.resource.popServer.id))

    def create(self):
        super().create()
        if not self._relay_domains.has_line(self.resource.domain.name):
            self._relay_domains.add_line(self.resource.domain.name)
            self._relay_domains.save()
        else:
            LOGGER.info("{0} already exists in {1}, nothing to do".format(
                    self.resource.domain.name, self._relay_domains.file_path
            ))

    def delete(self):
        super().delete()
        if self._is_last:
            self._relay_domains.remove_line(self.resource.domain.name)
            self._relay_domains.save()


class DatabaseUserProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._service = OpServiceBuilder(self.resource.serviceType.name)

    def create(self):
        self._service.add_create_user_query(self.resource.name,
                                            self.resource.passwordHash,
                                            self.resource.addressList)
        self._service.run_queryset()

    def update(self):
        self._service.add_update_password_query(self.resource.name,
                                                self.resource.passwordHash,
                                                self.resource.addressList)
        self._service.run_queryset()

    def delete(self):
        self._service.add_drop_user_query(self.resource.name)
        self._service.run_queryset()


class DatabaseProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._service = OpServiceBuilder(self.resource.serviceType.name)

    def create(self):
        privileges = \
            CONFIG.mysql.common_privileges + CONFIG.mysql.write_privileges
        self._service.add_create_database_query(self.resource.name,
                                                self.resource.databaseUsers)
        self._service.add_grant_privileges_query(self.resource.name,
                                                 self.resource.databaseUsers,
                                                 privileges)
        self._service.run_queryset()

    def update(self):
        if not self.resource.writable:
            self._service.add_revoke_privileges_query(
                    self.resource.name,
                    self.resource.databaseUsers,
                    CONFIG.mysql.write_privileges
            )
        else:
            self._service.add_grant_privileges_query(
                    self.resource.name,
                    self.resource.databaseUsers,
                    CONFIG.mysql.write_privileges
            )
        self._service.run_queryset()

    def delete(self):
        self._service.add_drop_database_query(self.resource.name)
        self._service.run_queryset()


# TODO: reimplement using OpServiceBuilder
class ServiceProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._service = OpServiceBuilder(self.resource)

    def create(self):
        pass

    def update(self):
        pass

    def delete(self):
        pass


class ResProcessorBuilder:
    def __new__(cls, res_type):
        if res_type == "service":
            return ServiceProcessor
        elif res_type == "unix-account" and CONFIG.hostname == "baton":
            return UnixAccountProcessorBaton
        elif res_type == "unix-account":
            return UnixAccountProcessor
        elif res_type == "database-user":
            return DatabaseUserProcessor
        elif res_type == "website" and CONFIG.hostname == "baton":
            return WebSiteProcessorBaton
        elif res_type == "website":
            return WebSiteProcessor
        elif res_type == "sslcertificate":
            return SSLCertificateProcessor
        elif res_type == "mailbox" and re.match("pop\d+",
                                                CONFIG.hostname):
            return MailboxAtPopperProcessor
        elif res_type == "mailbox" and re.match("mx\d+-(mr|dh)",
                                                CONFIG.hostname):
            return MailboxAtMxProcessor
        elif res_type == "mailbox" and re.match("mail-checker\d+",
                                                CONFIG.hostname):
            return MailboxProcessor
        elif res_type == "database":
            return DatabaseProcessor
        else:
            raise ValueError("Unknown resource type: {}".format(res_type))
