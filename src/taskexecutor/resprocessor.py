import os
import re
import shutil
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from taskexecutor.config import CONFIG
from taskexecutor.opservice import Apache, Nginx, UnmanagedApache, \
    UnmanagedNginx
from taskexecutor.httpclient import ApiClient
from taskexecutor.dbclient import MySQLClient
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
                     "0 0 /home".format(self.resource.uid, int(quota/1024)))

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
        _authorized_keys = ConfigFile(
            "{0.homeDir}/.ssh/authorized_keys".format(self.resource),
            owner_uid=self.resource.uid,
            mode=0o400
        )
        _authorized_keys.body = self.resource.keyPair.publicKey
        _authorized_keys.save()

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
        _jailed_ssh_config = ConfigFile(
                "/usr/jail/usr/local/etc/ssh/sshd_clients_config")
        _allow_users = _jailed_ssh_config.get_lines("^AllowUsers",
                                                    count=1).split(' ')
        getattr(_allow_users, action)(self.resource.name)
        _jailed_ssh_config.replace_line("^AllowUsers", " ".join(_allow_users))
        _jailed_ssh_config.save()
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
                     "{0}".format(self.resource.uid, int(quota/1024), CONFIG),
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
        self._nginx = self._build_webserver_container("nginx")
        self._apache = self._build_webserver_container("apache")
        self._fill_params()

    def _get_apache_service_obj(self):
        with ApiClient(**CONFIG.apigw) as api:
            return api.service(self.resource.applicationServerId).get()

    def _get_nginx_service_obj(self):
        for service in CONFIG.localserver.services:
            if service.name == "nginx":
                return service
        raise AttributeError("Local server has no nginx service")

    def _get_config_template(self, service_obj, template_name):
        for template in service_obj.serviceTemplate.configTemplates:
            if template.name == template_name:
                with ApiClient(**CONFIG.apigw) as api:
                    return api.get(template.fileLink)
        raise AttributeError("There is no {0} config template "
                             "in {1}".format(template_name,
                                             service_obj.serviceTemplate.name))

    def _build_webserver_container(self, webserver_type):
        if webserver_type == "apache":
            _service = self._get_apache_service_obj()
            _opservice = Apache(name=_service.name)
            _template_name = "ApacheVHost"
        elif webserver_type == "nginx":
            _service = self._get_nginx_service_obj()
            _opservice = Nginx()
            _template_name = "NginxServer"
        else:
            raise KeyError("{} is unknown server type".format(webserver_type))
        _config = ConfigFile("{0}/sites-available/{1}.conf".format(
                _opservice.cfg_base,
                self.resource.id
        ))
        _config.template = self._get_config_template(_service, _template_name)
        return namedtuple("WebServer", "service opservice config")(
            service=_service,
            opservice=_opservice,
            config=_config
        )

    def _build_vhost_obj_list(self):
        _vhosts = list()
        _non_ssl_domains = list()
        _res_dict = self.resource._asdict()
        for domain in self.resource.domains:
            if domain.sslCertificate:
                _res_dict["domains"] = [domain, ]
                _vhosts.append(
                        namedtuple("VHost", _res_dict.keys())(
                                *_res_dict.values())
                )
            else:
                _non_ssl_domains.append(domain)
        _res_dict["domains"] = _non_ssl_domains
        _vhosts.append(
                namedtuple("VHost", _res_dict.keys())(*_res_dict.values())
        )
        return _vhosts

    def _fill_params(self):
        self.params["apache_name"] = self._apache.service.name
        self.params["error_pages"] = \
            [(code, "http_{}.html".format(code)) for code in
             (403, 404, 502, 503, 504)]
        self.params["static_base"] = CONFIG.paths.nginx_static_base
        self.params["ssl_path"] = CONFIG.paths.ssl_certs
        self.params[
            "anti_ddos_set_cookie_file"] = "anti_ddos_set_cookie_file.lua"
        self.params[
            "anti_ddos_check_cookie_file"] = "anti_ddos_check_cookie_file.lua"
        self.params["subdomains_document_root"] = \
            "/".join(self.resource.documentRoot.split("/")[:-1])

    def _create_logs_directory(self):
        _logs_dir = "{}/logs".format(self.resource.unixAccount.homeDir)
        if not os.path.exists(_logs_dir):
            LOGGER.info(
                    "{} directory does not exist, creating".format(_logs_dir)
            )
            os.mkdir(_logs_dir, mode=0o755)

    def _create_document_root(self):
        _docroot_abs = "{0}{1}".format(self.resource.unixAccount.homeDir,
                                       self.resource.documentRoot)
        if not os.path.exists(_docroot_abs):
            LOGGER.info("{} directory does not exist, "
                        "creating".format(_docroot_abs))
            os.makedirs(_docroot_abs, mode=0o755)
            _chown_path = self.resource.unixAccount.homeDir
            for directory in self.resource.documentRoot.split("/")[1:]:
                _chown_path += "/{}".format(directory)
                os.chown(_chown_path,
                         self.resource.unixAccount.uid,
                         self.resource.unixAccount.uid)

    @synchronized
    def create(self):
        _vhosts_list = self._build_vhost_obj_list()
        self._create_logs_directory()
        self._create_document_root()
        for srv in (self._apache, self._nginx):
            srv.config.render_template(socket=srv.service.serviceSocket[0],
                                       vhosts=_vhosts_list,
                                       params=self.params)
            srv.config.write()
            if self.resource.switchedOn and not srv.config.is_enabled:
                srv.config.enable()
            try:
                srv.opservice.reload()
            except:
                srv.config.revert()
                raise
            srv.config.confirm()

    @synchronized
    def update(self):
        if not self.resource.switchedOn:
            for srv in (self._apache, self._nginx):
                if srv.config.is_enabled:
                    srv.config.disable()
                    srv.config.write()
                    srv.config.confirm()
                    srv.opservice.reload()
        else:
            self.create()

    @synchronized
    def delete(self):
        for srv in (self._nginx, self._apache):
            if srv.config.is_enabled:
                srv.config.disable()
            srv.config.delete()
            srv.opservice.reload()


# HACK: the only purpose of this class is to process
# WebSite resources at baton.intr
# should be removed when this server is gone
class WebSiteProcessorBaton(WebSiteProcessor):
    def _build_webserver_container(self, webserver_type):
        if webserver_type == "apache":
            _service = self._get_apache_service_obj()
            _opservice = UnmanagedApache(_service.name)
            _template_name = "BatonApacheVHost"
        elif webserver_type == "nginx":
            _service = self._get_nginx_service_obj()
            _opservice = UnmanagedNginx()
            _template_name = "BatonNginxServer"
        else:
            raise KeyError("{} is unknown server type".format(webserver_type))
        _config = ConfigFile("{0}/sites-available/{1}.conf".format(
                _opservice.cfg_base,
                self.resource.id
        ))
        _config.template = self._get_config_template(_service, _template_name)
        return namedtuple("WebServer", "service opservice config")(
                service=_service,
                opservice=_opservice,
                config=_config
        )


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
            _mailboxes_remaining = \
                api.mailbox(query={"domain": self.resource.domain.name}).get()
        if len(_mailboxes_remaining) == 1:
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

    def create(self):
        queryset = [
            (
                "CREATE USER `%s`@`%s` IDENTIFIED BY PASSWORD '%s'",
                (self.resource.name, addr, self.resource.passwordHash)
            ) for addr in self.resource.addressList
        ]
        with MySQLClient(**CONFIG.mysql) as c:
            for query, values in queryset:
                LOGGER.info("Executing query: {}".format(query % values))
                c.execute(query, values)

    def update(self):
        self.delete()
        self.create()

    def delete(self):
        query = "DROP USER %s"
        with MySQLClient(**CONFIG.mysql) as c:
            LOGGER.info("Executing query: "
                        "{}".format(query % self.resource.name))
            c.execute(query, (self.resource.name,))


class DatabaseProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)

    def create(self):
        queryset = [
            (
                "CREATE DATABASE IF NOT EXISTS %s", (self.resource.name,)
            ),
        ]
        for user in self.resource.databaseUsers:
            queryset += [
                ("GRANT "
                 "SELECT, "
                 "INSERT, "
                 "UPDATE, "
                 "DELETE, "
                 "CREATE, "
                 "DROP, "
                 "REFERENCES, "
                 "INDEX, "
                 "ALTER, "
                 "CREATE TEMPORARY TABLES, "
                 "LOCK TABLES, "
                 "CREATE VIEW, "
                 "SHOW VIEW, "
                 "CREATE ROUTINE, "
                 "ALTER ROUTINE, "
                 "EXECUTE"
                 " ON `%s`.* TO `%s`@`%s` "
                 "IDENTIFIED BY PASSWORD '%s'",
                 (self.resource.name, user.name, addr, user.passwordHash))
                for addr in user.addressList
            ]
        with MySQLClient(**CONFIG.mysql) as c:
            for query, values in queryset:
                LOGGER.info("Executing query: {}".format(query % values))
                c.execute(query, values)

    def update(self):
        if not self.resource.writable:
            query_action = "REVOKE"
        else:
            query_action = "GRANT"

        queryset = list()
        for user in self.resource.databaseUsers:
            queryset += [
                ("{0} INSERT, UPDATE ON "
                 "`%s`.* FROM `%s`@`%s`".format(query_action),
                 (self.resource.name, user.name, addr))
                for addr in user.addressList
            ]
        with MySQLClient(**CONFIG.mysql) as c:
            for query, values in queryset:
                LOGGER.info("Executing query: {}".format(query % values))
                c.execute(query, values)

    def delete(self):
        query = "DROP DATABASE %s"
        with MySQLClient(**CONFIG.mysql) as c:
            LOGGER.info(
                    "Executing query: {}".format(query % self.resource.name)
            )
            c.execute(query, (self.resource.name,))


class ServiceProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        if self.resource.name == "nginx":
            self._opservice = Nginx()
        elif self.resource.name.startswith("apache"):
            self._opservice = Apache(name=self.resource.name)
        self._excluded_templates = ("NginxServer", "ApacheVHost")

    def create(self):
        _all_configs = list()
        for config_template in self.resource.serviceTemplate.configTemplates:
            if config_template.name not in self._excluded_templates:
                _config = ConfigFile("{0}/{1}".format(self._opservice.cfg_base,
                                                      config_template.name))
                _all_configs.append(_config)
                with ApiClient(**CONFIG.apigw) as api:
                    _config.template = api.get(config_template.fileLink)
                _config.render_template(service=self.resource,
                                        params=self.params)
                _config.write()
        try:
            self._opservice.reload()
        except:
            for config in _all_configs:
                config.revert()
            raise
        for config in _all_configs:
            config.confirm()

    def update(self):
        self.create()

    def delete(self):
        for config_template in self.resource.serviceTemplate.configTemplates:
            _config = ConfigFile("{0}/{1}".format(self._opservice.cfg_base,
                                                  config_template.name))
            _config.delete()
        self._opservice.stop()


class ServiceProcessorBaton(ServiceProcessor):
    def __init__(self, resource, params):
        super(ServiceProcessor, self).__init__(resource, params)
        if self.resource.name == "nginx":
            self._opservice = UnmanagedNginx()
        elif self.resource.name.startswith("apache"):
            self._opservice = UnmanagedApache(name=self.resource.name)
        self._excluded_templates = ("BatonNginxServer", "BatonApacheVHost")


class ResProcessorBuilder:
    def __new__(cls, res_type):
        if res_type == "service" and CONFIG.hostname == "baton":
            return ServiceProcessorBaton
        elif res_type == "service":
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
