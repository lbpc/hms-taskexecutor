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
from taskexecutor.utils import ConfigFile, exec_command, render_template, \
    synchronized
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
        exec_command("adduser "
                     "--force-badname "
                     "--disabled-password "
                     "--gecos 'Hosting account' "
                     "--uid {0.uid} "
                     "--home {0.homeDir} "
                     "{0.name}".format(self.resource))

    def _setquota(self):
        LOGGER.info("Setting quota for user {0.name}".format(self.resource))
        exec_command("setquota "
                     "-g {0.uid} 0 {0.quota} "
                     "0 0 /home".format(self.resource))

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
        _authorized_keys.body = self.params["sshPublicKey"]
        _authorized_keys.save()

    def create(self):
        self._adduser()
        try:
            self._setquota()
        except Exception:
            LOGGER.error("Setting quota failed "
                         "for user {0.name}".format(self.resource))
            self._userdel()
            raise
        if "sshPublicKey" in self.params.keys() and self.params["sshPublicKey"]:
            self._create_authorized_keys()

    def update(self):
        LOGGER.info("Modifying user {0.name}".format(self.resource))
        exec_command("usermod "
                     "--move-home "
                     "--home {0.homeDir} "
                     "--uid {0.uid} "
                     "{0.name}".format(self.resource))
        if "sshPublicKey" in self.params.keys() and self.params["sshPublicKey"]:
            self._create_authorized_keys()

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
                     "pgrep sshd | xargs kill -HUP")

    def _adduser(self):
        LOGGER.info("Adding user {0.name} to system".format(self.resource))
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                     "pw useradd {0.name} "
                     "-u {0.uid} "
                     "-d {0.homeDir} "
                     "-h - "
                     "-s /usr/local/bin/bash "
                     "-c 'Hosting account'".format(self.resource, CONFIG))
        self._update_jailed_ssh("append")

    def _setquota(self):
        LOGGER.info("Setting quota for user {0.name}".format(self.resource))
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                     "edquota "
                     "-g "
                     "-e /home:0:{0.quota} "
                     "{0.uid}".format(self.resource, CONFIG))

    def _userdel(self):
        LOGGER.info("Deleting user {0.name}".format(self.resource))
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                     "pw userdel {0.name} -r".format(self.resource, CONFIG))
        self._update_jailed_ssh("remove")

    def _killprocs(self):
        LOGGER.info("Killing user {0.name}'s processes, "
                    "if any".format(self.resource))
        exec_command("killall -9 -u {0.uid} || true".format(self.resource))

    def update(self):
        LOGGER.info("Modifying user {0.name}".format(self.resource))
        exec_command("jexec "
                     "$(jls -ns | awk -F'[ =]' '/{1.hostname}-a/ {print $12}') "
                     "pw usermod {0.name}"
                     "-u {0.uid} "
                     "-g {0.uid} "
                     "-d {0.homeDir}".format(self.resource, CONFIG))
        if "sshPublicKey" in self.params.keys() and self.params["sshPublicKey"]:
            self._create_authorized_keys()


class DBAccountProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)

    def create(self):
        query = "CREATE USER `{0.name}`@`{1[addr]}` " \
                "IDENTIFIED BY PASSWORD '{1[passHash]}'".format(self.resource,
                                                                self.params)
        LOGGER.info("Executing query: {}".format(query))
        with MySQLClient(**CONFIG.mysql) as c:
            c.execute(query)

    def update(self):
        self.delete()
        self.create()

    def delete(self):
        query = "DROP USER {0.name}".format(self.resource)
        LOGGER.info("Executing query: {}".format(query))
        with MySQLClient(**CONFIG.mysql) as c:
            c.execute(query)


class WebSiteProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._apache_obj = self._get_app_server_obj()
        self._nginx = Nginx()
        self._apache = Apache(name=self._apache_obj.name)
        self._apache.config = ConfigFile("{0}/sites-available/{1}.conf".format(
                self._apache.cfg_base, self.resource.id))
        self._nginx.config = ConfigFile("{0}/sites-available/{1}.conf".format(
                self._nginx.cfg_base, self.resource.id))
        self._fill_params()

    def _get_app_server_obj(self):
        with ApiClient(**CONFIG.apigw.socket) as api:
            return api.service(self.resource.applicationServerId).get()

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
        self.params["apache_socket"] = self._apache_obj.serviceSocket[0]
        self.params["apache_name"] = self._apache_obj.name
        self.params["nginx_ip_addr"] = CONFIG.nginx.ip_addr
        self.params["error_pages"] = \
            [(code, "http_{}.html".format(code)) for code in
             (403, 404, 502, 503, 504)]
        self.params["static_base"] = CONFIG.paths.nginx_static_base
        self.params[
            "anti_ddos_set_cookie_file"] = "anti_ddos_set_cookie_file.lua"
        self.params[
            "anti_ddos_check_cookie_file"] = "anti_ddos_check_cookie_file.lua"
        self.params["subdomains_document_root"] = \
            "/".join(self.resource.documentRoot.split("/")[:-1])
        self.params["ssl_path"] = CONFIG.paths.ssl_certs

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
        self._nginx.config.body = render_template("NginxServer.j2",
                                                  vhosts=_vhosts_list,
                                                  params=self.params)

        self._apache.config.body = render_template("ApacheVHost.j2",
                                                   vhosts=_vhosts_list,
                                                   params=self.params)
        for srv in (self._apache, self._nginx):
            srv.config.write()
            srv.config.enable()
            try:
                srv.reload()
            except:
                srv.config.revert()
                raise
            srv.config.confirm()

    def update(self):
        if not self.resource.switchedOn:
            for srv in (self._apache, self._nginx):
                srv.config.disable()
                srv.reload()
        else:
            self.create()

    @synchronized
    def delete(self):
        for srv in (self._nginx, self._apache):
            if os.path.exists(srv.config.enabled_path):
                srv.config.disable()
            srv.config.delete()
            srv.reload()


# HACK: the only purpose of this class is to process
# WebSite resources at baton.intr
# should be removed when this server is gone
class WebSiteProcessorBaton(WebSiteProcessor):
    def __init__(self, resource, params):
        super(WebSiteProcessor, self).__init__(resource, params)
        _apache_name_mangle = {"apache2-php4": "apache",
                               "apache2-php52": "apache5",
                               "apache2-php53": "apache53"}
        self._apache_obj = self._get_app_server_obj()
        self._nginx = UnmanagedNginx()
        self._apache = UnmanagedApache(
                name=_apache_name_mangle[self._apache_obj.name]
        )
        self._apache.config = ConfigFile("{0}/nvh/{1}".format(
                self._apache.cfg_base, self.resource.id))
        self._apache.config.enabled_path = self._apache.config.file_path
        self._nginx.config = ConfigFile("{0}/servers/{1}.conf".format(
                self._nginx.cfg_base, self.resource.id))
        self._nginx.config.enabled_path = self._nginx.config.file_path
        self._fill_params()

    @synchronized
    def create(self):
        _vhosts_list = self._build_vhost_obj_list()
        self._create_logs_directory()
        self._create_document_root()
        self._nginx.config.body = render_template("BatonNginxServer.j2",
                                                  vhosts=_vhosts_list,
                                                  params=self.params)

        self._apache.config.body = render_template("BatonApacheVHost.j2",
                                                   vhosts=_vhosts_list,
                                                   params=self.params)
        for srv in (self._apache, self._nginx):
            srv.config.write()
            try:
                srv.reload()
            except:
                srv.config.revert()
                raise
            srv.config.confirm()


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
        with ApiClient(**CONFIG.apigw.socket) as api:
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


class DatabaseProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)

    def create(self):
        grant_query = "GRANT " \
                      "SELECT, " \
                      "INSERT, " \
                      "UPDATE, " \
                      "DELETE, " \
                      "CREATE, " \
                      "DROP, " \
                      "REFERENCES, " \
                      "INDEX, " \
                      "ALTER, " \
                      "CREATE TEMPORARY TABLES, " \
                      "LOCK TABLES, " \
                      "CREATE VIEW, " \
                      "SHOW VIEW, " \
                      "CREATE ROUTINE, " \
                      "ALTER ROUTINE, " \
                      "EXECUTE" \
                      " ON `{0.name}`.* TO `{0.user}`@`{1[addr]}%` " \
                      "IDENTIFIED BY PASSWORD " \
                      "'{1[passHash]}'".format(self.resource, self.params)
        create_query = "CREATE DATABASE IF NOT EXISTS {0.name}".format(
                self.resource)
        LOGGER.info("Executing queries: {0}; {1}".format(grant_query,
                                                         create_query))
        with MySQLClient(**CONFIG.mysql) as c:
            c.execute(grant_query)
            c.execute(create_query)

    def update(self):
        pass

    def delete(self):
        query = "DROP DATABASE {0.name}".format(self.resource)
        LOGGER.info("Executing query: {}".format(query))
        with MySQLClient(**CONFIG.mysql) as c:
            c.execute(query)


class ResProcessorBuilder:
    def __new__(cls, res_type):
        if res_type == "unixaccount":
            return UnixAccountProcessor
        elif res_type == "dbaccount":
            return DBAccountProcessor
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
