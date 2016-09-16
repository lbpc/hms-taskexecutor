import os
import re
import shutil
from abc import ABCMeta, abstractmethod
from collections import namedtuple
from itertools import product
from taskexecutor.config import Config
from taskexecutor.opservice import Apache, Nginx
from taskexecutor.httpclient import ApiClient
from taskexecutor.dbclient import MySQLClient
from taskexecutor.utils import exec_command, render_template, synchronized
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
        command = "adduser " \
                  "--force-badname " \
                  "--disabled-password " \
                  "--gecos 'Hosting account' " \
                  "--uid {0.uid} " \
                  "--home {0.homeDir} " \
                  "{0.name}".format(self.resource)
        exec_command(command)

    def _setquota(self):
        LOGGER.info("Setting quota for user {0.name}".format(self.resource))
        command = "setquota " \
                  "-g {0.uid} 0 {0.quota} " \
                  "0 0 /home".format(self.resource)
        exec_command(command)

    def _userdel(self):
        LOGGER.info("Deleting user {0.name}".format(self.resource))
        command = "userdel " \
                  "--force " \
                  "--remove " \
                  "{0.name}".format(self.resource)
        exec_command(command)

    def _killprocs(self):
        LOGGER.info("Killing user {0.name}'s processes, "
                    "if any".format(self.resource))
        command = "killall -9 -u {0.name} || true".format(self.resource)
        exec_command(command)

    def create(self):
        self._adduser()
        try:
            self._setquota()
        except Exception:
            LOGGER.error("Setting quota failed "
                         "for user {0.name}".format(self.resource))
            self._userdel()
            raise

    def update(self):
        LOGGER.info("Modifying user {0.name}".format(self.resource))
        command = "usermod " \
                  "--move-home " \
                  "--home {0.homeDir} " \
                  "{0.name}".format(self.resource)
        exec_command(command)

    def delete(self):
        self._killprocs()
        self._userdel()


class DBAccountProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)

    def create(self):
        query = "CREATE USER `{0.name}`@`{1[addr]}` " \
                "IDENTIFIED BY PASSWORD '{1[passHash]}'".format(self.resource,
                                                                self.params)
        LOGGER.info("Executing query: {}".format(query))
        with MySQLClient(Config.mysql.host,
                         Config.mysql.user,
                         Config.mysql.password) as c:
            c.execute(query)

    def update(self):
        self.delete()
        self.create()

    def delete(self):
        query = "DROP USER {0.name}".format(self.resource)
        LOGGER.info("Executing query: {}".format(query))
        with MySQLClient(Config.mysql.host,
                         Config.mysql.user,
                         Config.mysql.password) as c:
            c.execute(query)


class WebSiteProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._apache_obj = self._get_app_server_obj()
        self._nginx = Nginx()
        self._apache = Apache(name=self._apache_obj.name)
        self._fill_params()
        self._set_cfg_paths()

    def _get_app_server_obj(self):
        with ApiClient(Config.apigw.host, Config.apigw.port) as api:
            return api.service(id=self.resource.applicationServerId).get()

    def _build_vhost_obj_list(self):
        _vhosts = list()
        _non_ssl_domains = list()
        _res_dict = self.resource._asdict()
        for domain in self.resource.domains:
            if domain.sslCertificate:
                _res_dict["domains"] = [domain, ]
                _vhosts.append(
                    namedtuple("VHost", _res_dict.keys())(*_res_dict.values())
                )
            else:
                _non_ssl_domains.append(domain)
        _res_dict["domains"] = _non_ssl_domains
        _vhosts.append(
            namedtuple("VHost", _res_dict.keys())(*_res_dict.values())
        )
        return _vhosts

    def _set_cfg_paths(self):
        for srv, srv_type in product((self._apache, self._nginx),
                                     ("available", "enabled")):
            srv.__setattr__("{}_cfg_path".format(srv_type),
                            "{0}/sites-{1}/{2}.conf".format(srv.cfg_base,
                                                            srv_type,
                                                            self.resource.id))

    def _fill_params(self):
        self.params["apache_socket"] = self._apache_obj.serviceSocket[0]
        self.params["apache_name"] = self._apache_obj.name
        self.params["nginx_ip_addr"] = Config.nginx.ip_addr
        self.params["error_pages"] = [
                (code, "http_{}.html".format(code)) for code in
                (403, 404, 502, 503, 504)
        ]
        self.params["static_base"] = Config.paths.nginx_static_base
        self.params[
            "anti_ddos_set_cookie_file"] = "anti_ddos_set_cookie_file.lua"
        self.params[
            "anti_ddos_check_cookie_file"] = "anti_ddos_check_cookie_file.lua"
        self.params["subdomains_document_root"] = \
            "/".join(self.resource.documentRoot.split("/")[:-1])
        self.params["ssl_path"] = Config.paths.ssl_certs

    @staticmethod
    def _save_config(body, file_path):
        LOGGER.info("Saving {}".format(file_path))
        with open("{}.new".format(file_path), "w") as f:
            f.write(body)
        if os.path.exists(file_path):
            os.rename(file_path, "{}.old".format(file_path))
        os.rename("{}.new".format(file_path), file_path)

    @staticmethod
    def _enable_config(available_path, enabled_path):
        LOGGER.info("Linking {0} to {1}".format(available_path, enabled_path))
        try:
            os.symlink(available_path, enabled_path)
        except FileExistsError:
            if os.path.islink(enabled_path) and \
                            os.readlink(enabled_path) == available_path:
                LOGGER.info("Symlink {} already exists".format(enabled_path))
            else:
                raise

    @staticmethod
    def _revert_config(file_path):
        LOGGER.warning("Reverting {0} from {0}.old, {0} will be saved as "
                       "/tmp/te_{1}_error".format(file_path,
                                                  file_path.replace("/", "_")))
        os.rename(file_path,
                  "/tmp/te_{}_error".format(file_path.replace("/", "_")))
        os.rename("{}.old".format(file_path), file_path)

    @staticmethod
    def _drop_backup_config(file_path):
        if os.path.exists("{}.old".format(file_path)):
            LOGGER.info("Removing {}.old".format(file_path))
            os.unlink("{}.old".format(file_path))

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
        self._nginx.config_body = render_template("NginxServer.j2",
                                                  vhosts=_vhosts_list,
                                                  params=self.params)

        self._apache.config_body = render_template("ApacheVHost.j2",
                                                   vhosts=_vhosts_list,
                                                   params=self.params)
        for srv in (self._apache, self._nginx):
            self._save_config(srv.config_body, srv.available_cfg_path)
            self._enable_config(srv.available_cfg_path, srv.enabled_cfg_path)
            try:
                srv.reload()
            except:
                self._revert_config(srv.available_cfg_path)
                raise
            self._drop_backup_config(srv.available_cfg_path)

    def update(self):
        if not self.resource.switchedOn:
            for srv in (self._apache, self._nginx):
                LOGGER.info("Removing {} symlink".format(srv.enabled_cfg_path))
                os.unlink(srv.enabled_cfg_path)
                srv.reload()
        else:
            self.create()

    @synchronized
    def delete(self):
        for srv in (self._nginx, self._apache):
            if os.path.exists(srv.enabled_cfg_path):
                LOGGER.info("Removing {} symlink".format(srv.enabled_cfg_path))
                os.unlink(srv.enabled_cfg_path)
            LOGGER.info("Removing {} file".format(srv.available_cfg_path))
            os.unlink(srv.available_cfg_path)
            srv.reload()


class SSLCertificateProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)

    @synchronized
    def create(self):
        LOGGER.info("Saving {0.name}.pem certificate "
                    "to {1}".format(self.resource,
                                    Config.paths.ssl_certs))
        with open("{0}/{1.name}.pem".format(Config.paths.ssl_certs,
                                            self.resource)) as f:
            f.write(self.resource.certificate)
        LOGGER.info("Saving {0.name}.key key "
                    "to {1}".format(self.resource,
                                    Config.paths.ssl_certs))
        with open("{0}/{1.name}.key".format(Config.paths.ssl_certs,
                                            self.resource)) as f:
            f.write(self.resource.key)

    def update(self):
        self.create(self)

    def delete(self):
        LOGGER.info("Removing {0}/{1.name}.pem and "
                    "{0}/{1.name}.key".format(Config.paths.ssl_certs,
                                              self.resource))
        os.unlink("{0}/{1.name}.pem".format(Config.paths.ssl_certs,
                                            self.resource))
        os.unlink("{0}/{1.name}.key".format(Config.paths.ssl_certs,
                                            self.resource))


class MailboxProcessor(ResProcessor):
    def __init__(self, resource, params):
        super().__init__(resource, params)
        self._is_last = False
        if self.resource.antiSpamEnabled:
            self._avp_file = "/etc/exim4/etc/" \
                             "avp_domains{}".format(self.resource.popServer.id)
            self._avp_domains = self._get_domain_list(self._avp_file)

    @staticmethod
    def _get_domain_list(file):
        with open(file, "r") as f:
            return [s.rstrip("\n\r") for s in f.readlines()]

    @staticmethod
    def _save_domain_list(domains_list, file):
        with open("{}.new".format(file), "w") as f:
            for domain in domains_list:
                f.writelines("{}\n".format(domain))
        os.rename("{}.new".format(file), file)

    @synchronized
    def create(self):
        if self.resource.antiSpamEnabled and \
                        self.resource.domain.name not in self._avp_domains:
            LOGGER.info("Appending {0} to {1}".format(self.resource.domain.name,
                                                      self._avp_file))
            self._avp_domains.append(self.resource.domain.name)
            self._save_domain_list(self._avp_domains, self._avp_file)
        else:
            LOGGER.info("{0.name}@{0.domain.name} "
                        "is not spam protected".format(self.resource))

    def update(self):
        self.create()

    @synchronized
    def delete(self):
        with ApiClient(Config.apigw.host, Config.apigw.port) as api:
            _mailboxes_remaining = \
                api.Mailbox(query={"domain": self.resource.domain.name}).get()
        if len(_mailboxes_remaining) == 1:
            LOGGER.info("{0.name}@{0.domain} is the last mailbox "
                        "in {0.domain}".format(self.resource))
            self._is_last = True
        if self.resource.antiSpamEnabled and self._is_last:
            LOGGER.info("Removing {0.domain.name}"
                        " from {1}".format(self.resource, self._avp_file))
            self._avp_domains.remove(self.resource.domain.name)
            self._save_domain_list(self._avp_domains, self._avp_file)


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
        self._relay_file = "/etc/exim4/etc/" \
                           "relay_domains{}".format(self.resource.popServer.id)
        self._relay_domains = self.get_domain_list(self._relay_file)

    def create(self):
        super().create()
        if self.resource.domain.name not in self._relay_domains:
            LOGGER.info("Appending {0} to {1}".format(self.resource.domain.name,
                                                      self._relay_file))
            self._relay_domains.append(self.resource.domain.name)
            self.save_domain_list(self._relay_domains, self._relay_file)
        else:
            LOGGER.info("{0} already exists in {1}, nothing to do".format(
                    self.resource.domain.name, self._relay_file
            ))

    def delete(self):
        super().delete()
        if self._is_last:
            LOGGER.info("Removing {0.domain.name}"
                        " from {1}".format(self.resource, self._relay_file))
            self._relay_domains.remove(self.resource.domain.name)
            self.save_domain_list(self._relay_domains, self._relay_file)


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
        with MySQLClient(Config.mysql.host,
                         Config.mysql.user,
                         Config.mysql.password) as c:
            c.execute(grant_query)
            c.execute(create_query)

    def update(self):
        pass

    def delete(self):
        query = "DROP DATABASE {0.name}".format(self.resource)
        LOGGER.info("Executing query: {}".format(query))
        with MySQLClient(Config.mysql.host,
                         Config.mysql.user,
                         Config.mysql.password) as c:
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
                                                Config.hostname):
            return MailboxAtPopperProcessor
        elif res_type == "mailbox" and re.match("mx\d+-(mr|dh)",
                                                Config.hostname):
            return MailboxAtMxProcessor
        elif res_type == "mailbox" and re.match("mail-checker\d+",
                                                Config.hostname):
            return MailboxProcessor
        elif res_type == "database":
            return DatabaseProcessor
        else:
            raise ValueError("Unknown resource type: {}".format(res_type))
