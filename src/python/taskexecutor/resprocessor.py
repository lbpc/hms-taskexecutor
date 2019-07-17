import os
import shutil
import sys
import abc
import collections
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.conffile
import taskexecutor.ftpclient
import taskexecutor.httpsclient
import taskexecutor.opservice
import taskexecutor.watchdog
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

    def _process_data(self, src_uri, dst_uri, extra_postproc_args={}):
        datafetcher = taskexecutor.constructor.get_datafetcher(src_uri, dst_uri, self.params.get("dataSourceParams"))
        datafetcher.fetch()
        data_postprocessor_type = self.params.get("dataPostprocessorType")
        data_postprocessor_args = self.params.get("dataPostprocessorArgs") or {}
        if data_postprocessor_type:
            data_postprocessor_args.update(extra_postproc_args)
            postprocessor = taskexecutor.constructor.get_datapostprocessor(data_postprocessor_type,
                                                                           data_postprocessor_args)
            postprocessor.process()

    def __str__(self):
        return "{0}(resource=(name={1.name}, id={1.id}))".format(self.__class__.__name__, self.resource)


class UnixAccountProcessor(ResProcessor):
    @taskexecutor.utils.synchronized
    def create(self):
        if self.op_resource:
            LOGGER.warning("User {0.name} already exists, updating".format(self.resource))
            self.update()
            return
        LOGGER.info("Adding user {0.name} to system".format(self.resource))
        shell = {True: self.service.default_shell, False: self.service.disabled_shell}[self.resource.switchedOn]
        self.service.create_user(self.resource.name,
                                 self.resource.uid,
                                 self.resource.homeDir,
                                 self.resource.passwordHash,
                                 shell,
                                 "Hosting account,,,,"
                                 "UnixAccount(id={0.id}, "
                                 "accountId={0.accountId}, "
                                 "writable={0.writable})".format(self.resource),
                                 CONFIG.unix_account.groups)
        try:
            LOGGER.info("Setting quota for user {0.name}: {0.quota} bytes".format(self.resource))
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
        if not "dataSourceParams" in self.params.keys():
            self.params["dataSourceParams"] = {}
        self.params["dataSourceParams"]["ownerUid"] = self.params["dataSourceParams"].get("ownerUid") or self.resource.uid
        data_dest_uri = self.params.get("datadestinationUri", "file://{}".format(self.resource.homeDir))
        data_source_uri = self.params.get("datasourceUri") or data_dest_uri
        self._process_data(data_source_uri, data_dest_uri, {"dataType": "directory", "path": self.resource.homeDir})

    @taskexecutor.utils.synchronized
    def update(self):
        if self.op_resource:
            switched_on = self.resource.switchedOn and not self.params.get("forceSwitchOff")
            LOGGER.info("Modifying user {0.name}".format(self.resource))
            if self.resource.uid != self.op_resource.uid:
                LOGGER.warning("UnixAccount {0} UID changed from {1} "
                               "to: {2}".format(self.resource.name, self.op_resource.uid, self.resource.uid))
                self.service.change_uid(self.resource.name, self.resource.uid)
                taskexecutor.utils.exec_command("chown -R {0}:{0} {1}".format(self.resource.uid, self.resource.homeDir))
            self.service.set_shell(self.resource.name,
                                   {True: self.service.default_shell, False: None}[switched_on])
            if self.resource.sendmailAllowed:
                self.service.enable_sendmail(self.resource.uid)
            else:
                self.service.disable_sendmail(self.resource.uid)
            if not self.resource.writable:
                LOGGER.info("Disabling writes by setting quota=quotaUsed for user {0.name} "
                            "(quotaUsed={0.quotaUsed})".format(self.resource))
                self.service.set_quota(self.resource.uid, self.resource.quotaUsed)
            else:
                LOGGER.info("Setting quota for user {0.name}: {0.quota} bytes".format(self.resource))
                self.service.set_quota(self.resource.uid, self.resource.quota)
            if not "dataSourceParams" in self.params.keys():
                self.params["dataSourceParams"] = {}
            self.params["dataSourceParams"]["ownerUid"] = self.params["dataSourceParams"].get("ownerUid") or self.resource.uid
            data_dest_uri = self.params.get("datadestinationUri", "file://{}".format(self.resource.homeDir))
            data_source_uri = self.params.get("datasourceUri") or data_dest_uri
            self._process_data(data_source_uri, data_dest_uri, {"dataType": "directory", "path": self.resource.homeDir})
            if hasattr(self.resource, "keyPair") and self.resource.keyPair:
                LOGGER.info("Creating authorized_keys for user {0.name}".format(self.resource))
                self.service.create_authorized_keys(self.resource.keyPair.publicKey,
                                                    self.resource.uid, self.resource.homeDir)
            if len(self.resource.crontab) > 0 and switched_on:
                self.service.create_crontab(self.resource.name,
                                            [task for task in self.resource.crontab if task.switchedOn])
            else:
                self.service.delete_crontab(self.resource.name)
            self.service.set_comment(self.resource.name, "Hosting account,,,,"
                                                         "UnixAccount(id={0.id}, "
                                                         "accountId={0.accountId}, "
                                                         "writable={0.writable})".format(self.resource))
            if not self.resource.infected:
                taskexecutor.watchdog.ProcessWatchdog.get_uids_queue().put(-self.resource.uid)
            LOGGER.info("Creating 'logs' directory")
            os.makedirs(os.path.join(self.resource.homeDir, "logs"), mode=0o755, exist_ok=True)
        else:
            LOGGER.warning("UnixAccount {0} not found, creating".format(self.resource.name))
            self.create()

    @taskexecutor.utils.synchronized
    def delete(self):
        self.service.kill_user_processes(self.resource.name)
        self.service.delete_user(self.resource.name)


class WebSiteProcessor(ResProcessor):
    @property
    def _without_reload(self):
        return self.params.get("required_for", [None])[0] == "service" or \
               "appscat" in self.params.get("provider", [None])

    def _build_vhost_obj_list(self):
        vhosts = list()
        non_ssl_domains = list()
        res_dict = self.resource._asdict()
        for domain in (d for d in self.resource.domains if d.switchedOn):
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
                           subdomains_document_root="/".join(str(self.resource.documentRoot).split("/")[:-1]))
        vhosts_list = self._build_vhost_obj_list()
        home_dir = os.path.normpath(str(self.resource.unixAccount.homeDir))
        document_root = os.path.normpath(str(self.resource.documentRoot))
        document_root_abs = os.path.join(home_dir, document_root)
        opcache_root = os.path.join("/opcache", self.resource.id)
        if os.path.exists(opcache_root):
            shutil.rmtree(opcache_root, ignore_errors=True)
        for directory in (os.path.join(home_dir, "logs"), document_root_abs, opcache_root):
            if not os.path.islink(directory):
                os.makedirs(directory, mode=0o755, exist_ok=True)
            else:
                LOGGER.warning("{} is symbolic link".format(directory))
        for directory in map(lambda d: os.path.join(home_dir, d),
                             ["/".join(document_root.split("/")[0:i + 1])
                              for i, d in enumerate(document_root.split("/"))]):
            if os.path.exists(directory):
                os.chown(directory, self.resource.unixAccount.uid, self.resource.unixAccount.uid)
            else:
                LOGGER.warning("{} does not exist".format(directory))
        os.chown(opcache_root, self.resource.unixAccount.uid, self.resource.unixAccount.uid)
        services = []
        if self.params.get("oldHttpProxyIp") != self.extra_services.http_proxy.socket.http.address:
            services.append(self.service)
        services.append(self.extra_services.http_proxy)
        for service in services:
            config = service.get_website_config(self.resource.id)
            config.render_template(service=service, vhosts=vhosts_list, params=self.params)
            config.write()
            if self.resource.switchedOn and not config.is_enabled:
                config.enable()
            if not self._without_reload:
                try:
                    service.reload()
                except:
                    config.revert()
                    raise
            config.confirm()
        data_dest_uri = self.params.get("datadestinationUri", "file://{}".format(document_root_abs))
        data_source_uri = self.params.get("datasourceUri") or data_dest_uri
        given_postproc_args = self.params.get("dataPostprocessorArgs") or {}
        env = given_postproc_args.get("env") or {}
        env["DOCUMENT_ROOT"] = document_root_abs
        domain = next((d for d in self.resource.domains if d.name == env.get("DOMAIN_NAME")), self.resource.domains[0])
        env["DOMAIN_NAME"] = domain.name.encode("idna").decode()
        env["PROTOCOL"] = "https" if domain.sslCertificate and domain.sslCertificate.switchedOn else "http"
        postproc_args = dict(cwd=document_root_abs,
                             hosts={env["DOMAIN_NAME"]: self.extra_services.http_proxy.socket.http.address},
                             uid=self.resource.unixAccount.uid,
                             dataType="directory",
                             env=env)
        self._process_data(data_source_uri, data_dest_uri, postproc_args)

    @taskexecutor.utils.synchronized
    def update(self):
        if not self.resource.switchedOn:
            for service in (self.service, self.extra_services.http_proxy):
                config = service.get_website_config(self.resource.id)
                if config.is_enabled:
                    config.disable()
                    config.save()
                    if not self._without_reload:
                        service.reload()
        else:
            self.create()
            if self.extra_services.old_app_server and (self.extra_services.old_app_server.name != self.service.name or
                                                       type(self.extra_services.old_app_server) != type(self.service)):
                LOGGER.info("Removing config from old application server "
                            "{}".format(self.extra_services.old_app_server.name))
                config = self.extra_services.old_app_server.get_website_config(self.resource.id)
                config.disable()
                config.delete()
                if not self._without_reload:
                    self.extra_services.old_app_server.reload()

    @taskexecutor.utils.synchronized
    def delete(self):
        shutil.rmtree(os.path.join("/opcache", self.resource.id), ignore_errors=True)
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
        self.resource = collections.namedtuple("ApiObject", res_dict.keys())(*res_dict.values())


class SslCertificateProcessor(ResProcessor):
    @taskexecutor.utils.synchronized
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
            LOGGER.info("{0.name} should be restricted to use no more than "
                        "{0.maxCpuTimePerSecond} CPU seconds per wall clock second".format(self.resource))
            self.service.restrict_user_cpu(self.resource.name, self.resource.maxCpuTimePerSecond)
        else:
            self.service.unrestrict_user_cpu(self.resource.name)

    def _apply_customizations(self):
        vars = getattr(self.resource, "sessionVariables", {})
        if not isinstance(vars, dict):
            vars = vars._asdict()
        if len(set(vars.keys()).intersection({"queryCacheType", "characterSetClient", "characterSetConnection",
                                              "characterSetResults", "collationConnection"})) > 0:
            vars = {taskexecutor.utils.to_snake_case(k): v for k, v in vars.items()}
            addrs_set = set(self.service.normalize_addrs(self.resource.allowedIPAddresses))
            LOGGER.info("Presetting session variables for user {0} with addresses {1}: {2}".format(
                    self.resource.name,
                    addrs_set,
                    ", ".join(("{}={}".format(k ,v) for k, v in vars.items()))
            ))
            self.service.preset_user_session_vars(self.resource.name, list(addrs_set), vars)

    def create(self):
        if not self.op_resource:
            addrs_set = set(self.service.normalize_addrs(self.resource.allowedIPAddresses))
            LOGGER.info("Creating {0} user {1} with addresses {2}".format(self.service.__class__.__name__,
                                                                          self.resource.name,
                                                                          addrs_set))
            self.service.create_user(self.resource.name, self.resource.passwordHash, list(addrs_set))
            self.service.set_initial_permissions(self.resource.name, list(addrs_set))
            self._apply_restrictions()
            self._apply_customizations()
        else:
            LOGGER.warning("{0} user {1} already exists, updating".format(self.service.__class__.__name__,
                                                                          self.resource.name))
            self.update()

    def update(self):
        if not self.resource.switchedOn or self.params.get("forceSwitchOff"):
            LOGGER.info("User {0} is switched off, deleting".format(self.resource.name))
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
            self.service.set_initial_permissions(self.resource.name, list(staging_addrs_set))
            self._apply_restrictions()
            self._apply_customizations()
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
        data_dest_uri = self.params.get("datadestinationUri",
                                        "mysql://{}/{}".format(CONFIG.hostname, self.resource.name))
        data_source_uri = self.params.get("datasourceUri") or data_dest_uri
        self._process_data(data_source_uri, data_dest_uri, dict(name=self.resource.name,
                                                                dataType="database",
                                                                dbServer=self.service))

    def update(self):
        if "dataSourceParams" in self.params.keys() and self.params["dataSourceParams"].get("deleteExtraneous", False):
            LOGGER.info("Data cleanup requested, "
                        "dropping {0} database {1}".format(self.service.__class__.__name__, self.resource.name))
            self.service.drop_database(self.resource.name)
            self.op_resource = None
            self.create()
            return
        database_users = self.resource.databaseUsers
        if self.params.get("delete"):
            database_users.remove(self.params["delete"])
        if self.op_resource:
            current_usernames_set = set((user.name for user in self.op_resource.databaseUsers))
            staging_usernames_set = set((user.name for user in database_users if user.switchedOn))
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
            data_dest_uri = self.params.get("datadestinationUri",
                                            "mysql://{}/{}".format(CONFIG.hostname, self.resource.name))
            data_source_uri = self.params.get("datasourceUri") or data_dest_uri
            self._process_data(data_source_uri, data_dest_uri, dict(name=self.resource.name,
                                                                    dataType="database",
                                                                    dbServer=self.service))
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
        if isinstance(self.service, taskexecutor.opservice.NginxInDocker):
            self.params["app_servers"] = taskexecutor.constructor.get_all_opservices_by_res_type("website")
        if isinstance(self.service, taskexecutor.opservice.Nginx):
            self._create_error_pages()
            self.params.update(app_servers=taskexecutor.constructor.get_all_opservices_by_res_type("website"),
                               anti_ddos_location=CONFIG.nginx.anti_ddos_location)
        elif isinstance(self.service, taskexecutor.opservice.Apache):
            self.params.update(admin_networks=CONFIG.apache.admin_networks)
        configs = self.service.get_concrete_configs_set()
        if isinstance(self.service, taskexecutor.opservice.Apache) and self.service.interpreter.name != "php":
            configs = [c for c in configs if os.path.basename(c.file_path) != "php.ini"]
        for config in configs:
            if isinstance(config, taskexecutor.conffile.SwitchableConfigFile):
                for s in ["available", "enabled"]:
                    os.makedirs(os.path.join(self.service.config_base_path, "sites-{}".format(s)), exist_ok=True)
            config.render_template(service=self.service, params=self.params)
            config.write()
        try:
            if self.service.status() is taskexecutor.opservice.UP:
                self.service.reload()
            else:
                LOGGER.warning("{} is down, starting it".format(self.service.name))
                self.service.start()
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
        pass

    def delete(self):
        LOGGER.info("Deleting {0} file at {1}".format(self._archive_filename, self._archive_storage.host))
        self._archive_storage.delete(self._archive_filename)


class RedirectProcessor(ResProcessor):
    @property
    def _without_reload(self):
        return self.params.get("required_for", [None])[0] == "service"

    @taskexecutor.utils.synchronized
    def create(self):
        res_dict = self.resource._asdict()
        res_dict['domains'] = [res_dict.get('domain')]
        del res_dict['domain']
        vhost = collections.namedtuple("VHost", res_dict.keys())(*res_dict.values())
        config = self.service.get_website_config(self.resource.id)
        config.render_template(service=self.service, vhosts=[vhost], params=self.params)
        config.write()
        if self.resource.switchedOn and not config.is_enabled:
            config.enable()
        if not self._without_reload:
            try:
                self.service.reload()
            except:
                config.revert()
                raise
        config.confirm()

    def update(self):
        if self.resource.switchedOn:
            self.create()
        else:
            self.delete()

    @taskexecutor.utils.synchronized
    def delete(self):
        config = self.service.get_website_config(self.resource.id)
        if not os.path.exists(config.file_path):
            LOGGER.warning("{} does not exist".format(config.file_path))
            return
        if config.is_enabled:
            config.disable()
        config.delete()
        self.service.reload()


class Builder:
    def __new__(cls, res_type):
        ResProcessorClass = {"service": ServiceProcessor,
                             "unix-account": UnixAccountProcessor,
                             "database-user": DatabaseUserProcessor,
                             "database": DatabaseProcessor,
                             "website": WebSiteProcessor if sys.platform != "freebsd9" else WebSiteProcessorFreeBsd,
                             "ssl-certificate": SslCertificateProcessor,
                             "mailbox": MailboxProcessor,
                             "resource-archive": ResourceArchiveProcessor,
                             "redirect": RedirectProcessor}.get(res_type)
        if not ResProcessorClass:
            raise BuilderTypeError("Unknown resource type: {}".format(res_type))
        return ResProcessorClass
