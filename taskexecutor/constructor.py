import collections
import urllib.parse

import taskexecutor.backup
import taskexecutor.builtinservice
import taskexecutor.conffile
import taskexecutor.executor
import taskexecutor.httpsclient
import taskexecutor.listener
import taskexecutor.opservice
import taskexecutor.reporter
import taskexecutor.rescollector
import taskexecutor.resdatafetcher
import taskexecutor.resdataprocessor
import taskexecutor.resprocessor
import taskexecutor.utils
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER


class ClassSelectionError(Exception):
    pass


class OpServiceNotFound(Exception):
    pass


SERVICE_ID_TO_OPSERVICE_MAPPING = dict()


def get_conffile(config_type, abs_path, owner_uid=None, mode=None):
    ConfigFile = {"templated": taskexecutor.conffile.TemplatedConfigFile,
                  "lines": taskexecutor.conffile.LineBasedConfigFile,
                  "basic": taskexecutor.conffile.ConfigFile}.get(config_type)
    if not ConfigFile: raise ClassSelectionError("Unknown config type: {}".format(config_type))
    return ConfigFile(abs_path, owner_uid, mode)


def get_services_of_type(type_name):
    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
        return (get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
                if s.template.__class__.__name__ == type_name)


def get_http_proxy_service():
    return next(get_services_of_type('HttpServer'), None)


def get_application_servers():
    return get_services_of_type('ApplicationServer')


def get_database_server():
    return next(get_services_of_type('DatabaseServer'), None)


def get_mta_service():
    return next(get_services_of_type('Postfix'), None)


def get_cron_service():
    return next(get_services_of_type('CronD'), None)


def get_ssh_service():
    return next(get_services_of_type('SshD'), None)


def get_ftp_service():
    return next(get_services_of_type('FtpD'), None)


def get_opservice(service):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    opservice = SERVICE_ID_TO_OPSERVICE_MAPPING.get(service.id)
    if not opservice:
        LOGGER.debug("service template name is {}".format(service.template.name))
        t_name = service.template.__class__.__name__
        superv = service.template.supervisionType
        private = service.template.availableToAccounts
        t_mod = getattr(service.template, "type", None)
        OpService = {
            superv == "docker": taskexecutor.opservice.SomethingInDocker,
            t_name == "CronD": taskexecutor.opservice.Cron,
            t_name == "Postfix": taskexecutor.opservice.Postfix,
            t_name == "HttpServer": taskexecutor.opservice.HttpServer,
            t_name == "ApplicationServer": taskexecutor.opservice.Apache,
            t_name == "ApplicationServer" and superv == "docker": taskexecutor.opservice.SharedAppServer,
            t_name == "ApplicationServer" and superv == "docker" and private: taskexecutor.opservice.PersonalAppServer,
            t_name == "DatabaseServer" and t_mod == "MYSQL": taskexecutor.opservice.MySQL,
            t_name == "DatabaseServer" and t_mod == "POSTGRESQL": taskexecutor.opservice.PostgreSQL
        }.get(True)
        if not OpService: raise ClassSelectionError("Unknown OpService type: {} "
                                                    "and catch-all 'SomethingInDocker' did not match "
                                                    "due to '{}' supervision".format(t_name, superv))
        service_name = service.name.lower().split("@")[0]
        if hasattr(service, "accountId") and service.accountId:
            service_name += "-" + service.id
        opservice = OpService(service_name, service)
        if isinstance(opservice, taskexecutor.opservice.DockerService):
            LOGGER.debug("{} is dockerized service".format(service_name))
        if isinstance(opservice, taskexecutor.opservice.PersonalAppServer):
            LOGGER.debug("{} is personal application server".format(service_name))
        if isinstance(opservice, taskexecutor.opservice.NetworkingService):
            LOGGER.debug("{} is networking service".format(service_name))
            for socket in service.sockets:
                opservice.set_socket(socket.protocol or "default", socket)
        if isinstance(opservice, taskexecutor.opservice.ConfigurableService):
            LOGGER.debug("{} is configurable service".format(service_name))
            for each in service.template.configTemplates:
                opservice.set_config(each.pathTemplate or each.name, each.fileLink, each.context)
        SERVICE_ID_TO_OPSERVICE_MAPPING[service.id] = opservice
    return opservice


def get_opservice_by_resource(resource, resource_type):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    if hasattr(resource, 'serverId') and resource_type != 'service':
        BuiltinService = {'unix-account': taskexecutor.builtinservice.LinuxUserManager,
                          'mailbox': taskexecutor.builtinservice.MaildirManager}.get(resource_type)
        if not BuiltinService: raise ClassSelectionError(f"Resource has 'serverId' property, "
                                                         f"but no built-in service exist for {resource_type}")
        service = BuiltinService()
    elif hasattr(resource, 'serviceId'):
        service = SERVICE_ID_TO_OPSERVICE_MAPPING.get(resource.serviceId)
        if not service:
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                service = get_opservice(api.Service(resource.serviceId).get())
    elif hasattr(resource, 'template'):
        service = get_opservice(resource)
    elif resource_type == 'ssl-certificate':
        service = get_http_proxy_service()
    else:
        raise OpServiceNotFound(f"Cannot find operational service for given '{resource_type}' resource: {resource}")
    return service


def get_all_opservices_by_res_type(resource_type):
    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
        next((get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
              if s.template.resourceType == resource_type.upper()), None)


def get_extra_services(res_processor):
    if isinstance(res_processor, taskexecutor.resprocessor.WebSiteProcessor):
        ServiceContainer = collections.namedtuple("ServiceContainer", "http_proxy old_app_server")
        return ServiceContainer(http_proxy=get_http_proxy_service(),
                                old_app_server=get_opservice_by_resource(res_processor.op_resource, "website")
                                if res_processor.op_resource else None)
    if isinstance(res_processor, taskexecutor.resprocessor.UnixAccountProcessor):
        ServiceContainer = collections.namedtuple("ServiceContainer", "mta cron")
        return ServiceContainer(mta=get_mta_service(), cron=get_cron_service())
    return list()


def get_resprocessor(resource_type, resource, params=None):
    ResProcessor = {"service": taskexecutor.resprocessor.ServiceProcessor,
                    "unix-account": taskexecutor.resprocessor.UnixAccountProcessor,
                    "database-user": taskexecutor.resprocessor.DatabaseUserProcessor,
                    "database": taskexecutor.resprocessor.DatabaseProcessor,
                    "website": taskexecutor.resprocessor.WebSiteProcessor,
                    "ssl-certificate": taskexecutor.resprocessor.SslCertificateProcessor,
                    "mailbox": taskexecutor.resprocessor.MailboxProcessor,
                    "resource-archive": taskexecutor.resprocessor.ResourceArchiveProcessor,
                    "redirect": taskexecutor.resprocessor.RedirectProcessor}.get(resource_type)
    if not ResProcessor: raise ClassSelectionError("Unknown resource type: {}".format(resource_type))
    op_service = get_opservice_by_resource(resource, resource_type)
    processor = ResProcessor(resource, op_service, params=params or {})
    collector = get_rescollector(resource_type, resource)
    collector.ignore_property("quotaUsed")
    processor.op_resource = collector.get()
    processor.extra_services = get_extra_services(processor)
    return processor


def get_rescollector(resource_type, resource):
    ResCollector = {"unix-account": taskexecutor.rescollector.UnixAccountCollector,
                    "database-user": taskexecutor.rescollector.DatabaseUserCollector,
                    "database": taskexecutor.rescollector.DatabaseCollector,
                    "mailbox": taskexecutor.rescollector.MailboxCollector,
                    "website": taskexecutor.rescollector.WebsiteCollector,
                    "ssl-certificate": taskexecutor.rescollector.SslCertificateCollector,
                    "service": taskexecutor.rescollector.ServiceCollector,
                    "resource-archive": taskexecutor.rescollector.ResourceArchiveCollector,
                    "redirect": taskexecutor.rescollector.RedirectCollector}.get(resource_type)
    if not ResCollector: raise ClassSelectionError("Unknown resource type: {}".format(resource_type))
    op_service = get_opservice_by_resource(resource, resource_type)
    return ResCollector(resource, op_service)


def get_datafetcher(src_uri, dst_uri, params=None):
    scheme = urllib.parse.urlparse(src_uri).scheme
    DataFetcher = {"file": taskexecutor.resdatafetcher.FileDataFetcher,
                   "rsync": taskexecutor.resdatafetcher.RsyncDataFetcher,
                   "mysql": taskexecutor.resdatafetcher.MysqlDataFetcher,
                   "http": taskexecutor.resdatafetcher.HttpDataFetcher,
                   "git+ssh": taskexecutor.resdatafetcher.GitDataFetcher,
                   "git+http": taskexecutor.resdatafetcher.GitDataFetcher,
                   "git+https": taskexecutor.resdatafetcher.GitDataFetcher}.get(urllib.parse.urlparse(src_uri).scheme)
    if not DataFetcher: raise ClassSelectionError("Unknown data source URI scheme: {}".format(scheme))
    return DataFetcher(src_uri, dst_uri, params=params or {})


def get_datapostprocessor(postproc_type, args):
    DataPostprocessor = {"docker": taskexecutor.resdataprocessor.DockerDataPostprocessor,
                         "string-replace": taskexecutor.resdataprocessor.StringReplaceDataProcessor,
                         "eraser": taskexecutor.resdataprocessor.DataEraser}.get(postproc_type)
    if not DataPostprocessor: raise ClassSelectionError("Unknown data postprocessor type: {}".format(postproc_type))
    return DataPostprocessor(**args)


def get_listener(listener_type):
    Listener = {"amqp": taskexecutor.listener.AMQPListener,
                "time": taskexecutor.listener.TimeListener}.get(listener_type)
    if not Listener: raise ClassSelectionError("Unknown Listener type: {}".format(listener_type))
    out_queue = taskexecutor.executor.Executor.get_new_task_queue()
    return Listener(out_queue)


def get_reporter(reporter_type):
    Reporter = {"amqp": taskexecutor.reporter.AMQPReporter,
                "https": taskexecutor.reporter.HttpsReporter,
                "alerta": taskexecutor.reporter.AlertaReporter,
                "null": taskexecutor.reporter.NullReporter}.get(reporter_type)
    if not Reporter: raise ClassSelectionError("Unknown Reporter type: {}".format(reporter_type))
    return Reporter()


def get_backuper(res_type, resource):
    Backuper = {"unix-account": taskexecutor.backup.ResticBackup,
                "website": taskexecutor.backup.ResticBackup}.get(res_type)
    if not Backuper: raise ClassSelectionError("Unknown resource type: {}".format(res_type))
    return Backuper(resource)
