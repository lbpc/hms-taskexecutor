import collections
import sys
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.conffile
import taskexecutor.executor
import taskexecutor.opservice
import taskexecutor.resdatafetcher
import taskexecutor.resdataprocessor
import taskexecutor.rescollector
import taskexecutor.resprocessor
import taskexecutor.sysservice
import taskexecutor.listener
import taskexecutor.reporter
import taskexecutor.backup
import taskexecutor.httpsclient
import taskexecutor.utils


class OpServiceNotFound(Exception):
    pass


SERVICE_ID_TO_OPSERVICE_MAPPING = dict()


def get_conffile(config_type, abs_path, owner_uid=None, mode=None):
    ConfigFile = taskexecutor.conffile.Builder(config_type)
    return ConfigFile(abs_path, owner_uid, mode)


def get_http_proxy_service():
    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
        return next((get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
              if s.template.__class__.__name__ == "HttpServer"), None)


def get_application_servers():
    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
        return (get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
              if s.template.__class__.__name__ == "ApplicationServer")


def get_mta_service():
    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
        return next((get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
              if s.template.__class__.__name__ == "Postfix"), None)


def get_cron_service():
    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
        return next((get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
              if s.template.__class__.__name__ == "CronD"), None)


def get_opservice(service):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    opservice = SERVICE_ID_TO_OPSERVICE_MAPPING.get(service.id)
    if not opservice:
        LOGGER.debug("service template name is {}".format(service.template.name))
        OpService = taskexecutor.opservice.Builder(service.template.__class__.__name__,
                                                   service.template.supervisionType,
                                                   service.template.availableToAccounts,
                                                   getattr(service.template, "type", None))

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
    if hasattr(resource, "serverId") and resource_type != "service":
        resource_to_service_type_mapping = {"unix-account": "USER_MANAGER",
                                            "mailbox": "MAILDIR_MANAGER"}
        service_type = "{0}_{1}".format(sys.platform.upper(), resource_to_service_type_mapping[resource_type])
        SysService = taskexecutor.sysservice.Builder(service_type)
        service = SysService()
    elif hasattr(resource, "serviceId"):
        service = SERVICE_ID_TO_OPSERVICE_MAPPING.get(resource.serviceId)
        if not service:
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                service = get_opservice(api.Service(resource.serviceId).get())
    elif hasattr(resource, "template"):
        service = get_opservice(resource)
    elif resource_type == "ssl-certificate":
        service = get_http_proxy_service()
    else:
        raise OpServiceNotFound("Cannot find operational service for given "
                                "'{0}' resource: {1}".format(resource_type, resource))
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
    ResProcessor = taskexecutor.resprocessor.Builder(resource_type)
    op_service = get_opservice_by_resource(resource, resource_type)
    processor = ResProcessor(resource, op_service, params=params or {})
    collector = get_rescollector(resource_type, resource)
    collector.ignore_property("quotaUsed")
    processor.op_resource = collector.get()
    processor.extra_services = get_extra_services(processor)
    return processor


def get_rescollector(resource_type, resource):
    ResCollector = taskexecutor.rescollector.Builder(resource_type)
    op_service = get_opservice_by_resource(resource, resource_type)
    collector = ResCollector(resource, op_service)
    return collector


def get_datafetcher(src_uri, dst_uri, params=None):
    DataFetcher = taskexecutor.resdatafetcher.Builder(urllib.parse.urlparse(src_uri).scheme)
    data_fetcher = DataFetcher(src_uri, dst_uri, params=params or {})
    return data_fetcher


def get_datapostprocessor(postproc_type, args):
    DataPostprocessor = taskexecutor.resdataprocessor.Builder(postproc_type)
    postprocessor = DataPostprocessor(**args)
    return postprocessor


def get_listener(listener_type):
    Listener = taskexecutor.listener.Builder(listener_type)
    out_queue = taskexecutor.executor.Executor.get_new_task_queue()
    return Listener(out_queue)


def get_reporter(reporter_type):
    Reporter = taskexecutor.reporter.Builder(reporter_type)
    return Reporter()

def get_backuper(res_type, resource):
    Backuper = taskexecutor.backup.Builder(res_type)
    return Backuper(resource)
