import collections
import sys

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.conffile
import taskexecutor.baseservice
import taskexecutor.opservice
import taskexecutor.rescollector
import taskexecutor.resprocessor
import taskexecutor.sysservice
import taskexecutor.listener
import taskexecutor.reporter
import taskexecutor.httpsclient
import taskexecutor.utils


class OpServiceNotFound(Exception):
    pass


COMMAND_EXECUTORS_POOL = None
QUERY_EXECUTORS_POOL = None
SERVICE_ID_TO_OPSERVICE_MAPPING = dict()


def get_conffile(config_type, abs_path, owner_uid=None, mode=None):
    ConfigFile = taskexecutor.conffile.Builder(config_type)
    return ConfigFile(abs_path, owner_uid, mode)


def get_http_proxy_service():
    return next((get_opservice(local_service) for local_service in CONFIG.localserver.services
                 if local_service.serviceTemplate.serviceType.name == "STAFF_NGINX"), None)


def get_opservice(service_api_obj):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    service = SERVICE_ID_TO_OPSERVICE_MAPPING.get(service_api_obj.id)
    if not service:
        OpService = taskexecutor.opservice.Builder(service_api_obj.serviceTemplate.serviceType.name)
        service_name = "-".join(service_api_obj.serviceTemplate.serviceType.name.lower().split("_")[1:])
        service = OpService(service_name)
        if isinstance(service, taskexecutor.baseservice.NetworkingService):
            LOGGER.debug("{} is networking service".format(service_name))
            for socket in service_api_obj.serviceSockets:
                service.set_socket(socket.name.split("@")[0].split("-")[-1], socket)
        if isinstance(service, taskexecutor.baseservice.ConfigurableService) and service.config_base_path:
            LOGGER.debug("{} is configurable service".format(service_name))
            for template in service_api_obj.serviceTemplate.configTemplates:
                service.set_config(template.name, template.fileLink)
        SERVICE_ID_TO_OPSERVICE_MAPPING[service_api_obj.id] = service
    return service


def get_opservice_by_resource(resource, resource_type):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    if hasattr(resource, "serverId"):
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
    elif hasattr(resource, "serviceTemplate"):
        service = get_opservice(resource)
    elif resource_type == "ssl-certificate":
        service = get_http_proxy_service()
    else:
        raise OpServiceNotFound("Cannot find operational service for given "
                                "'{0}' resource: {1}".format(resource_type, resource))
    return service


def get_all_opservices_by_res_type(resource_type):
    return [get_opservice(local_service) for local_service in CONFIG.localserver.services
            if local_service.serviceTemplate.serviceType.name.split("_")[0] == resource_type.upper()]


def get_extra_services(res_processor):
    extra_services = {}
    if isinstance(res_processor, taskexecutor.resprocessor.WebSiteProcessor):
        extra_services["http_proxy"] = get_http_proxy_service()
        if not extra_services["http_proxy"]:
            raise OpServiceNotFound("Local server has no HTTP proxy service")
    return collections.namedtuple("ServiceContainer", extra_services.keys())(**extra_services)


def get_resprocessor(resource_type, resource, params=None):
    ResProcessor = taskexecutor.resprocessor.Builder(resource_type)
    op_service = get_opservice_by_resource(resource, resource_type)
    processor = ResProcessor(resource, op_service, params=params or {})
    collector = get_rescollector(resource_type, resource)
    collector.ignore_property("quotaUsed")
    processor.op_resource = collector.get()
    processor.extra_services = get_extra_services(processor)
    return processor


def get_prequestive_resprocessors(processor, params=None):
    if isinstance(processor, taskexecutor.resprocessor.WebSiteProcessor):
        return [get_resprocessor("ssl-certificate", domain.sslCertificate)
                for domain in processor.resource.domains if domain.sslCertificate]
    else:
        return []


def get_siding_resprocessors(processor, params=None):
    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
        if isinstance(processor, taskexecutor.resprocessor.DatabaseUserProcessor):
            return [get_resprocessor("database", database, params=params)
                    for database in api.Database().filter(databaseUserId=processor.resource.id).get()]
        elif isinstance(processor, taskexecutor.resprocessor.SslCertificateProcessor):
            domain = api.Domain().find(sslCertificateId=processor.resource.id).get()
            website = api.Website().find(domainId=domain.id).get()
            return [get_resprocessor("website", website)]
        else:
            return []


def get_rescollector(resource_type, resource):
    ResCollector = taskexecutor.rescollector.Builder(resource_type)
    op_service = get_opservice_by_resource(resource, resource_type)
    collector = ResCollector(resource, op_service)
    return collector


def get_listener(listener_type):
    Listener = taskexecutor.listener.Builder(listener_type)
    return Listener()


def get_reporter(reporter_type):
    Reporter = taskexecutor.reporter.Builder(reporter_type)
    return Reporter()


def get_command_executors_pool():
    global COMMAND_EXECUTORS_POOL
    if not COMMAND_EXECUTORS_POOL:
        COMMAND_EXECUTORS_POOL = taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.command)
    return COMMAND_EXECUTORS_POOL


def get_query_executors_pool():
    global QUERY_EXECUTORS_POOL
    if not QUERY_EXECUTORS_POOL:
        QUERY_EXECUTORS_POOL = taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.query)
    return QUERY_EXECUTORS_POOL
