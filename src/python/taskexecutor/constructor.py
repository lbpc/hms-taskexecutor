import collections
import sys
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.conffile
import taskexecutor.baseservice
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
    return next((get_opservice(s) for s in CONFIG.localserver.services
                 if (hasattr(s, "serviceTemplate") and
                     s.serviceTemplate and
                     s.serviceTemplate.serviceType.name == "STAFF_NGINX") or
                 (hasattr(s, "template") and s.template and s.template.__class__.__name__ == "HttpServer")), None)


def get_opservice(service_api_obj):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    service = SERVICE_ID_TO_OPSERVICE_MAPPING.get(service_api_obj.id)
    oldschool_service = hasattr(service_api_obj, "serviceTemplate") and service_api_obj.serviceTemplate
    if not service:
        if oldschool_service:
            LOGGER.debug("service template name is {}".format(service_api_obj.serviceTemplate.name))
            in_docker = service_api_obj.serviceTemplate.name.endswith("@docker")
            type_name = service_api_obj.serviceTemplate.serviceType.name
        else:
            LOGGER.debug("service template name is {}".format(service_api_obj.template.name))
            in_docker = service_api_obj.template.supervisionType == "docker"
            type_name = "{}_{}".format(service_api_obj.template.resourceType,
                                     service_api_obj.template.name.replace("-", "_").replace(".", "").upper())
            if hasattr(service_api_obj, "instanceProps") and \
                    hasattr(service_api_obj.instanceProps, "security_level") and \
                    service_api_obj.instanceProps.security_level:
                type_name += "_{}".format(service_api_obj.instanceProps.security_level.upper())
        OpService = taskexecutor.opservice.Builder(type_name, docker=in_docker,
                                                   personal=hasattr(service_api_obj, "accountId"))
        service_name = service_api_obj.name.lower().replace("_", "-").split("@")[0]
        if hasattr(service_api_obj, "accountId") and service_api_obj.accountId:
            service_name += "-" + service_api_obj.id
        service = OpService(service_name)
        if isinstance(service, taskexecutor.opservice.DockerService):
            LOGGER.debug("{} is dockerized service".format(service_name))
        if isinstance(service, taskexecutor.baseservice.NetworkingService):
            LOGGER.debug("{} is networking service".format(service_name))
            if hasattr(service_api_obj, "serviceSockets"):
                for socket in service_api_obj.serviceSockets:
                    service.set_socket(socket.name.split("@")[0].split("-")[-1], socket)
            if hasattr(service_api_obj, "sockets"):
                for socket in service_api_obj.sockets:
                    service.set_socket(socket.protocol, socket)
        if isinstance(service, taskexecutor.baseservice.ConfigurableService) and service.config_base_path:
            LOGGER.debug("{} is configurable service".format(service_name))
            templates = service_api_obj.serviceTemplate.configTemplates if oldschool_service \
                else service_api_obj.template.configTemplates
            for template in templates:
                service.set_config(template.name, template.fileLink)
        SERVICE_ID_TO_OPSERVICE_MAPPING[service_api_obj.id] = service
    return service


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
    elif hasattr(resource, "serviceTemplate") or hasattr(resource, "template"):
        service = get_opservice(resource)
    elif resource_type == "ssl-certificate":
        service = get_http_proxy_service()
    else:
        raise OpServiceNotFound("Cannot find operational service for given "
                                "'{0}' resource: {1}".format(resource_type, resource))
    return service


def get_all_opservices_by_res_type(resource_type):
    return [get_opservice(s) for s in CONFIG.localserver.services
            if (hasattr(s, "serviceTemplate") and
                s.serviceTemplate and
                s.serviceTemplate.serviceType.name.split("_")[0] == resource_type.upper()) or
            (hasattr(s, "template") and s.template and s.template.resourceType == resource_type.upper())]


def get_extra_services(res_processor):
    if isinstance(res_processor, taskexecutor.resprocessor.WebSiteProcessor):
        ServiceContainer = collections.namedtuple("ServiceContainer", "http_proxy old_app_server")
        return ServiceContainer(http_proxy=get_http_proxy_service(),
                                old_app_server=get_opservice_by_resource(res_processor.op_resource, "website")
                                if res_processor.op_resource else None)
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
