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


class Constructor:
    __command_executors_pool = None
    __query_executors_pool = None
    __service_id_opservice_mapping = dict()

    def get_conffile(self, config_type, abs_path, owner_uid=None, mode=None):
        ConfigFile = taskexecutor.conffile.Builder(config_type)
        return ConfigFile(abs_path, owner_uid, mode)

    def get_opservice(self, service_api_obj):
        service = Constructor.__service_id_opservice_mapping.get(service_api_obj.id)
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
            Constructor.__service_id_opservice_mapping[service_api_obj.id] = service
        return service

    def get_opservice_by_resource(self, resource, resource_type):
        if hasattr(resource, "serverId"):
            resource_to_service_type_mapping = {"unix-account": "USER_MANAGER",
                                                "mailbox": "MAILDIR_MANAGER"}
            service_type = "{0}_{1}".format(sys.platform.upper(), resource_to_service_type_mapping[resource_type])
            SysService = taskexecutor.sysservice.Builder(service_type)
            service = SysService()
        elif hasattr(resource, "serviceId"):
            service = Constructor.__service_id_opservice_mapping.get(resource.serviceId)
            if not service:
                with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                    service = self.get_opservice(api.Service(resource.serviceId).get())
        elif hasattr(resource, "serviceTemplate"):
            service = self.get_opservice(resource)
        else:
            raise OpServiceNotFound("Cannot find operational service for given "
                                    "'{0}' resource: {1}".format(resource_type, resource))
        return service

    def get_all_opservices_by_res_type(self, resource_type):
        return [self.get_opservice(local_service) for local_service in CONFIG.localserver.services
                if local_service.serviceTemplate.serviceType.name.split("_")[0] == resource_type.upper()]

    def get_extra_services(self, res_processor):
        extra_services = {}
        if isinstance(res_processor, taskexecutor.resprocessor.WebSiteProcessor):
            for local_service in CONFIG.localserver.services:
                if local_service.serviceTemplate.serviceType.name == "STAFF_NGINX":
                    nginx = self.get_opservice(local_service)
                    extra_services["http_proxy"] = nginx
                    break
            if "http_proxy" not in extra_services.keys():
                raise OpServiceNotFound("Local server has no HTTP proxy service")
        return collections.namedtuple("ServiceContainer", extra_services.keys())(**extra_services)

    def get_resprocessor(self, resource_type, resource, params=None):
        ResProcessor = taskexecutor.resprocessor.Builder(resource_type)
        op_service = self.get_opservice_by_resource(resource, resource_type)
        processor = ResProcessor(resource, op_service, params=params or {})
        collector = self.get_rescollector(resource_type, resource)
        collector.ignore_property("quotaUsed")
        processor.op_resource = collector.get()
        processor.extra_services = self.get_extra_services(processor)
        return processor

    def get_siding_resprocessors(self, processor, params=None):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            if isinstance(processor, taskexecutor.resprocessor.DatabaseUserProcessor):
                return [self.get_resprocessor("database", database, params=params)
                        for database in api.Database().filter(databaseUserId=processor.resource.id).get()]
            elif isinstance(processor, taskexecutor.resprocessor.SSLCertificateProcessor):
                domain = api.Domain().filter(sslCertificateId=processor.resource.id).get()
                website = api.Website().filter(domainId=domain.id).get()
                return self.get_resprocessor("website", website)
            else:
                return []

    def get_rescollector(self, resource_type, resource):
        ResCollector = taskexecutor.rescollector.Builder(resource_type)
        op_service = self.get_opservice_by_resource(resource, resource_type)
        collector = ResCollector(resource, op_service)
        return collector

    def get_listener(self, listener_type):
        Listener = taskexecutor.listener.Builder(listener_type)
        return Listener()

    def get_reporter(self, reporter_type):
        Reporter = taskexecutor.reporter.Builder(reporter_type)
        return Reporter()

    def get_command_executors_pool(self):
        if not Constructor.__command_executors_pool:
            Constructor.__command_executors_pool = \
                taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.command)
        return Constructor.__command_executors_pool

    def get_query_executors_pool(self):
        if not Constructor.__query_executors_pool:
            Constructor.__query_executors_pool = \
                taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.query)
        return Constructor.__query_executors_pool
