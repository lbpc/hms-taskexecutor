import collections
import sys

from taskexecutor.config import CONFIG
import taskexecutor.conffile
import taskexecutor.baseservice
import taskexecutor.facts
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
    __CommandExecutorsPool = None
    __QueryExecutorsPool = None

    def get_conffile(self, config_type, abs_path, owner_uid=None, mode=None):
        ConfigFile = taskexecutor.conffile.Builder(config_type)
        return ConfigFile(abs_path, owner_uid, mode)

    def get_opservice(self, service_api_obj):
        OpService = taskexecutor.opservice.Builder(service_api_obj.serviceType.name)
        service_name = "-".join(service_api_obj.serviceType.name.lower().split("_")[1:])
        service = OpService(service_name)
        if isinstance(service, taskexecutor.baseservice.NetworkingService):
            for socket in service_api_obj.serviceSockets:
                service.set_socket(socket.name.split("@")[0].split("-")[-1], socket)
        if isinstance(service, taskexecutor.baseservice.ConfigurableService) and service.config_base_path:
            for template in service_api_obj.serviceTemplate.configTemplates:
                service.set_config_from_template_obj(template)
        return service

    def get_opservice_by_resource(self, resource, resource_type):
        if hasattr(resource, "serverId"):
            resource_to_service_type_mapping = {"unix-account": "USER_MANAGER",
                                                "mailbox": "MAILDIR_MANAGER"}
            service_type = "{0}_{1}".format(sys.platform.upper(), resource_to_service_type_mapping[resource_type])
            SysService = taskexecutor.sysservice.Builder(service_type)
            service = SysService()
        elif hasattr(resource, "serviceId"):
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                service = self.get_opservice(api.Service(resource.serviceId).get())
        else:
            raise OpServiceNotFound("Cannot find operational service for given "
                                    "'{0}' resource: {1}".format(resource_type, resource))
        return service

    def get_extra_services(self, res_processor):
        extra_services = {}
        if isinstance(res_processor, taskexecutor.resprocessor.WebSiteProcessor):
            for local_service in CONFIG.localserver.services:
                if local_service.serviceType.name == "STAFF_NGINX":
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
        processor.extra_services = self.get_extra_services(processor)
        return processor

    def get_siding_resprocessors(self, processor):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            if isinstance(processor, taskexecutor.resprocessor.DatabaseUserProcessor):
                return [self.get_resprocessor("database", database)
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
        if not Constructor.__CommandExecutorsPool:
            Constructor.__CommandExecutorsPool = \
                taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.command)
        return Constructor.__CommandExecutorsPool

    def get_query_executors_pool(self):
        if not Constructor.__QueryExecutorsPool:
            Constructor.__QueryExecutorsPool = \
                taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.query)
        return Constructor.__QueryExecutorsPool
