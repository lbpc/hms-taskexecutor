import collections

from taskexecutor.config import CONFIG
import taskexecutor.conffile
import taskexecutor.opservice
import taskexecutor.resprocessor
import taskexecutor.listener
import taskexecutor.reporter
import taskexecutor.httpsclient


class Constructor:
    def get_conffile(self, config_type, abs_path, owner_uid=None, mode=None):
        ConfigFile = taskexecutor.conffile.Builder(config_type)
        return ConfigFile(abs_path, owner_uid, mode)

    def get_opservice(self, service_type, template_obj_list=None, socket_obj_list=None):
        name = "-".join(service_type.lower().split("_")[1:])
        OpService = taskexecutor.opservice.Builder(name)
        service = OpService(name)
        if isinstance(service, taskexecutor.opservice.NetworkingService):
            for socket in socket_obj_list:
                service.set_socket(socket.name.split("@")[0].split("-")[-1], socket)
        if isinstance(service, taskexecutor.opservice.ConfigurableService) and service.config_base_path:
            for template in template_obj_list:
                service.set_config_from_template_obj(template)
        return service

    def get_resprocessor(self, resource_type, resource, params):
        ResProcessor = taskexecutor.resprocessor.Builder(resource_type)
        processor = ResProcessor(resource, params)
        if hasattr(resource, "serviceId"):
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                service = api.Service(resource.serviceId).get()
                processor.service = self.get_opservice(service.serviceType.name,
                                                       template_obj_list=service.serviceTemplate.configTemplates,
                                                       socket_obj_list=service.serviceSockets)
        if isinstance(processor, taskexecutor.resprocessor.WebSiteProcessor):
            ExtraServices = collections.namedtuple("Service", "http_proxy")
            for service in CONFIG.localserver.services:
                if service.serviceType.name == "STAFF_NGINX":
                    processor.extra_services = ExtraServices(http_proxy=taskexecutor.opservice.Builder(service))
                raise AttributeError("Local server has no HTTP proxy service")
        return processor

    def get_siding_resprocessors(self, processor):
        if isinstance(processor, taskexecutor.resprocessor.DatabaseUserProcessor):
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                return [self.get_resprocessor("database", resource, {})
                        for resource in api.Database(query={"databaseUserId": processor.resource.id})]
        else:
            return []

    def get_listener(self, listener_type):
        Listener = taskexecutor.listener.Builder(listener_type)
        return Listener()

    def get_reporter(self, reporter_type):
        Reporter = taskexecutor.resprocessor.Builder(reporter_type)
        return Reporter()
