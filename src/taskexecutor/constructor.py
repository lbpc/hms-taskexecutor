import collections
import sys

from taskexecutor.config import CONFIG
import taskexecutor.conffile
import taskexecutor.opservice
import taskexecutor.resprocessor
import taskexecutor.sysservice
import taskexecutor.listener
import taskexecutor.reporter
import taskexecutor.httpsclient


class Constructor:
    def get_conffile(self, config_type, abs_path, owner_uid=None, mode=None):
        ConfigFile = taskexecutor.conffile.Builder(config_type)
        return ConfigFile(abs_path, owner_uid, mode)

    def get_opservice(self, service_type, template_obj_list=None, socket_obj_list=None):
        OpService = taskexecutor.opservice.Builder(service_type)
        service = OpService("-".join(service_type.lower().split("_")[1:]))
        if isinstance(service, taskexecutor.opservice.NetworkingService):
            for socket in socket_obj_list:
                service.set_socket(socket.name.split("@")[0].split("-")[-1], socket)
        if isinstance(service, taskexecutor.opservice.ConfigurableService) and service.config_base_path:
            for template in template_obj_list:
                service.set_config_from_template_obj(template)
        return service

    def get_sysservice(self, resource_type):
        resource_to_service_mapping = {"unix-account": "USER_MANAGER",
                                       "mailbox": "MAILDIR_MANAGER"}
        service_type = "{0}_{1}".format(sys.platform.upper(), resource_to_service_mapping[resource_type])
        SysService = taskexecutor.sysservice.Builder(service_type)
        return SysService()

    def get_resprocessor(self, resource_type, resource, params=None):
        ResProcessor = taskexecutor.resprocessor.Builder(resource_type)
        op_service = None
        if hasattr(resource, "serviceId"):
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                api_service = api.Service(resource.serviceId).get()
                op_service = self.get_opservice(api.Service(resource.serviceId).get().serviceType.name,
                                             template_obj_list=api_service.serviceTemplate.configTemplates,
                                             socket_obj_list=api_service.serviceSockets)
        elif hasattr(resource, "serverId"):
            op_service = self.get_sysservice(resource_type)
        processor = ResProcessor(resource, op_service, params=params or {})
        if isinstance(processor, taskexecutor.resprocessor.WebSiteProcessor):
            ExtraServices = collections.namedtuple("Service", "http_proxy")
            for local_service in CONFIG.localserver.services:
                if local_service.serviceType.name == "STAFF_NGINX":
                    nginx = self.get_opservice(local_service.serviceType.name,
                                               template_obj_list=local_service.serviceTemplate.configTemplates,
                                               socket_obj_list=local_service.serviceSockets)
                    processor.extra_services = ExtraServices(http_proxy=nginx)
                    return processor
            raise AttributeError("Local server has no HTTP proxy service")
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

    def get_listener(self, listener_type):
        Listener = taskexecutor.listener.Builder(listener_type)
        return Listener()

    def get_reporter(self, reporter_type):
        Reporter = taskexecutor.reporter.Builder(reporter_type)
        return Reporter()
