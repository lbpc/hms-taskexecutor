import os
import socket

import taskexecutor.httpsclient
from taskexecutor.logger import LOGGER


class PropertyValidationError(Exception):
    pass


class __Config:
    def __init__(self):
        LOGGER.info("Initializing config")
        self.hostname = socket.gethostname().split('.')[0]
        self.profile = os.environ.get("SPRING_PROFILES_ACTIVE") or "dev"
        self.apigw = dict(host=os.environ.get("APIGW_HOST") or "api.majordomo.ru",
                          port=int(os.environ.get("APIGW_PORT") or 443),
                          user=os.environ.get("APIGW_USER") or "admin",
                          password=os.environ.get("APIGW_PASSWORD") or "admin")
        self._amqp = dict(host=os.environ.get("SPRING_RABBITMQ_HOST"),
                          user=os.environ.get("SPRING_RABBITMQ_USERNAME"),
                          password=os.environ.get("SPRING_RABBITMQ_PASSWORD"))
        self._fetch_remote_properties()
        self._obtain_local_server_props()
        self._declare_enabled_resources()
        LOGGER.info("Effective configuration:{}".format(self))

    def _fetch_remote_properties(self):
        LOGGER.info("Fetching properties from config server")
        with taskexecutor.httpsclient.ConfigServerClient(**self.apigw) as cfg_srv:
            extra_attrs = ["amqp.{0}={1}".format(k, v) for k, v in self._amqp.items() if v]
            extra_attrs.append("amqp.consumer_routing_key=te.{}".format(self.hostname))
            cfg_srv.extra_attrs = extra_attrs
            props = cfg_srv.te(self.profile).get().propertySources[0].source
            for attr, value in vars(props).items():
                if not attr.startswith("_"):
                    setattr(self, attr, value)

    def _obtain_local_server_props(self):
        with taskexecutor.httpsclient.ApiClient(**self.apigw) as api:
            result = api.Server(query={"name": self.hostname}).get()
            if len(result) > 1:
                raise PropertyValidationError("There is more than one server with name {0}: "
                                              "{1}".format(self.hostname, result))
            elif len(result) == 0:
                raise PropertyValidationError("No {} server found".format(self.hostname))
            self.localserver = result[0]

    def _declare_enabled_resources(self):
        if not hasattr(self, "role"):
            raise PropertyValidationError("No role descriptions found")
        enabled_resources = list()
        for server_role in self.localserver.serverRoles:
            server_role_attr = server_role.name.replace("-", "_")
            if hasattr(self.role, server_role_attr):
                config_role = getattr(self.role, server_role_attr)
                if isinstance(config_role.resources, list):
                    enabled_resources.extend(config_role.resources)
                else:
                    enabled_resources.append(config_role.resources)
        self.enabled_resources = set(enabled_resources)
        LOGGER.info("Server roles: {0}, "
                    "manageable resources: {1}".format(self.localserver.serverRoles, enabled_resources))

    @classmethod
    def __setattr__(self, name, value):
        if hasattr(self, name) and not name.startswith("_"):
            raise AttributeError("{} is a read-only attribute".format(name))
        setattr(self, name, value)

    @classmethod
    def __str__(self):
        attr_list = list()
        for attr, value in vars(self).items():
            if not attr.startswith("_") and not callable(getattr(self, attr)):
                attr_list.append("{0}={1}".format(attr, value))
        return "CONFIG({})".format(", ".join(attr_list))


CONFIG = __Config()
