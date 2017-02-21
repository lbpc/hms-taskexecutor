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
        self.apigw = {"host": "api.majordomo.ru",
                      "port": 443,
                      "user": "admin",
                      "password": "admin"}
        self._read_os_env()
        self._fetch_remote_properties()
        self._obtain_local_server_props()
        self._declare_enabled_resources()
        LOGGER.info("Effective configuration:{}".format(self))

    def _read_os_env(self):
        if "SPRING_PROFILES_ACTIVE" in os.environ.keys():
            self.profile = os.environ["SPRING_PROFILES_ACTIVE"]
            LOGGER.info("'{}' profile set according to SPRING_PROFILES_ACTIVE env variable".format(self.profile))
        else:
            LOGGER.warning("There is no SPRING_PROFILES_ACTIVE env variable set, falling back to 'dev' profile")
            self.profile = "dev"
        self._amqp_host = os.environ["SPRING_RABBITMQ_HOST"]
        if "APIGW_HOST" in os.environ.keys():
            self.apigw["host"] = os.environ["APIGW_HOST"]
        if "APIGW_PORT" in os.environ.keys():
            self.apigw["port"] = os.environ["APIGW_PORT"]

    def _fetch_remote_properties(self):
        LOGGER.info("Fetching properties from config server")
        with taskexecutor.httpsclient.ConfigServerClient(**self.apigw) as cfg_srv:
            cfg_srv.extra_attrs = ["amqp.host={}".format(self._amqp_host),
                                   "amqp.consumer_routing_key=te.{}".format(self.hostname)]
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
        enabled_resources = list()
        resource_to_server_role_mapping = {"shared-hosting": ["service",
                                                              "unix-account",
                                                              "database-user",
                                                              "database",
                                                              "website",
                                                              "sslcertificate"],
                                           "mail-storage": ["mailbox"],
                                           "mail-exchanger": ["mailbox"],
                                           "mail-checker": ["mailbox"],
                                           "database-server": ["database-user",
                                                               "database"]}
        for server_role in self.localserver.serverRoles:
            enabled_resources.extend(resource_to_server_role_mapping[server_role.name])
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
