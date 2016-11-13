import os
import socket

from taskexecutor.httpsclient import ApiClient, ConfigServerClient, GitLabClient
from taskexecutor.logger import LOGGER


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
        self._gitlab_connect()
        self._declare_enabled_resources()
        LOGGER.info("Effective configuration:{}".format(self))

    def _read_os_env(self):
        if "SPRING_PROFILES_ACTIVE" in os.environ.keys():
            self.profile = os.environ["SPRING_PROFILES_ACTIVE"]
            LOGGER.info("'{}' profile set according to SPRING_PROFILES_ACTIVE "
                        "environment variable".format(self.profile))
        else:
            LOGGER.warning("There is no SPRING_PROFILES_ACTIVE "
                           "environment variable set, "
                           "falling back to 'dev' profile")
            self.profile = "dev"
        self._amqp_host = os.environ["SPRING_RABBITMQ_HOST"]

    def _fetch_remote_properties(self):
        LOGGER.info("Fetching properties from config server")
        with ConfigServerClient(**self.apigw) as cfg_srv:
            cfg_srv.extra_attrs = [
                "amqp.host={}".format(self._amqp_host),
                "amqp.consumer_routing_key=te.{}".format(self.hostname),
            ]
            props = cfg_srv.te(self.profile).get().propertySources[0].source
            for attr, value in vars(props).items():
                if not attr.startswith("_"):
                    setattr(self, attr, value)

    def _gitlab_connect(self):
        LOGGER.info("Setting gitlab connection")
        with GitLabClient(self.gitlab.host, self.gitlab.port, self.gitlab.private_token) as gitlab:
            gitlab.authorize()
            self.gitlab = gitlab

    def _obtain_local_server_props(self):
        with ApiClient(**self.apigw) as api:
            result = api.server(query={"name": self.hostname}).get()
            if len(result) > 1:
                raise Exception("There is more than one server "
                                "with name {0}: {1}".format(self.hostname,
                                                            result))
            elif len(result) == 0:
                raise Exception("No {} server found".format(self.hostname))
            self.localserver = result[0]

    def _declare_enabled_resources(self):
        enabled_resources = []
        resource_to_server_role_mapping = \
            {
                "shared-hosting": ["service",
                                   "unix-account",
                                   "database-user",
                                   "database",
                                   "website",
                                   "sslcertificate"],
                "mail-storage": ["mailbox"],
                "mail-exchanger": ["mailbox"],
                "mail-checker": ["mailbox"],
                "database-server": ["database-user",
                                    "database"]
            }
        for serverRole in self.localserver.serverRoles:
            enabled_resources = enabled_resources \
                + resource_to_server_role_mapping[serverRole.name]
        self.enabled_resources = list(set(enabled_resources))

        LOGGER.info("Server role is '{server_role}', manageable resources: "
                    "{enabled_resource}".format(server_role=self.localserver.serverRoles,
                                 enabled_resource=self.enabled_resources))

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
