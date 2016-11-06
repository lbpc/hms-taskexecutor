import os
import socket
import time
from urllib.parse import urlparse
from taskexecutor.httpclient import ConfigServerClient, EurekaClient, ApiClient
from taskexecutor.logger import LOGGER


class __Config:
    def __init__(self):
        LOGGER.info("Initializing config")
        self.hostname = socket.gethostname().split('.')[0]
        self._configserver = None
        self._apigw = None
        self._rc_user = None
        self._read_os_env()
        self._fetch_remote_properties()
        self._obtain_local_server_props()
        self._declare_enabled_resources()
        LOGGER.info("Effective configuration:{}".format(self))

    @property
    def configserver(self):
        if not self._configserver:
            LOGGER.debug("Performing first lookup to Eureka for configserver")
            with EurekaClient(self.eureka_socket_list) as eureka:
                self._configserver = eureka.get_random_instance("configserver")
        if self._configserver.timestamp + 30 < time.time():
            LOGGER.debug("Configserver timestamp is stale, "
                         "perfoming new lookup")
            with EurekaClient(self.eureka_socket_list) as eureka:
                _instance = eureka.get_random_instance("configserver")
                if _instance:
                    self._configserver = _instance
                else:
                    LOGGER.warning("Eureka lookup returned nothing, "
                                   "preserving last value")
        return {"address": self._configserver.address,
                "port": self._configserver.port}

    @property
    def apigw(self):
        if not self._apigw:
            LOGGER.debug("Performing first lookup to Eureka for apigw")
            with EurekaClient(self.eureka_socket_list) as eureka:
                self._apigw = eureka.get_random_instance("apigw")
        if self._apigw.timestamp + 30 < time.time():
            LOGGER.debug("Apigw timestamp is stale, perfoming new lookup")
            with EurekaClient(self.eureka_socket_list) as eureka:
                _instance = eureka.get_random_instance("apigw")
                if _instance:
                    self._apigw = _instance
                else:
                    LOGGER.warning("Eureka lookup returned nothing, "
                                   "preserving last value")
        return {"address": self._apigw.address,
                "port": self._apigw.port,
                "user": self.api.user,
                "password": self.api.password}

    @property
    def rc_user(self):
        if not self._rc_user:
            LOGGER.debug("Performing first lookup to Eureka for rc-user")
            with EurekaClient(self.eureka_socket_list) as eureka:
                self._rc_user = eureka.get_random_instance("rc-user")
        if self._rc_user.timestamp + 30 < time.time():
            LOGGER.debug("Rc-user timestamp is stale, perfoming new lookup")
            with EurekaClient(self.eureka_socket_list) as eureka:
                _instance = eureka.get_random_instance("rc-user")
                if _instance:
                    self._rc_user = _instance
                else:
                    LOGGER.warning("Eureka lookup returned nothing, "
                                   "preserving last value")
        return {"address": self._rc_user.address,
                "port": self._rc_user.port}

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
        self.eureka_socket_list = [
            urlparse(url).netloc.split(":") for url in
            os.environ["EUREKA_CLIENT_SERVICE-URL_defaultZone"].split(",")
        ]
        self._amqp_host = os.environ["SPRING_RABBITMQ_HOST"]

    def _fetch_remote_properties(self):
        LOGGER.info("Fetching properties from config server")
        with ConfigServerClient(**self.configserver) as cnf:
            cnf.extra_attrs = [
                "amqp.host={}".format(self._amqp_host),
                "amqp.consumer_routing_key=te.{}".format(self.hostname),
            ]
            for attr, value in vars(
                    cnf.get_property_sources_list("te",
                                                  self.profile)[0]).items():
                if not attr.startswith("_"):
                    setattr(self, attr, value)

    def _obtain_local_server_props(self):
        with ApiClient(**self.apigw) as api:
            _result = api.server(query={"name": self.hostname}).get()[0]
            if isinstance(_result, list):
                raise Exception("There is more than one server "
                                "with name {0}: {1}".format(self.hostname,
                                                            _result))
            self.localserver = _result

    def _declare_enabled_resources(self):
        self.enabled_resources = \
            {"web": ["service", "unix-account", "dbaccount", "database",
             "website", "sslcertificate"],
             "pop": ["mailbox"],
             "mx": ["mailbox"],
             "mailchecker": ["mailbox"],
             "db": ["dbaccount", "database"]}[self.localserver.serverRole.name]
        LOGGER.info("Server role is '{0}', manageable resources: "
                    "{1}".format(self.localserver.serverRole.name,
                                 self.enabled_resources))

    @classmethod
    def __setattr__(self, name, value):
        if hasattr(self, name) and not name.startswith("_"):
            raise AttributeError("{} is a read-only attribute".format(name))
        setattr(self, name, value)

    @classmethod
    def __str__(self):
        _attr_list = list()
        for attr, value in vars(self).items():
            if not attr.startswith("_") and not callable(getattr(self, attr)):
                _attr_list.append("{0}={1}".format(attr, value))
        return "CONFIG({})".format(", ".join(_attr_list))

CONFIG = __Config()
