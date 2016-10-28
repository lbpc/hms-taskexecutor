import os
import socket
import re
import time
from urllib.parse import urlparse
from taskexecutor.httpclient import ConfigServerClient, EurekaClient, ApiClient
from taskexecutor.logger import LOGGER


class __Config:
    def __init__(self):
        LOGGER.info("Initializing config")
        self.discovered_services = ["configserver", "apigw"]
        self.eureka_socket = dict()
        self.hostname = socket.gethostname().split('.')[0]
        self._read_os_env()
        LOGGER.info("Registering discoverable services: "
                    "{}".format(self.discovered_services))
        for service, property in self._register_discovered_services():
            LOGGER.info("'{0}' service is now accessible as '{1}' property of "
                        "config class".format(service, property))
        self._fetch_remote_properties()
        self._obtain_local_server_props()
        self._declare_enabled_resources()
        LOGGER.info("Effective configuration:{}".format(self))

    def _read_os_env(self):
        if "HMS_ENV" in os.environ.keys():
            self.profile = os.environ["HMS_ENV"]
            LOGGER.info("'{}' profile set according to "
                        "HMS_ENV environment variable".format(self.profile))
        else:
            LOGGER.warning("There is no HMS_ENV environment variable set, "
                           "falling back to 'dev' profile")
            self.profile = "dev"
        self.eureka_socket["address"], self.eureka_socket["port"] = urlparse(
                os.environ["EUREKA_CLIENT_SERVICE-URL_defaultZone"]
        ).netloc.split(":")

    def _fetch_remote_properties(self):
        LOGGER.info("Fetching properties from config server")
        with ConfigServerClient(**self.configserver.serviceSocket) as cnf:
            cnf.extra_attrs = [
                "amqp.consumer_routing_key=te.{}".format(self.hostname),
            ]
            for attr, value in vars(
                    cnf.get_property_sources_list("te",
                                                  self.profile)[0]).items():
                if not attr.startswith("_"):
                    setattr(self, attr, value)

    def _obtain_local_server_props(self):
        self.localserver = type('', (), {})
        self.localserver.serverRole = type('', (), {})
        self.localserver.serverRole.name = "web"
        return
        with ApiClient(**self.apigw.serviceSocket) as api:
            self.localserver = api.server(query={"name": self.hostname}).get()

    def _declare_enabled_resources(self):
        self.enabled_resources = \
            {"web": ["unixaccount", "dbaccount", "database",
             "website", "sslcertificate"],
             "pop": ["mailbox"],
             "mx": ["mailbox"],
             "mailchecker": ["mailbox"],
             "db": ["dbaccount", "database"]}[self.localserver.serverRole.name]
        LOGGER.info("Server role is '{0}', manageable resources: "
                    "{1}".format(self.localserver.serverRole.name,
                                 self.enabled_resources))

    @classmethod
    def _register_discovered_services(cls):
        _registered = list()
        for service in cls.discovered_services:
            def closure(service, prop):
                _attr = "_{}".format(prop)

                def discovered_service_getter(self):
                    if not hasattr(self, _attr):
                        LOGGER.info(
                            "Performing first lookup to Eureka for "
                            "'{}' service".format(service)
                        )
                        with EurekaClient(**self.eureka_socket) as eureka:
                            setattr(self, _attr,
                                    eureka.get_random_instance(service))
                        return getattr(self, _attr)
                    if getattr(self, _attr).timestamp + 30 < time.time():
                        LOGGER.info(
                            "'{}' timestamp is stale, "
                            "perfoming new lookup".format(service)
                        )
                        with EurekaClient(**self.eureka_socket) as eureka:
                            _service_instance = eureka.get_random_instance(
                                service
                            )
                        if _service_instance:
                            setattr(self, _attr, _service_instance)
                        else:
                            LOGGER.warning(
                                "Eureka lookup returned nothing, "
                                "preserving last value"
                            )
                    return getattr(self, _attr)

                def discovered_service_setter(self):
                    raise AttributeError(
                        "{} is a read-only property".format(service))

                setattr(cls, prop, property(discovered_service_getter,
                                            discovered_service_setter))

            _normalized_property = re.sub('\W|^(?=\d)', '_', service)
            closure(service, _normalized_property)
            _registered.append((service, _normalized_property))
        return _registered

    @classmethod
    def __setattr__(self, name, value):
        if hasattr(self, name) and not name.startswith("_") and \
                        name not in self.discovered_services:
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
