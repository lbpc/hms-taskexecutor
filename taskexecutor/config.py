import os
import socket
import time
import logging

import taskexecutor.httpsclient
from taskexecutor.logger import LOGGER


class PropertyValidationError(Exception):
    pass


REMOTE_CONFIG_TIMESTAMP = 0
REMOTE_CONFIG_STALE = False
REMOTE_CONFIG_TTL = os.environ.get("REMOTE_CONFIG_TTL") or 60


class __Config:
    @classmethod
    def __init__(cls):
        log_level = os.environ.get("LOG_LEVEL") or "INFO"
        log_level = getattr(logging, log_level.upper())
        LOGGER.setLevel(log_level)
        LOGGER.debug("Initializing config")
        cls.hostname = socket.gethostname().split('.')[0]
        cls.profile = os.environ.get("SPRING_PROFILES_ACTIVE") or "dev"
        cls.apigw = dict(host=os.environ.get("APIGW_HOST") or "api.intr",
                         port=int(os.environ.get("APIGW_PORT") or 443),
                         user=os.environ.get("APIGW_USER") or "service",
                         password=os.environ.get("APIGW_PASSWORD") or "Efu0ahs6")
        cls._amqp = dict(host=os.environ.get("SPRING_RABBITMQ_HOST"),
                         port=os.environ.get("SPRING_RABBITMQ_PORT") or 5672,
                         user=os.environ.get("SPRING_RABBITMQ_USERNAME"),
                         password=os.environ.get("SPRING_RABBITMQ_PASSWORD"))
        LOGGER.debug("Effective configuration:{}".format(cls))

    @classmethod
    def _fetch_remote_properties(cls):
        LOGGER.info("Fetching properties from config server")
        with taskexecutor.httpsclient.ConfigServerClient(**cls.apigw) as cfg_srv:
            extra_attrs = ["amqp.{0}={1}".format(k, v) for k, v in cls._amqp.items() if v]
            extra_attrs.append("amqp.consumer_routing_key=te.{}".format(cls.hostname))
            for k, v in os.environ.items():
                if k.startswith("TE_"):
                    k = k[3:].lower().replace("_", ".").replace("-", "_")
                    extra_attrs.append("{0}={1}".format(k, v))
            cfg_srv.extra_attrs = extra_attrs
            props = cfg_srv.te(cls.profile).get().propertySources[0].source
            for attr, value in props._asdict().items():
                if not attr.startswith("_"):
                    setattr(cls, attr, value)
        with taskexecutor.httpsclient.ApiClient(**cls.apigw) as api:
            result = api.Server(query={"name": cls.hostname}).get()
            if len(result) > 1:
                raise PropertyValidationError("There is more than one server with name {0}: "
                                              "{1}".format(cls.hostname, result))
            elif len(result) == 0:
                raise PropertyValidationError("No {} server found".format(cls.hostname))
            cls.localserver = result[0]
        global REMOTE_CONFIG_TIMESTAMP
        REMOTE_CONFIG_TIMESTAMP = time.time()
        global REMOTE_CONFIG_STALE
        REMOTE_CONFIG_STALE = False
        if not hasattr(cls, "role"):
            raise PropertyValidationError("No role descriptions found")
        enabled_resources = list()
        for server_role in cls.localserver.serverRoles:
            server_role_attr = server_role.name.replace("-", "_")
            if hasattr(cls.role, server_role_attr):
                config_role = getattr(cls.role, server_role_attr)
                if isinstance(config_role.resources, list):
                    enabled_resources.extend(config_role.resources)
                else:
                    enabled_resources.append(config_role.resources)
        cls.enabled_resources = set(enabled_resources)
        LOGGER.info("Server roles: {0}, manageable "
                    "resources: {1}".format([r.name for r in cls.localserver.serverRoles], enabled_resources))

    @classmethod
    def __getattr__(cls, item):
        LOGGER.warn(item)
        value = getattr(cls, item, None)
        if not value or REMOTE_CONFIG_STALE:
            cls._fetch_remote_properties()
            LOGGER.debug("Effective configuration:{}".format(cls))
            value = getattr(cls, item)
        return value

    @classmethod
    def __setattr__(cls, name, value):
        if hasattr(cls, name) and not name.startswith("_"):
            raise AttributeError("{} is a read-only attribute".format(name))
        setattr(cls, name, value)

    def __getattribute__(self, item):
        if not item.startswith("_") and time.time() - REMOTE_CONFIG_TIMESTAMP > REMOTE_CONFIG_TTL:
            global REMOTE_CONFIG_STALE
            REMOTE_CONFIG_STALE = True
            raise AttributeError
        return super().__getattribute__(item)

    def __str__(self):
        attr_list = list()
        for attr, value in vars(self).items():
            if not attr.startswith("_") and not callable(getattr(self, attr)):
                attr_list.append("{0}={1}".format(attr, value))
        return "CONFIG({})".format(", ".join(attr_list))


CONFIG = __Config()
