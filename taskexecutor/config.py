import os
import socket
from taskexecutor.httpclient import ApiClient


class __ConfigMeta(type):
    def __init__(cls, name, bases, d):
        type.__init__(cls, name, bases, d)
        if "HMS_ENV" in os.environ.keys() and os.environ["HMS_ENV"] == "dev":
            _hms_env = "dev"
            _config_server_host = "172.17.0.1"
            _config_server_port = "8888"
            cls.enabled_resources = ["unixaccount", "dbaccount", "database",
                                     "website", "sslcertificate", "mailbox"]
            cls.enabled_actions = ["create", "update", "delete"]
        else:
            _hms_env = "dev"
            _config_server_host = "172.17.0.1"
            _config_server_port = "8888"
            cls.enabled_resources = ["unixaccount", "dbaccount", "database",
                                     "website", "sslcertificate", "mailbox"]
            cls.enabled_actions = ["create", "update", "delete"]
        cls.hostname = socket.gethostname().split('.')[0]
        with ApiClient(_config_server_host, _config_server_port) as cnf_api:
            _extra_attrs = {
                "amqp": {
                    "consumer_routing_key": "te.{}".format(cls.hostname)
                }
            }
            _global_config = cnf_api.get(uri="/te/{}".format(_hms_env),
                                         extra_attrs=_extra_attrs)
        for attr, value in vars(_global_config.propertySources[0].source).items():
            if not attr.startswith("_"):
                setattr(cls, attr, value)

    @classmethod
    def __setattr__(cls, name, value):
        if hasattr(cls, name):
            raise Exception("{} is a read-only attribute".format(name))
        setattr(cls, name, value)

    @classmethod
    def __str__(cls):
        _attr_list = list()
        for attr, value in vars(cls).items():
            if not attr.startswith("_"):
                _attr_list.append("{0}={1}".format(attr, value))
        return "Config({})".format(", ".join(_attr_list))

class Config(metaclass=__ConfigMeta):

    def __init__(self):
        raise Exception("Config is a non-instantiable class")

