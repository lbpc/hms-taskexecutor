import abc
import json
import pika

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.utils
import taskexecutor.httpsclient

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class Reporter(metaclass=abc.ABCMeta):
    def __init__(self):
        self._report = dict()

    @abc.abstractmethod
    def create_report(self, task):
        pass

    @abc.abstractmethod
    def send_report(self):
        pass


class AMQPReporter(Reporter):
    def __init__(self):
        super().__init__()
        self._url = "amqp://{0.user}:{0.password}@{0.host}:5672/%2F" \
                    "?heartbeat_interval={0.heartbeat_interval}".format(
                        CONFIG.amqp
                    )
        self._connection = None
        self._channel = None
        self._exchange = None
        self._routing_key = None
        self._task = None

    def _connnect(self):
        return pika.BlockingConnection(pika.URLParameters(self._url))

    def _open_channel(self):
        return self._connection.channel()

    def _close_channel(self):
        self._channel.close()

    def _declare_exchange(self, exchange, exchange_type):
        self._channel.exchange_declare(exchange=exchange, type=exchange_type, auto_delete=False)

    def _publish_message(self, message):
        self._channel.basic_publish(exchange=self._exchange,
                                    routing_key=self._routing_key,
                                    properties=pika.BasicProperties(headers={"provider": "te"}),
                                    body=message)

    def create_report(self, task):
        self._task = task
        self._report["operationIdentity"] = task.opid
        self._report["actionIdentity"] = task.actid
        self._report["objRef"] = task.params["objRef"]
        self._report["params"] = {"success": True}
        return self._report

    def send_report(self):
        self._exchange = "{0}.{1}".format(self._task.res_type,
                                          self._task.action)
        self._routing_key = self._task.params["provider"].replace("-", ".")
        LOGGER.info("Publishing to {0} exchange with "
                    "{1} routing key".format(self._exchange, self._routing_key))
        self._connection = self._connnect()
        self._channel = self._open_channel()
        self._declare_exchange(self._exchange, CONFIG.amqp.exchange_type)
        self._publish_message(json.dumps(self._report))
        self._close_channel()


class HttpsReporter(Reporter):
    def __init__(self):
        super().__init__()
        self._task = None
        self._resource = None

    def create_report(self, task):
        self._task = task
        self._resource = task.params["resource"]
        self._report = task.params["data"]
        return self._report

    def send_report(self):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            Resource = getattr(api, taskexecutor.utils.to_camel_case(self._task.res_type))
            Resource("{0}/{1}".format(self._resource.id, self._task.action)).post(json.dumps(self._report))

class Builder:
    def __new__(cls, reporter_type):
        if reporter_type == "amqp":
            return AMQPReporter
        elif reporter_type == "https":
            return HttpsReporter
        else:
            raise BuilderTypeError("Unknown Reporter type: {}".format(reporter_type))
