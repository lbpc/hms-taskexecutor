import json
from abc import ABCMeta, abstractmethod
import pika
from taskexecutor.config import CONFIG


class Reporter(metaclass=ABCMeta):
    def __init__(self):
        self._report = dict()

    @abstractmethod
    def create_report(self, task):
        pass

    @abstractmethod
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
        self._exchange = "service.rc.user"
        self._routing_key = "service.rc.user"

    def _connnect(self):
        return pika.BlockingConnection(pika.URLParameters(self._url))

    def _open_channel(self):
        return self._connection.channel()

    def _close_channel(self):
        self._channel.close()

    def _declare_exchange(self, exchange, exchange_type):
        self._channel.exchange_declare(exchange=exchange,
                                       type=exchange_type,
                                       auto_delete=False)

    def _publish_message(self, message):
        self._channel.basic_publish(exchange=self._exchange,
                                    routing_key=self._routing_key,
                                    body=message)

    def _report_to_json(self):
        return json.dumps(self._report)

    def create_report(self, task):
        self._report["operationIdentity"] = task.opid
        self._report["actionIdentity"] = task.actid
        self._report["objRef"] = task.params["objRef"]
        self._report["params"] = {"success": True}
        return self._report

    def send_report(self):
        self._connection = self._connnect()
        self._channel = self._open_channel()
        self._declare_exchange(self._exchange, CONFIG.amqp.exchange_type)
        self._publish_message(self._report_to_json())
        self._close_channel()


class ReporterBuilder:
    def __new__(cls, reporter_type):
        if reporter_type == "amqp":
            return AMQPReporter
        else:
            raise ValueError("Unknown Reporter type: {}".format(reporter_type))
