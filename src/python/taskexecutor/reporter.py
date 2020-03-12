import abc
import json
import pika
import alertaclient.api as alerta

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.task
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
        self._url = "amqp://{0.user}:{0.password}@{0.host}:{0.port}/%2F" \
                    "?heartbeat_interval={0.heartbeat_interval}".format(
                        CONFIG.amqp
                    )
        self._connection = None
        self._channel = None
        self._exchange = None
        self._routing_key = None
        self._task = None
        self.next_te = None

    @property
    def _report_to_next_te(self):
        return self.next_te and self._task.res_type == "website"

    def _connnect(self):
        return pika.BlockingConnection(pika.URLParameters(self._url))

    def _open_channel(self):
        return self._connection.channel()

    def _close_channel(self):
        self._channel.close()

    def _declare_exchange(self, exchange, exchange_type):
        self._channel.exchange_declare(exchange=exchange,
                                       type=exchange_type,
                                       auto_delete=False,
                                       durable=bool(CONFIG.amqp._asdict().get("exchange_durability")))

    def _publish_message(self, message, provider="te"):
        self._channel.basic_publish(exchange=self._exchange,
                                    routing_key=self._routing_key,
                                    properties=pika.BasicProperties(headers={"provider": provider},
                                                                    content_type='application/json'),
                                    body=message)

    def create_report(self, task):
        self._task = task
        params = task.params
        if params.get("success"):
            del params["success"]
        self._report["operationIdentity"] = task.opid
        self._report["actionIdentity"] = task.actid
        self._report["objRef"] = params["objRef"]
        self.next_te = params.pop("oldServerName", None)
        self._report["params"] = {"success": bool(task.state ^ taskexecutor.task.FAILED)}
        if "last_exception" in params:
            self._report["params"]["errorMessage"] = params["last_exception"].get("message")
            self._report["params"]["exceptionClass"] = params["last_exception"].get("class")
        LOGGER.debug("Report to next TE: {}".format(self._report_to_next_te))
        if self._report_to_next_te:
            for k in ("resource", "dataPostprocessorType", "dataPostprocessorArgs"):
                if k in params.keys():
                    del params[k]
            params["paramsForRequiredResources"] = {"forceSwitchOff": True}
            if "httpProxyIp" in params.keys():
                params["newHttpProxyIp"] = params["httpProxyIp"]
            self._report["params"] = params
        return self._report

    def send_report(self):
        self._exchange = "{0}.{1}".format(self._task.res_type,
                                          self._task.action)
        self._routing_key = self._task.params["provider"].replace("-", ".") if not self._report_to_next_te \
            else "te.{}".format(self.next_te)
        provider = self._task.params["provider"] if self._report_to_next_te else "te"
        LOGGER.info("Publishing to {0} exchange with {1} routing key, headers: provider={2}, "
                    "payload: {3}".format(self._exchange, self._routing_key, provider, self._report))
        self._connection = self._connnect()
        self._channel = self._open_channel()
        self._declare_exchange(self._exchange, CONFIG.amqp.exchange_type)
        self._publish_message(json.dumps(self._report), provider=provider)
        self._close_channel()


class HttpsReporter(Reporter):
    def __init__(self):
        super().__init__()
        self._task = None
        self._resource = None

    def create_report(self, task):
        self._task = task
        self._resource = task.params.get("resource")
        if self._resource:
            self._report = task.params["data"]
        return self._report

    def send_report(self):
        if not self._resource:
            LOGGER.warning("Attepmted to send report without resource: {0._report}, task: {0._task}".format(self))
            return
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            Resource = getattr(api, taskexecutor.utils.to_camel_case(self._task.res_type))
            endpoint = "{0}/{1}".format(self._resource.id, taskexecutor.utils.to_lower_dashed(self._task.action))
            Resource(endpoint).post(json.dumps(self._report))


class AlertaReporter(Reporter):
    def __init__(self):
        super().__init__()
        self._alerta = alerta.Client(**CONFIG.alerta._asdict())

    def create_report(self, task):
        success = bool(task.state ^ taskexecutor.task.FAILED)
        attributes = dict(publicParams=[],
                          tag=task.tag,
                          origin=str(task.origin),
                          opid=task.opid,
                          actid=task.actid,
                          res_type=task.res_type,
                          action=task.action)
        try:
            resource = task.params.pop("resource")
            task.params["hmsResource"] = resource._asdict()
        except KeyError:
            pass
        attributes.update(task.params)
        self._report = dict(environment="HMS",
                            service=["taskexecutor"],
                            resource=task.actid,
                            event="task.finished",
                            value={True: "Ok", False: "Failed"}[success],
                            text="Done" if success else task.params.get("last_exception", "Failed"),
                            severity={True: "Ok", False: "Minor"}[success],
                            hostname=CONFIG.hostname,
                            attributes=attributes)
        return self._report

    def send_report(self):
        try:
            self._alerta.send_alert(**self._report)
        except Exception as e:
            LOGGER.error("Failed to send report to Alerta: {}".format(e))


class NullReporter(Reporter):
    def create_report(self, task):
        return

    def send_report(self):
        pass


class Builder:
    def __new__(cls, reporter_type):
        ReporterClass = {"amqp": AMQPReporter,
                         "https": HttpsReporter,
                         "alerta": AlertaReporter,
                         "null": NullReporter}.get(reporter_type)
        if not ReporterClass:
            raise BuilderTypeError("Unknown Reporter type: {}".format(reporter_type))
        return ReporterClass
