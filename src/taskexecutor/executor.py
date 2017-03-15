import copy
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.httpsclient
import taskexecutor.task
import taskexecutor.utils

__all__ = ["Executor"]


class PropertyValidationError(Exception):
    pass


class UnknownTaskAction(Exception):
    pass


class Executor:
    def __init__(self, task, callback=None, args=None):
        self._task = None
        self._callback = None
        self._args = None
        self.task = task
        self.callback = callback
        self.args = args

    @property
    def task(self):
        return self._task

    @task.setter
    def task(self, value):
        if not isinstance(value, taskexecutor.task.Task):
            raise PropertyValidationError("task must be instance of Task class")
        self._task = value

    @task.deleter
    def task(self):
        del self._task

    @property
    def callback(self):
        return self._callback

    @callback.setter
    def callback(self, f):
        if f and not callable(f):
            raise PropertyValidationError("callback must be callable")
        self._callback = f

    @callback.deleter
    def callback(self):
        del self._callback

    @property
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        if value and not isinstance(value, (list, tuple)):
            raise PropertyValidationError("args must be list or tuple")
        self._args = value

    @args.deleter
    def args(self):
        del self._args

    def get_resource(self):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            return api.get(urllib.parse.urlparse(self.task.params["objRef"]).path)

    def get_all_resources(self):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            if self.task.res_type == "unix-account":
                return api.UnixAccount().filter(serverId=CONFIG.localserver.id).get()
            elif self.task.res_type == "mailbox":
                return api.Mailbox().filter(serverId=CONFIG.localserver.id).get()
            elif self.task.res_type == "database":
                resources = list()
                for service in CONFIG.localserver.services:
                    if service.serviceTemplate.serviceType.name.startswith("DATABASE_"):
                        resources += api.Database().filter(serviceId=service.id).get()
                return resources

    def process_command_task(self):
        LOGGER.info("Fetching {0} resource by {1}".format(self.task.res_type, self.task.params["objRef"]))
        resource = self.get_resource()
        processor = taskexecutor.constructor.get_resprocessor(self.task.res_type, resource, self.task.params)
        for prequestive_processor in taskexecutor.constructor.get_prequestive_resprocessors(processor):
            LOGGER.info("Updating necessary resource {}".format(prequestive_processor.resource))
            prequestive_processor.update()
        LOGGER.info("Invoking {0}.{1} method on {2}".format(type(processor).__name__,
                                                            self.task.action, processor.resource))
        getattr(processor, self.task.action)()
        for siding_processor in taskexecutor.constructor.get_siding_resprocessors(processor,
                                                                                  params={self.task.action: resource}):
            LOGGER.info("Updating affected resource {}".format(siding_processor.resource))
            siding_processor.update()
        LOGGER.info("Calling back {0}{1}".format(self._callback.__name__, self._args))
        self._callback(*self._args)

    def process_query_task(self):
        if self.task.action == "quota_report":
            collector = taskexecutor.constructor.get_rescollector(self.task.res_type, self.task.params["resource"])
            LOGGER.info("Collecting 'quotaUsed' property for '{0}' resource {1} "
                        "by {2}".format(self.task.res_type, collector.resource.name, type(collector).__name__))
            self.task.params["data"] = dict()
            self.task.params["data"]["quotaUsed"] = collector.get_property("quotaUsed",
                                                                           cache_ttl=self.task.params["interval"] - 1)

    def process_batch_query_task(self):
        LOGGER.info("Fetching all local {0} resources by type".format(self.task.res_type))
        for resource in self.get_all_resources():
            context = {"res_type": self.task.res_type, "action": self.task.action}
            params = copy.copy(self.task.params)
            params.update({"resource": resource})
            message = {"params": params}
            self._callback(context, message)

    def process_task(self):
        taskexecutor.utils.set_thread_name("OPERATION IDENTITY: {0.opid} ACTION IDENTITY: {0.actid}".format(self.task))
        if self.task.action in ("create", "update", "delete") and self.task.actid:
            reporter = taskexecutor.constructor.get_reporter("amqp")
            self.process_command_task()
        elif self.task.action == "quota_report":
            if "resource" not in self.task.params.keys():
                reporter = taskexecutor.constructor.get_reporter("null")
                self.process_batch_query_task()
            else:
                reporter = taskexecutor.constructor.get_reporter("https")
                self.process_query_task()
        else:
            raise UnknownTaskAction(self.task.action)
        report = reporter.create_report(self.task)
        LOGGER.info("Sending report {0} using {1}".format(report, type(reporter).__name__))
        reporter.send_report()
        LOGGER.info("Done with task {}".format(self.task))
