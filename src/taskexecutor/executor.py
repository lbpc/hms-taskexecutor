import concurrent.futures
import time
import traceback
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.httpsclient
import taskexecutor.task
import taskexecutor.utils

__all__ = ["Executors", "Executor"]


class PropertyValidationError(Exception):
    pass


class UnknownTaskAction(Exception):
    pass


class ThreadPoolExecutorStackTraced(concurrent.futures.ThreadPoolExecutor):
    def submit(self, f, *args, **kwargs):
        return super(ThreadPoolExecutorStackTraced, self).submit(self._function_wrapper, f, *args, **kwargs)

    @staticmethod
    def _function_wrapper(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            raise type(e)(traceback.format_exc())


class Executors:
    class __Executors:
        def __init__(self, pool):
            self.pool = pool

    command_instance = None
    query_instance = None

    def __init__(self):
        if not Executors.command_instance:
            Executors.command_instance = Executors.__Executors(ThreadPoolExecutorStackTraced(CONFIG.max_workers))
        else:
            self.pool = Executors.command_instance.pool

    def __getattr__(self, name):
        return getattr(self.command_instance, name)


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
        if not callable(f):
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
        if not isinstance(value, (list, tuple)):
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
                    if service.serviceType.name.startswith("DATABASE_"):
                        resources += api.Database().filter(serviceId=service.id).get()
                return resources

    def create_subtasks(self, resources):
        subtasks = list()
        for resource in resources:
            subtasks.append(taskexecutor.task.Task(self.task.opid, self.task.actid, self.task.res_type,
                                                   self.task.action, self.task.params.update({"resource": resource})))
        return subtasks

    def spawn_subexecutors(self, tasks, pool):
        futures = list()
        for task in tasks:
            subexecutor = Executor(task)
            LOGGER.debug("Spawning subexecutor for subtask: {}".format(task))
            futures.append(pool.submit(subexecutor.process_task))
        return futures

    def wait_for_subexecutors(self, futures):
        while futures:
            for future in futures:
                if not future.running():
                    if future.exception():
                        LOGGER.error(future.exception())
                    futures.remove(future)
            time.sleep(.1)

    def process_task(self):
        taskexecutor.utils.set_thread_name("OPERATION IDENTITY: {0.opid} ACTION IDENTITY: {0.actid}".format(self.task))
        constructor = taskexecutor.constructor.Constructor()
        if self.task.action in ("create", "update", "delete") and self.task.actid:
            LOGGER.info("Fetching {0} resource by {1}".format(self.task.res_type, self.task.params["objRef"]))
            resource = self.get_resource()
            processor = constructor.get_resprocessor(self.task.res_type, resource, self.task.params)
            reporter = constructor.get_reporter("amqp")
            LOGGER.info("Invoking {0}.{1} method on {2}".format(type(processor).__name__,
                                                                self.task.action, processor.resource))
            getattr(processor, self.task.action)()
            for processor in constructor.get_siding_resprocessors(processor):
                LOGGER.info("Updating affected resource {}".format(processor.resource))
                processor.update()
        elif self.task.action == "quota_report":
            if "resource" not in self.task.params.keys():
                LOGGER.info("Fetching all local {0} resources by type}".format(self.task.res_type))
                resources = self.get_all_resources()
                query_pool = constructor.get_query_executors_pool()
                subexecutors = self.spawn_subexecutors(self.create_subtasks(resources), query_pool)
                self.wait_for_subexecutors(subexecutors)
                return
            else:
                collector = constructor.get_rescollector(self.task.res_type, self.task.params["resource"])
                reporter = constructor.get_reporter("https")
                LOGGER.info("Collecting 'quotaUsed' property for resource {0} "
                            "by {1}".format(collector.resource, type(collector).__name__))
                self.task.params.data = dict()
                self.task.params["data"]["quotaUsed"] = \
                    collector.get_property("quotaUsed", cache_ttl=self.task.params["interval"] - 1)
        else:
            raise UnknownTaskAction(self.task.action)
        if self.callback:
            LOGGER.info("Calling back {0}{1}".format(self._callback.__name__, self._args))
            self._callback(*self._args)
        report = reporter.create_report(self.task)
        LOGGER.info("Sending report {0} using {1}".format(report, type(reporter).__name__))
        reporter.send_report()
        LOGGER.info("Done with task {}".format(self.task))
