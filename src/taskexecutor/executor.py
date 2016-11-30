import concurrent.futures
import traceback
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.reporter
import taskexecutor.resprocessor
import taskexecutor.task
import taskexecutor.opservice
import taskexecutor.httpsclient
import taskexecutor.utils

__all__ = ["Executors", "Executor"]


class ThreadPoolExecutorStackTraced(concurrent.futures.ThreadPoolExecutor):
    def submit(self, f, *args, **kwargs):
        return super(ThreadPoolExecutorStackTraced, self).submit(self._function_wrapper, f, *args, **kwargs)

    @staticmethod
    def _function_wrapper(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            raise Exception(traceback.format_exc())


class Executors:
    class __Executors:
        def __init__(self, pool):
            self.pool = pool

    instance = None

    def __init__(self):
        if not Executors.instance:
            Executors.instance = Executors.__Executors(ThreadPoolExecutorStackTraced(CONFIG.max_workers))
        else:
            self.pool = Executors.instance.pool

    def __getattr__(self, name):
        return getattr(self.instance, name)


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
            raise TypeError("task must be instance of Task class")
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
            raise TypeError("callback must be callable")
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
            raise TypeError("args must be list or tuple")
        self._args = value

    @args.deleter
    def args(self):
        del self._args

    @staticmethod
    def get_resource(obj_ref):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            return api.get(urllib.parse.urlparse(obj_ref).path)

    def process_task(self):
        taskexecutor.utils.set_thread_name("OPERATION IDENTITY: {0.opid} ACTION IDENTITY: {0.actid}".format(self.task))
        constructor = taskexecutor.constructor.Constructor()
        LOGGER.info("Fetching {0} resource by {1}".format(self.task.res_type, self.task.params["objRef"]))
        resource = self.get_resource(self.task.params["objRef"])
        processor = constructor.get_resprocessor(self.task.res_type, resource, self.task.params)
        LOGGER.info(
                "Invoking {0}.{1} method on {2}".format(type(processor).__name__, self.task.action, processor.resource)
        )
        getattr(processor, self.task.action)()
        for processor in constructor.get_siding_resprocessors(processor):
            LOGGER.info("Updating affected resource {}".format(processor.resource))
            processor.update()
        LOGGER.info("Calling back {0}{1}".format(self._callback.__name__, self._args))
        self._callback(*self._args)
        reporter = constructor.get_reporter("amqp")
        report = reporter.create_report(self.task)
        LOGGER.info("Sending report {0} using {1}".format(report, type(reporter).__name__))
        reporter.send_report()
        LOGGER.info("Done with task {}".format(self.task))
