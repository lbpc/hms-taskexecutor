import threading

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.resprocessor import ResProcessorBuilder
from taskexecutor.reporter import ReporterBuilder
from taskexecutor.task import Task
from taskexecutor.utils import RESTClient, ThreadPoolExecutorStackTraced


class Executors:
	class __Executors:
		def __init__(self, pool):
			self.pool = pool
	instance = None
	def __init__(self):
		if not Executors.instance:
			Executors.instance = Executors.__Executors(
					ThreadPoolExecutorStackTraced(CONFIG["max_workers"])
			)
		else:
			self.pool = Executors.instance.pool
	def __getattr__(self, name):
		return getattr(self.instance, name)


class Executor:
	def __init__(self, task, callback=None, args=None):
		self.task = task
		self.callback = callback
		self.args = args

	@property
	def task(self):
		return self._task

	@task.setter
	def task(self, value):
		if type(value) != Task:
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
	def agrs(self):
		del self._args

	def get_resource(self, obj_ref):
		with RESTClient() as api:
			return api.get(obj_ref)

	def process_task(self):
		threading.current_thread().name = self._task.id
		LOGGER.info(
				"Fetching {0} resorce by {1}".format(self._task.res_type,
				                                     self._task.params["objRef"])
		)
		_resource = self.get_resource(self._task.params["objRef"])
		processor = ResProcessorBuilder(self._task.res_type)(_resource,
		                                                     self._task.params)
		LOGGER.info(
				"Invoking {0} {1} method on {2}".format(
						type(processor).__name__,
						self._task.action,
						processor.resource
				)
		)
		if self._task.action == "create":
			processor.create()
		elif self._task.action == "update":
			processor.update()
		elif self._task.action == "delete":
			processor.delete()
		LOGGER.info("Calling back {0}{1}".format(self._callback.__name__, self._args))
		self._callback(*self._args)
		reporter = ReporterBuilder("amqp")()
		report = reporter.create_report(self._task)
		LOGGER.info("Sending report {0} using {1}".format(report,
		                                                 type(reporter).__name__))
		reporter.send_report()
		LOGGER.info("Done with task {}".format(self._task.id))
