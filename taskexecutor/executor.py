import http.client
import json
from collections import namedtuple
from concurrent.futures import ThreadPoolExecutor

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.resprocessor import ResProcessorBuilder
from taskexecutor.reporter import ReporterBuilder
from taskexecutor.task import Task

class RESTClient:
	def __enter__(self):
		self._connection = http.client.HTTPConnection(
				"{host}:{port}".format_map(CONFIG["rest"])
		)
		return self

	def get(self, uri, type_name="Resource"):
		self._connection.request("GET", uri)
		resp = self._connection.getresponse()
		if resp.status != 200:
			raise Exception("GET failed, REST server returned "
			                "{0.status} {0.reason}".format(resp))
		json_str = self.decode_response(resp.read())
		return self.json_to_object(json_str, type_name)

	def decode_response(self, bytes):
		return bytes.decode("UTF-8")

	def json_to_object(self, json_str, type_name):
		return json.loads(
				json_str,
				object_hook=lambda d: namedtuple(type_name,
				                                 d.keys())(*d.values())
		)

	def __exit__(self, exc_type, exc_val, exc_tb):
		self._connection.close()


class Executors:
	class __Executors:
		def __init__(self, pool):
			self.pool = pool
	instance = None
	def __init__(self):
		if not Executors.instance:
			Executors.instance = Executors.__Executors(
					ThreadPoolExecutor(CONFIG["max_workers"])
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
		with RESTClient() as c:
			obj = c.get(obj_ref, "Resource")
		return obj

	def process_task(self):
		processor = ResProcessorBuilder(self._task.res_type)
		LOGGER.info(
				"Fetching {0} resorce by {1}".format(self._task.res_type,
				                                     self._task.params["objRef"])
		)
		processor.resource = self.get_resource(self._task.params["objRef"])
		processor.params = self._task.params
		LOGGER.info(
				"Invoking {0} {1} method on {2}".format(
						type(processor).__name__,
						self._task.action,
						processor.resource
				)
		)
		if self._task.action == "Create":
			processor.create()
		elif self._task.action == "Update":
			processor.update()
		elif self._task.action == "Delete":
			processor.delete()
		LOGGER.info("Calling back {0}{1}".format(self._callback.__name__, self._args))
		self._callback(*self._args)
		reporter = ReporterBuilder("amqp")
		report = reporter.create_report(self._task)
		LOGGER.info("Sending report {0} using {1}".format(report,
		                                                 type(reporter).__name__))
		reporter.send_report()
		LOGGER.info("Done with task {}".format(self._task.id))
