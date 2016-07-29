import http.client
import json
from collections import namedtuple

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.resprocessor import ResProcessorBuilder
from taskexecutor.task import Task


class Executor:
	def __init__(self):
		self._task = Task()

	@property
	def task(self):
		return self._task

	@task.setter
	def task(self, value):
		self._task = value

	@task.deleter
	def task(self):
		del self._task

	def rest_connect(self):
		return http.client.HTTPConnection(
				"{host}:{port}".format_map(CONFIG["rest"])
		)

	def rest_get(self, uri):
		conn = self.rest_connect()
		conn.request("GET", uri)
		resp = conn.getresponse()
		if resp.status != 200:
			raise Exception("GET failed, REST server returned "
			                "{0.status} {0.reason}".format(resp))
		return self.decode_response(resp.read())

	def decode_response(self, bytes):
		return bytes.decode("UTF-8")

	def json_to_res_object(self, json_str):
		return json.loads(json_str,
		                  object_hook=lambda d: namedtuple('Resource', d.keys())(*d.values())
		                  )

	def get_resource(self, obj_ref):
		return self.json_to_res_object(self.rest_get(obj_ref))

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
