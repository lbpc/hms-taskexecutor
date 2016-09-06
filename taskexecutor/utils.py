import http.client
import urllib.parse
import mysql.connector
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from threading import current_thread
from traceback import format_exc
from collections import namedtuple
from functools import wraps
from threading import RLock
from jinja2 import FileSystemLoader, Environment

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER

LOCKS = {}

class ThreadPoolExecutorStackTraced(ThreadPoolExecutor):
	def submit(self, f, *args, **kwargs):
		return super(ThreadPoolExecutorStackTraced, self).submit(
				self._function_wrapper, f, *args, **kwargs)

	def _function_wrapper(self, fn, *args, **kwargs):
		try:
			return fn(*args, **kwargs)
		except Exception:
			raise Exception(format_exc())


class RESTClient:
	def __init__(self):
		self._uri = None

	def __enter__(self):
		self._connection = http.client.HTTPConnection(
				"{host}:{port}".format_map(CONFIG["rest"])
		)
		return self

	def get(self, uri=None):
		if not uri:
			uri = self._uri
		self._connection.request("GET", uri)
		resp = self._connection.getresponse()
		if resp.status != 200:
			raise Exception("GET failed, REST server returned "
			                "{0.status} {0.reason}".format(resp))
		json_str = self._decode_response(resp.read())
		return self._json_to_object(json_str)

	def _build_resource_uri(self, res_name, id):
		self._uri = "/{0}/{1}".format(res_name, id)
		return self

	def _build_collection_uri(self, res_name, query=None):
		if query:
			self._uri = "/{0}?{1}".format(res_name,
			                              urllib.parse.urlencode(query))
		else:
			self._uri = "/{}".format(res_name)
		return self

	def _decode_response(self, bytes):
		return bytes.decode("UTF-8")

	def _json_to_object(self, json_str):
		return json.loads(
				json_str,
				object_hook=lambda d: namedtuple("Resource",
				                                 d.keys())(*d.values())
		)

	def __getattr__(self, name):
		def wrapper(id=None, query=None):
			if id:
				return self._build_resource_uri(name, id)
			elif query:
				return self._build_collection_uri(name, query)
			else:
				return self._build_collection_uri(name)
		return wrapper

	def __exit__(self, exc_type, exc_val, exc_tb):
		self._connection.close()


class MySQLClient:
	def __init__(self, database="mysql"):
		self._connection = mysql.connector.connect(database=database,
		                                           **CONFIG["mysql"])

	def __enter__(self):
		self._cursor = self._connection.cursor()
		return self._cursor

	def __exit__(self, exc_type, exc_val, exc_tb):
		self._connection.commit()
		self._cursor.close()
		self._connection.close()


def exec_command(command):
	LOGGER.info("Running shell command: {}".format(command))
	with subprocess.Popen(command,
	                      stderr=subprocess.PIPE,
	                      shell=True,
	                      executable="/bin/bash") as proc:
		stderr = proc.stderr.read()
		proc.communicate()
		ret_code = proc.returncode
	if ret_code != 0:
		LOGGER.error("Command '{0}' returned {1} code".format(command, ret_code))
		if stderr:
			LOGGER.error("STDERR: {}".format(stderr.decode("UTF-8")))
		raise Exception("Failed to execute command '{}'".format(command))

def set_apparmor_mode(mode, binary):
	LOGGER.info("Applying {0} AppArmor mode on {1}".format(mode, binary))
	exec_command("aa-{0} {1}".format(mode, binary))

def render_template(template_name, **kwargs):
	template_env = Environment(loader=FileSystemLoader(CONFIG["paths"]["templates"]),
	                           lstrip_blocks=True,
                               trim_blocks=True)
	template = template_env.get_template(template_name)

	return template.render(**kwargs)

def set_thread_name(name):
	current_thread().name = name

def synchronized(f):
	@wraps(f)
	def wrapper(self, *args, **kwargs):
		if not f in LOCKS.keys():
			LOCKS[f] = RLock()
		with LOCKS[f]:
			return f(self, *args, **kwargs)
	return wrapper
