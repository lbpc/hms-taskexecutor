import subprocess
import sys
from abc import ABCMeta, abstractmethod

from taskexecutor.logger import LOGGER


class ResProcessor(metaclass=ABCMeta):
	def __init__(self):
		self._resource = dict()
		self._params = dict()

	@property
	def resource(self):
		return self._resource

	@resource.setter
	def resource(self, value):
		self._resource = value

	@resource.deleter
	def resource(self):
		del self._resource

	@property
	def params(self):
		return self._params

	@params.setter
	def params(self, value):
		self._params = value

	@params.deleter
	def params(self):
		del self._params

	@abstractmethod
	def create(self):
		pass

	@abstractmethod
	def update(self):
		pass

	@abstractmethod
	def delete(self):
		pass


class UnixAccountProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		command = "adduser " \
		          "--force-badname " \
		          "--disabled-password " \
		          "--gecos 'Hosting account' " \
		          "--uid {0.uid} " \
		          "--home {0.homeDir} " \
		          "{1[username]}".format(self._resource, self._params)
		LOGGER.info("Running shell command: {}".format(command))
		self.exec_command(command)

	def update(self):
		command = "usermod " \
		          "--move-home " \
		          "--home {0.homeDir} " \
		          "{1[username]}".format(self._resource, self._params)
		LOGGER.info("Running shell command: {}".format(command))
		self.exec_command(command)

	def delete(self):
		command = "userdel " \
		          "--force " \
		          "--remove " \
		          "{0[username]}".format(self._params)
		LOGGER.info("Running shell command: {}".format(command))
		self.exec_command(command)

	def exec_command(self, command):
		subprocess.check_call(command,
		                        shell=True,
		                        executable="/bin/bash",
		                        stderr=sys.stderr)
		sys.stdout.write("STDOUT:")


class FTPAccountProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		pass

	def update(self):
		pass

	def delete(self):
		pass


class DBAccountProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		pass

	def update(self):
		pass

	def delete(self):
		pass


class WebAccessAccountProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		pass

	def update(self):
		pass

	def delete(self):
		pass


class ResProcessorBuilder:
	def __new__(self, res_type):
		if res_type == "UnixAccount":
			return UnixAccountProcessor()
		elif res_type == "FTPAccount":
			return FTPAccountProcessor()
		elif res_type == "DBAccount":
			return DBAccountProcessor()
		elif res_type == "WebAccessAccount":
			return WebAccessAccountProcessor()
		else:
			raise ValueError("Unknown resource type: {}".format(res_type))
