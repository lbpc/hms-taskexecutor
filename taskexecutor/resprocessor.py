import subprocess
from abc import ABCMeta, abstractmethod
from logging import INFO, ERROR

from taskexecutor.logger import LOGGER, StreamToLogger
from taskexecutor.utils import MySQLClient


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
		command = "sudo adduser " \
		          "--force-badname " \
		          "--disabled-password " \
		          "--gecos 'Hosting account' " \
		          "--uid {0.uid} " \
		          "--home {0.homeDir} " \
		          "{1[username]}".format(self.resource, self.params)
		LOGGER.info("Running shell command: {}".format(command))
		self.exec_command(command)

	def update(self):
		command = "sudo usermod " \
		          "--move-home " \
		          "--home {0.homeDir} " \
		          "{1[username]}".format(self.resource, self.params)
		LOGGER.info("Running shell command: {}".format(command))
		self.exec_command(command)

	def delete(self):
		command = "sudo userdel " \
		          "--force " \
		          "--remove " \
		          "{0[username]}".format(self.params)
		LOGGER.info("Running shell command: {}".format(command))
		self.exec_command(command)

	def exec_command(self, command):
		subprocess.check_call(command,
		                      shell=True,
		                      executable="/bin/bash",
		                      stdout=StreamToLogger(LOGGER, INFO),
		                      stderr=StreamToLogger(LOGGER, ERROR))

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
		query = "CREATE USER `{0.name}`@`{1[addr]}` " \
		        "IDENTIFIED BY PASSWORD '{1[passHash]}'".format(self.resource,
		                                                        self.params)
		LOGGER.info("Executing query: {}".format(query))
		with MySQLClient("mysql") as c:
			c.execute(query)


	def update(self):
		pass

	def delete(self):
		query = "DROP USER {0.name}".format(self.resource)
		LOGGER.info("Executing query: {}".format(query))
		with MySQLClient("mysql") as c:
			c.execute(query)


class WebSiteProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		pass

	def update(self):
		pass

	def delete(self):
		pass


class MailboxProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		pass

	def update(self):
		pass

	def delete(self):
		pass


class DatabaseProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		grant_query = "GRANT " \
		              "SELECT, " \
		              "INSERT, " \
		              "UPDATE, " \
		              "DELETE, " \
		              "CREATE, " \
		              "DROP, " \
		              "REFERENCES, " \
		              "INDEX, " \
		              "ALTER, " \
		              "CREATE TEMPORARY TABLES, " \
		              "LOCK TABLES, " \
		              "CREATE VIEW, " \
		              "SHOW VIEW, " \
		              "CREATE ROUTINE, " \
		              "ALTER ROUTINE, " \
		              "EXECUTE" \
		              " ON `{0.name}`.* TO `{0.user}`@`{1[addr]}%` " \
		              "IDENTIFIED BY PASSWORD " \
		              "'{1[passHash]}'".format(self.resource, self.params)
		create_query = "CREATE DATABASE IF NOT EXISTS {0.name}".format(self.resource)
		LOGGER.info("Executing queries: {0}; {1}".format(grant_query,
		                                                 create_query))
		with MySQLClient("mysql") as c:
			c.execute(grant_query)
			c.execute(create_query)

	def update(self):
		pass

	def delete(self):
		query = "DROP DATABASE {0.name}".format(self.resource)
		LOGGER.info("Executing query: {}".format(query))
		with MySQLClient("mysql") as c:
			c.execute(query)


class ResProcessorBuilder:
	def __new__(self, res_type):
		if res_type == "UnixAccount":
			return UnixAccountProcessor()
		elif res_type == "FTPAccount":
			return FTPAccountProcessor()
		elif res_type == "DBAccount":
			return DBAccountProcessor()
		elif res_type == "Website":
			return WebSiteProcessor()
		elif res_type == "Mailbox":
			return MailboxProcessor()
		elif res_type == "Database":
			return DatabaseProcessor()
		else:
			raise ValueError("Unknown resource type: {}".format(res_type))
