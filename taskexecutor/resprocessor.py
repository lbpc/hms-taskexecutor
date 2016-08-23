import os
import re
import shutil
from abc import ABCMeta, abstractmethod

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.utils import MySQLClient, exec_command


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
		self.adduser()
		self.setquota()

	def update(self):
		command = "usermod " \
		          "--move-home " \
		          "--home {0.homeDir} " \
		          "{1[username]}".format(self.resource, self.params)
		exec_command(command)

	def delete(self):
		self.killprocs()
		self.userdel()

	def adduser(self):
		command = "adduser " \
		          "--force-badname " \
		          "--disabled-password " \
		          "--gecos 'Hosting account' " \
		          "--uid {0.uid} " \
		          "--home {0.homeDir} " \
		          "{1[username]}".format(self.resource, self.params)
		exec_command(command)

	def setquota(self):
		command = "setquota " \
		          "-g {0.uid} 0 {1[quota]} " \
		          "0 0 /home".format(self.resource, self.params)
		exec_command(command)

	def userdel(self):
		command = "userdel " \
		          "--force " \
		          "--remove " \
		          "{0[username]}".format(self.params)
		exec_command(command)

	def killprocs(self):
		command = "killall -9 -u {0[username]} || true".format(self.params)
		exec_command(command)


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


class MailboxAtPopperProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		if not os.path.isdir(self.params["mailspool"]):
			LOGGER.info("Creating directory {}".format(self.params["mailspool"]))
			os.mkdir(self.params["mailspool"])

		else:
			LOGGER.info("Mail spool directory {} "
			            "already exists".format(self.params["mailspool"]))
		LOGGER.info("Setting owner {0[uid]} "
		            "for {0[mailspool]}".format(self.params))
		os.chown(self.params["mailspool"],
		         self.params["uid"],
		         self.params["uid"])

	def update(self):
		pass

	def delete(self):
		LOGGER.info("Removing {1[mailspool]}/{0.name} "
		            "recursively".format(self.resource, self.params))
		shutil.rmtree("{1[mailspool]}/{0.name}".format(self.resource, self.params))
		if len(os.listdir(self.params["mailspool"])) == 0:
			LOGGER.info("{1[mailspool]}/{0.name} was the last maildir, "
			            "removing spool itself".format(self.resource, self.params))
			os.rmdir(self.params["mailspool"])


class MailboxAtMxProcessor(ResProcessor):
	def __init__(self):
		super().__init__()

	def create(self):
		relay_file = "/etc/exim4/etc/relay_domains{}".format(self.params["popId"])
		with open(relay_file, "r") as f:
			relay_domains = [s.rstrip("\n\r") for s in f.readlines()]
		if not self.resource.domain in relay_domains:
			LOGGER.info("Appending {0} to {1}".format(self.resource.domain,
			                                          relay_file))
			relay_domains.append(self.resource.domain)

	def update(self):
		pass

	def delete(self):
		pass


class MailboxAtCheckerProcessor(ResProcessor):
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
		elif res_type == "Mailbox" and re.match("pop\d+",
		                                        CONFIG["hostname"]):
			return MailboxAtPopperProcessor()
		elif res_type == "Mailbox" and re.match("mx\d+-(mr|dh)",
		                                        CONFIG["hostname"]):
			return MailboxAtMxProcessor()
		elif res_type == "Mailbox" and re.match("mail-checker\d+",
		                                        CONFIG["hostname"]):
			return MailboxAtCheckerProcessor()
		elif res_type == "Database":
			return DatabaseProcessor()
		else:
			raise ValueError("Unknown resource type: {}".format(res_type))
