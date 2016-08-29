import os
import re
import shutil
from itertools import product
from abc import ABCMeta, abstractmethod

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.utils import MySQLClient, RESTClient, \
	exec_command, render_template, synchronized
from taskexecutor.opservice import Nginx, Apache

class ResProcessor(metaclass=ABCMeta):
	def __init__(self, resource, params):
		self._resource = object()
		self._params = dict()
		self.resource = resource
		self.params = params

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
	def __init__(self, resource, params):
		super().__init__(resource, params)

	def create(self):
		self.adduser()
		self.setquota()

	def update(self):
		command = "usermod " \
		          "--move-home " \
		          "--home {0.homeDir} " \
		          "{0.name}".format(self.resource)
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
		          "{0.name}".format(self.resource)
		exec_command(command)

	def setquota(self):
		command = "setquota " \
		          "-g {0.uid} 0 {0.quota} " \
		          "0 0 /home".format(self.resource)
		exec_command(command)

	def userdel(self):
		command = "userdel " \
		          "--force " \
		          "--remove " \
		          "{0.name}".format(self.resource)
		exec_command(command)

	def killprocs(self):
		command = "killall -9 -u {0.name} || true".format(self.resource)
		exec_command(command)


class DBAccountProcessor(ResProcessor):
	def __init__(self, resource, params):
		super().__init__(resource, params)

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
	def __init__(self, resource, params):
		super().__init__(resource, params)
		self._nginx = Nginx()
		self._apache = Apache(
				"{0}-{1}".format(self.resource.Options["phpVersion"],
				                 self.resource.Options["phpSecurityMode"])
		)
		self.set_cfg_paths()

	@synchronized
	def create(self):
		self._nginx.config_body = render_template("ApacheVHost.tmpl",
		                                          website=self.resource,
		                                          params=self.params)

		self._apache.config_body = render_template("NginxServer.tmpl",
		                                           website=self.resource,
		                                           params=self.params)
		for srv in (self._apache, self._nginx):
			self.save_config(srv.config_body, srv.available_cfg_path)
			LOGGER.info("Linking {0} to {1}".format(srv.available_cfg_path,
			                                        srv.enabled_cfg_path))
			try:
				os.symlink(srv.available_cfg_path, srv.enabled_cfg_path)
			except FileExistsError:
				if os.path.islink(srv.enabled_cfg_path) and \
					os.readlink(srv.enabled_cfg_path) == srv.available_cfg_path:
					LOGGER.info("Symlink {} "
					            "already exists".format(srv.enabled_cfg_path))
				else:
					raise
			srv.reload()

	def update(self):
		if self.resource.switchedOff:
			for srv in (self._apache, self._nginx):
				LOGGER.info("Removing {} symlink".format(srv.enabled_cfg_path))
				os.unlink(srv.enabled_cfg_path)
				srv.reload()
		else:
			self.create()

	@synchronized
	def delete(self):
		for srv in (self._apache, self._nginx):
			if os.path.exists(srv.enabled_cfg_path):
				LOGGER.info("Removing {} symlink".format(srv.enabled_cfg_path))
				os.unlink(srv.enabled_cfg_path)
			LOGGER.info("Removing {} file".format(srv.available_cfg_path))
			os.unlink(srv.available_cfg_path)
			srv.reload()

	def set_cfg_paths(self):
		for srv, type in product((self._apache, self._nginx),
		                         ("available", "enabled")):
			srv.__setattr__("{}_cfg_path".format(type),
			                "{0}/sites-{1}/{2}.conf".format(srv.cfg_base,
			                                                type,
			                                                self.resource.id))

	def save_config(self, body, file):
		LOGGER.info("Saving {}".format(file))
		with open("{}.new".format(file), "w") as f:
			f.write(body)
		os.rename("{}.new".format(file), file)

class MailboxAtPopperProcessor(ResProcessor):
	def __init__(self, resource, params):
		super().__init__(resource, params)

	def create(self):
		if not os.path.isdir(self.resource.mailSpool):
			LOGGER.info("Creating directory {}".format(self.resource.mailSpool))
			os.mkdir(self.resource.mailSpool)

		else:
			LOGGER.info("Mail spool directory {} "
			            "already exists".format(self.resource.mailSpool))
		LOGGER.info("Setting owner {0.unixAccount.uid} "
		            "for {0.mailSpool}".format(self.resource))
		os.chown(self.resource.mailSpool,
		         self.resource.unixAccount.uid,
		         self.resource.unixAccount.uid)

	def update(self):
		pass

	def delete(self):
		LOGGER.info("Removing {0.mailSpool]}/{0.name} "
		            "recursively".format(self.resource))
		shutil.rmtree("{0.mailSpool]}/{0.name}".format(self.resource))
		if len(os.listdir(self.resource.mailSpool)) == 0:
			LOGGER.info("{0.mailSpool}/{0.name} was the last maildir, "
			            "removing spool itself".format(self.resource))
			os.rmdir(self.resource.mailSpool)


class MailboxAtMxProcessor(ResProcessor):
	def __init__(self, resource, params):
		super().__init__(resource, params)
		self._relay_file = "/etc/exim4/etc/" \
		                   "relay_domains{}".format(self.resource.popServer.id)
		self._relay_domains = self.get_domain_list(self._relay_file)

	@synchronized
	def create(self):
		if not self.resource.domain.name in self._relay_domains:
			LOGGER.info("Appending {0} to {1}".format(self.resource.domain.name,
			                                          self._relay_file))
			self._relay_domains.append(self.resource.domain.name)
			self.save_domain_list(self._relay_domains, self._relay_file)
		else:
			LOGGER.info("{0} already exists in {1}, nothing to do".format(
					self.resource.domain.name, self._relay_file
			))

	def update(self):
		self.create()

	@synchronized
	def delete(self):
		with RESTClient() as c:
			_mailboxes_remaining = \
				c.get("/Mailbox/?domain={}".format(self.resource.domain.name))
		if len(_mailboxes_remaining) == 1:
			LOGGER.info("{0.name}@{0.domain} is the last mailbox in {0.domain}, "
			            "removing domain from {1}".format(self.resource,
			                                              self._relay_file))
			self._relay_domains.remove(self.resource.domain.name)
			self.save_domain_list(self._relay_domains, self._relay_file)
		else:
			LOGGER.info("Nothing to do here")

	def get_domain_list(self, file):
		with open(file, "r") as f:
			return [s.rstrip("\n\r") for s in f.readlines()]

	def save_domain_list(self, list, file):
		with open("{}.new".format(file), "w") as f:
			for domain in list:
				f.writelines("{}\n".format(domain))
		os.rename("{}.new".format(file), file)


class MailboxAtCheckerProcessor(ResProcessor):
	def __init__(self, resource, params):
		super().__init__(resource, params)

	def create(self):
		pass

	def update(self):
		pass

	def delete(self):
		pass


class DatabaseProcessor(ResProcessor):
	def __init__(self, resource, params):
		super().__init__(resource, params)

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
			return UnixAccountProcessor
		elif res_type == "DBAccount":
			return DBAccountProcessor
		elif res_type == "WebSite":
			return WebSiteProcessor
		elif res_type == "Mailbox" and re.match("pop\d+",
		                                        CONFIG["hostname"]):
			return MailboxAtPopperProcessor
		elif res_type == "Mailbox" and re.match("mx\d+-(mr|dh)",
		                                        CONFIG["hostname"]):
			return MailboxAtMxProcessor
		elif res_type == "Mailbox" and re.match("mail-checker\d+",
		                                        CONFIG["hostname"]):
			return MailboxAtCheckerProcessor
		elif res_type == "Database":
			return DatabaseProcessor
		else:
			raise ValueError("Unknown resource type: {}".format(res_type))
