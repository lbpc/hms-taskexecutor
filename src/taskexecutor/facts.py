import abc
import json
import os
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.dbclient
import taskexecutor.httpsclient
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class FactsReporter(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def get_resources(self):
        pass

    @abc.abstractmethod
    def get_quota(self):
        pass

    @abc.abstractmethod
    def report_quota(self):
        pass


class UnixAccountFactsReporter(FactsReporter):
    def get_resources(self):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            return api.UnixAccount().filter(serverId=CONFIG.localserver.id).get()

    def get_quota(self):
        resources = self.get_resources()
        constructor = taskexecutor.constructor.Constructor()
        if isinstance(resources, list):
            op_service = constructor.get_opservice_by_resource(resources[0], "unix-account")
        else:
            op_service = constructor.get_opservice_by_resource(resources, "unix-account")
        uid_quotaused_mapping = op_service.get_quota_used([res.uid for res in resources])
        for res in resources:
            LOGGER.info("UnixAccount {0} quota usage: {1} bytes".format(res.name, uid_quotaused_mapping[res.uid]))
        return ((res.id, uid_quotaused_mapping[res.uid]) for res in resources)

    def report_quota(self):
        taskexecutor.utils.set_thread_name("UnixAccountFactsReporter")
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            for res_id, quota_used in self.get_quota():
                api.UnixAccount(res_id).quota_report().post(json.dumps({"quotaUsed": quota_used}))


class DatabaseFactsReporter(FactsReporter):
    def __init__(self):
        self._db_services = list()

    @property
    def db_services(self):
        if not self._db_services:
            for service in CONFIG.localserver.services:
                if service.serviceType.name.startswith("DATABASE_"):
                    self._db_services.append(service)
        return self._db_services

    def get_resources(self):
        resources = list()
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            for service in self.db_services:
                resources.append(api.Database().filter(serviceId=service.id).get())
        return resources

    def get_quota(self):
        quota_used = []
        resources = self.get_resources()
        service_resource_mapping = {service: [res for res in resources if res.serviceId == service.id]
                                    for service in self.db_services}
        constructor = taskexecutor.constructor.Constructor()
        for service, resources in service_resource_mapping:
            op_service = constructor.get_opservice(service)
            database_quotaused_mapping = op_service.get_quota_used([res.name for res in resources])
            for res in resources:
                LOGGER.info("Database {0} quota usage: {1} bytes".format(res.name, database_quotaused_mapping[res.uid]))
            quota_used += ((res.id, database_quotaused_mapping[res.name]) for res in resources)
        return quota_used

    def report_quota(self):
        taskexecutor.utils.set_thread_name("DatabaseFactsReporter")
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            for res_id, quota_used in self.get_quota():
                api.Database(res_id).quota_report().post(json.dumps({"quotaUsed": quota_used}))


class MailboxFactsReporter(FactsReporter):
    def get_resources(self):
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            return api.Mailbox().filter(serverId=CONFIG.localserver.id).get()

    def get_quota(self):
        resources = self.get_resources()
        constructor = taskexecutor.constructor.Constructor()
        if isinstance(resources, list):
            op_service = constructor.get_opservice_by_resource(resources[0], "mailbox")
        else:
            op_service = constructor.get_opservice_by_resource(resources, "mailbox")
        maildir_quotaused_mapping = \
            op_service.get_quota_used([os.path.join(res.mailSpool, res.name) for res in resources])
        for res in resources:
            LOGGER.info("Mailbox {0}@{1} quota usage: "
                        "{2} bytes".format(res.name, res.domain.name, maildir_quotaused_mapping[res.uid]))
        return ((res.id, maildir_quotaused_mapping[os.path.join(res.mailSpool, res.name)]) for res in resources)

    def report_quota(self):
        taskexecutor.utils.set_thread_name("MailboxFactsReporter")
        with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
            for res_id, quota_used in self.get_quota():
                api.Mailbox(res_id).quota_report().post(json.dumps({"quotaUsed": quota_used}))


class Builder:
    def __new__(cls, res_type):
        if res_type == "unix-account":
            return UnixAccountFactsReporter
        elif res_type == "database":
            return DatabaseFactsReporter
        elif res_type == "mailbox":
            return MailboxFactsReporter
        else:
            raise BuilderTypeError("No FactsReporter defined for {}".format(res_type))
