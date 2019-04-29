import copy
import collections
import datetime
import os
import pickle
import time
import urllib.parse
import queue

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.httpsclient
import taskexecutor.constructor
import taskexecutor.listener
import taskexecutor.task
import taskexecutor.watchdog
import taskexecutor.utils

__all__ = ["Executor"]


class ResourceBuildingError(Exception):
    pass


class PropertyValidationError(Exception):
    pass


class UnknownTaskAction(Exception):
    pass


class ResourceBuilder:
    def __init__(self, res_type, obj_ref=""):
        self._res_type = res_type
        self._obj_ref = obj_ref
        self._resources = list()

    @property
    def resources(self):
        if not self._resources:
            obj_ref = urllib.parse.urlparse(self._obj_ref).path
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                if obj_ref:
                    LOGGER.debug("Fetching {0} resource by {1}".format(self._res_type, obj_ref))
                    resource = api.get(obj_ref)
                    if not resource:
                        raise ResourceBuildingError("Failed to fetch resource "
                                                    "by directly provided objRef: {}".format(obj_ref))
                    self._resources.append(resource)
                elif self._res_type in ("unix-account", "mailbox"):
                    self._resources.extend(api.resource(self._res_type).filter(serverId=CONFIG.localserver.id).get())
                elif self._res_type == "service":
                    self._resources.extend(CONFIG.localserver.services)
                else:
                    service_type_resource = taskexecutor.utils.to_camel_case(self._res_type).upper()
                    for service in CONFIG.localserver.services:
                        if service.serviceTemplate.serviceType.name.startswith(service_type_resource):
                            self._resources.extend(api.resource(self._res_type).filter(serviceId=service.id).get())
        return self._resources

    def get_required_resources(self, resource=None):
        resources = [resource] if resource else self._resources
        required_resources = list()
        for resource in resources:
            if self._res_type == "website":
                LOGGER.debug("website depends on unix-account and ssl-certificate")
                required_resources.append(("unix-account", resource.unixAccount))
                required_resources.extend([("ssl-certificate", d.sslCertificate) for d in
                                           resource.domains if d.sslCertificate])
            elif self._res_type == "redirect" and resource.domain.sslCertificate:
                LOGGER.debug("redirect depends on ssl-certificate")
                required_resources.append(("ssl-certificate", resource.domain.sslCertificate))
            elif self._res_type == "service":
                req_r_type, service_name = [w.lower() for w in resource.serviceTemplate.serviceType.name.split("_")][:2]
                if service_name == "nginx":
                    LOGGER.debug("{0} service depends on application servers".format(resource.name))
                    required_resources.extend([("service", s) for s in CONFIG.localserver.services
                                               if s.serviceTemplate.serviceType.name.startswith("WEBSITE_")])
                else:
                    LOGGER.debug("{0} service depends on {1}".format(resource.name, req_r_type))
                    with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                        required_resources.extend([(req_r_type, r) for r in
                                                   api.resource(req_r_type).filter(serviceId=resource.id).get() or []])
        return required_resources

    def get_affected_resources(self, resource=None):
        resources = [resource] if resource else self._resources
        affected_resources = list()
        for resource in resources:
            with taskexecutor.httpsclient.ApiClient(**CONFIG.apigw) as api:
                if self._res_type == "database-user":
                    LOGGER.debug("database-user affects database")
                    affected_resources.extend([("database", db) for db in
                                               api.Database().filter(databaseUserId=resource.id).get()])
                elif self._res_type == "ssl-certificate":
                    LOGGER.debug("ssl-certificate affects website and redirect")
                    domain = api.Domain().find(sslCertificateId=resource.id).get()
                    website = api.Website().find(domainId=domain.id).get()
                    redirect = api.Redirect().find(domainId=domain.id).get()
                    if website:
                        affected_resources.append(("website", website))
                    if redirect:
                        affected_resources.append(("redirect", redirect))
                elif self._res_type == "service" and resource.serviceTemplate.serviceType.name.startswith("WEBSITE_"):
                    nginx = next((s for s in CONFIG.localserver.services
                                  if s.serviceTemplate.serviceType.name == "STAFF_NGINX"), None)
                    if nginx:
                        affected_resources.append(("service", nginx))
        return affected_resources


class Executor:
    __new_task_queue = queue.Queue()
    __failed_tasks = dict()
    pool_dump_template = "/var/cache/te/{}.pkl"

    def __init__(self):
        self._stopping = False
        self._shutdown_wait = False
        self._command_task_pool = taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.command)
        self._command_task_pool.name = "command_task_pool"
        self._long_command_task_pool = taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.command // 2)
        self._long_command_task_pool.name = "long_command_task_pool"
        self._query_task_pool = taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.query)
        self._query_task_pool.name = "query_task_pool"
        self._backup_files_task_pool = taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.backup.files)
        self._backup_files_task_pool.name = "backup_files_task_pool"
        self._backup_dbs_task_pool = taskexecutor.utils.ThreadPoolExecutorStackTraced(CONFIG.max_workers.backup.dbs)
        self._backup_dbs_task_pool.name = "backup_dbs_task_pool"
        self._future_to_task_map = dict()

    @classmethod
    def get_new_task_queue(cls):
        return cls.__new_task_queue

    @classmethod
    def get_failed_tasks(cls):
        return cls.__failed_tasks.values()

    @classmethod
    def _get_task_failcount(cls, task):
        if task.actid in cls.__failed_tasks.keys():
            return cls.__failed_tasks[task.actid].get("failcount") or 0
        return 0

    @classmethod
    def _remember_failed_task(cls, task):
        failcount = cls._get_task_failcount(task) + 1
        cls.__failed_tasks[task.actid] = {"task": task, "failcount": failcount}

    @classmethod
    def _forget_failed_task(cls, task):
        del cls.__failed_tasks[task.actid]

    def select_pool(self, task):
        return {True: self._query_task_pool,
                task.action in ("create", "update", "delete"): self._command_task_pool,
                "oldServerName" in task.params.keys(): self._long_command_task_pool,
                task.action == "backup": self._backup_files_task_pool,
                task.action == "backup" and task.res_type == "database": self._backup_dbs_task_pool}[True]

    def select_reporter(self, task):
        if task.origin.__name__ == "AMQPListener" and task.action in ("create", "update", "delete"):
            return taskexecutor.constructor.get_reporter("amqp")
        elif task.action == "backup":
            return taskexecutor.constructor.get_reporter("alerta")
        elif task.action in ("quota_report", "malware_report"):
            return taskexecutor.constructor.get_reporter("https")
        else:
            return taskexecutor.constructor.get_reporter("null")

    def select_reported_properties(self, task):
        return {"quota_report": ["quotaUsed"],
                "malware_report": ["infectedFiles"]}[task.action]

    def create_subtasks(self, task, resources):
        subtasks = list()
        last_idx = len(resources) - 1
        for idx, resource in enumerate(resources):
            params = copy.copy(task.params)
            params.update({"resource": resource})
            suffix = resource.name
            tag = task.tag if idx == last_idx else "{0}.{1}".format(task.tag, suffix)
            if (hasattr(resource, "quota") and resource.quota == 0) or not resource.switchedOn:
                continue
            if hasattr(resource, "domain"):
                suffix = "{0}@{1}".format(suffix, resource.domain.name)
            subtasks.append(taskexecutor.task.Task(tag=tag,
                                                   origin=task.origin,
                                                   opid=task.opid,
                                                   actid="{}.{}".format(task.actid, suffix),
                                                   res_type=task.res_type,
                                                   action=task.action,
                                                   params=params))
            del params
        return subtasks

    def spawn_subtask(self, task):
        in_queue = self.get_new_task_queue()
        in_queue.put(task)

    def build_processing_sequence(self, res_type, resource, action, params):
        sequence = list()
        if not params.get("isolated"):
            res_builder = ResourceBuilder(res_type)
            for req_r_type, req_resource in res_builder.get_required_resources(resource):
                req_r_params = {"required_for": (res_type, resource)}
                req_r_params.update(params.get("paramsForRequiredResources", {}))
                sequence.extend(self.build_processing_sequence(req_r_type, req_resource, "update", req_r_params))
        processor = taskexecutor.constructor.get_resprocessor(res_type, resource, params)
        sequence.append((processor, getattr(processor, action)))
        if not params.get("isolated"):
            causer_resource = resource if "required_for" not in params.keys() else params["required_for"][1]
            for aff_r_type, aff_resource in [(t, r) for t, r in res_builder.get_affected_resources(resource)
                                             if r.id != causer_resource.id]:
                aff_r_params = {"caused_by": (res_type, resource)}
                aff_r_params.update(params.get("paramsForAffectedResources", {}))
                processor = taskexecutor.constructor.get_resprocessor(aff_r_type, aff_resource, params=aff_r_params)
                sequence.append((processor, getattr(processor, "update")))
            sequence_mapping = collections.OrderedDict()
            for processor, method in sequence:
                k = "{}{}".format(processor.resource.id, method.__name__)
                sequence_mapping[k] = (processor, method)
            sequence = list(sequence_mapping.values())
        return sequence

    def process_task(self, task):
        task.params["started"] = datetime.datetime.now().isoformat()
        taskexecutor.utils.set_thread_name("OPERATION IDENTITY: {0.opid} ACTION IDENTITY: {0.actid}".format(task))
        if task.params.get("failcount"):
            if task.params["failcount"] >= CONFIG.task.max_retries:
                LOGGER.warning("Currently processed task had failed {0} times before, "
                               "giving up".format(task.params["failcount"]))
                self.finish_task(task, taskexecutor.task.FAILED)
                return
            delay = task.params["failcount"] if task.params["failcount"] < 60 else 60
            LOGGER.warning("Currently processed task had failed {0} times before, "
                           "sleeping for {1}s".format(task.params["failcount"], delay))
            time.sleep(delay)
        if not task.params.get("resource"):
            res_builder = ResourceBuilder(task.res_type, task.params.get("objRef"))
            if len(res_builder.resources) == 0:
                LOGGER.info("There is no {} resources here".format(task.res_type))
                return
            if len(res_builder.resources) > 1:
                for subtask in self.create_subtasks(task, res_builder.resources):
                    if task.params.get("exec_type") == "parallel":
                        self.spawn_subtask(subtask)
                    else:
                        self.process_task(subtask)
                task.tag = None
                return
            else:
                task.params["resource"] = res_builder.resources[0]
        if task.action in ("create", "update", "delete"):
            sequence = self.build_processing_sequence(task.res_type, task.params["resource"], task.action, task.params)
            for processor, method in sequence:
                LOGGER.info("Calling {0} {1}".format(method, processor))
                method()
                if processor.extra_services and hasattr(processor.extra_services, "http_proxy"):
                    task.params["httpProxyIp"] = processor.extra_services.http_proxy.socket.http.address
        elif task.action == "backup":
            backuper = taskexecutor.constructor.get_backuper(task.res_type, task.params["resource"])
            backuper.backup()
        else:
            collector = taskexecutor.constructor.get_rescollector(task.res_type, task.params["resource"])
            task.params["data"] = dict()
            ttl = task.params.get("interval") or 1
            ttl -= 1
            for property in self.select_reported_properties(task):
                task.params["data"][property] = collector.get_property(property, cache_ttl=ttl)
        self.finish_task(task, taskexecutor.task.DONE)

    def finish_task(self, task, report_state):
        resource = task.params.get("resource")
        task.state = report_state
        reporter = self.select_reporter(task)
        report = reporter.create_report(task)
        if task.action == "malware_report":
            infected_sign = int(bool(report.get("infectedFiles") or resource.infected)) * 2 - 1
            taskexecutor.watchdog.ProcessWatchdog.get_uids_queue().put(resource.uid * infected_sign)
        if report and not any(report.values()):
            LOGGER.debug("Discarding empty report: {}".format(report))
        else:
            LOGGER.info("Sending report {0} using {1}".format(report, type(reporter).__name__))
            reporter.send_report()
        task.state = taskexecutor.task.DONE
        LOGGER.info("Done with task {}".format(task))

    def run(self):
        taskexecutor.utils.set_thread_name("Executor")
        in_queue = self.get_new_task_queue()
        for pool in (self._command_task_pool, self._long_command_task_pool,
                     self._query_task_pool, self._backup_files_task_pool, self._backup_dbs_task_pool):
            filename = self.pool_dump_template.format(pool.name)
            if os.path.exists(filename):
                LOGGER.info("Restoring {} tasks from disk".format(pool.name))
                with open(filename, "wb") as f:
                    for fn, args, kwargs in pickle.load(f):
                        LOGGER.debug("Submitting to {} {} "
                                     "with args {} and keyword args {}".format(pool.name, fn, args, kwargs))
                        pool.submit(fn, *args, **kwargs)
                os.unlink(filename)

        while not self._stopping:
            try:
                task = in_queue.get(timeout=.2)
                pool = self.select_pool(task)
                task.params["failcount"] = self._get_task_failcount(task)
                task.state = taskexecutor.task.PROCESSING
                future = pool.submit(self.process_task, task)
                self._future_to_task_map[future] = task
                LOGGER.debug("task processing submitted to pool, max workers: {0}, "
                             "current queue size: {1}".format(pool._max_workers, pool._work_queue.qsize()))
            except queue.Empty:
                future_to_task_map = copy.copy(self._future_to_task_map)
                for future, task in future_to_task_map.items():
                    if future.done():
                        exc  = future.exception()
                        if exc:
                            task.state = taskexecutor.task.FAILED
                            task.params['last_exception'] = str(exc)
                            self._remember_failed_task(task)
                        elif self._get_task_failcount(task) > 0:
                            self._forget_failed_task(task)
                        if task.tag:
                            out_queue = task.origin.get_processed_task_queue()
                            out_queue.put(task)
                        del self._future_to_task_map[future]
                del future_to_task_map
        LOGGER.info("Shutting all pools down {}"
                    "waiting for workers".format({True: "", False: "not "}[self._shutdown_wait]))
        def filter(i):
            LOGGER.debug("fn: {0.fn} args: {0.args} kwargs: {0.kwargs}".format(i))
            return i.args[0].origin is not taskexecutor.listener.AMQPListener
        for pool in (self._command_task_pool, self._long_command_task_pool,
                     self._query_task_pool, self._backup_files_task_pool, self._backup_dbs_task_pool):
            q = list(pool.dump_work_queue(filter))
            if q:
                filename = self.pool_dump_template.format(pool.name)
                LOGGER.info("Dumping {0} tasks from {1} to disk: {2}".format(len(q), pool.name, filename))
                with open(filename, "wb") as f:
                    pickle.dump(q, f)
            pool.shutdown(wait=self._shutdown_wait)

    def stop(self, wait=False):
        self._shutdown_wait = wait
        self._stopping = True
