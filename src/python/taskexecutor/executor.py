import collections
import copy
import datetime
import os
import pickle
import queue
import time
import urllib.parse

import taskexecutor.constructor as cnstr
from taskexecutor.config import CONFIG
from taskexecutor.httpsclient import ApiClient
from taskexecutor.listener import AMQPListener
from taskexecutor.logger import LOGGER
from taskexecutor.task import Task, TaskState
from taskexecutor.utils import set_thread_name, to_camel_case, to_lower_dashed, ThreadPoolExecutorStackTraced, to_namedtuple
from taskexecutor.watchdog import ProcessWatchdog

__all__ = ['Executor']


class ResourceBuildingError(Exception):
    pass


class PropertyValidationError(Exception):
    pass


class UnknownTaskAction(Exception):
    pass


class ResourceBuilder:
    def __init__(self, res_type, resource=None, obj_ref=None):
        self._res_type = res_type
        self._resource = resource
        self._obj_ref = obj_ref
        self._resources = []

    @property
    def resources(self):
        if not self._resources:
            with ApiClient(**CONFIG.apigw) as api:
                if self._resource:
                    self._resources.append(to_namedtuple(self._resource))
                elif self._obj_ref:
                    self._resources.append(api.get(urllib.parse.urlparse(self._obj_ref).path))
                elif self._res_type in ('unix-account', 'mailbox'):
                    self._resources.extend(api.resource(self._res_type).filter(serverId=CONFIG.localserver.id).get())
                elif self._res_type == 'service':
                    self._resources.extend(cnstr.get_services())
                else:
                    for each in cnstr.get_services_by_res_type(to_camel_case(self._res_type).upper()):
                        self._resources.extend(api.resource(self._res_type).filter(serviceId=each.id).get())
        return self._resources

    def get_required_resources(self, resource=None):
        resources = [resource] if resource else self._resources
        required_resources = list()
        for resource in resources:
            if self._res_type == 'website':
                LOGGER.debug('website depends on unix-account and ssl-certificate')
                required_resources.append(('unix-account', resource.unixAccount))
                required_resources.extend([('ssl-certificate', d.sslCertificate) for d in
                                           resource.domains if d.sslCertificate])
            elif self._res_type == 'redirect' and resource.domain.sslCertificate:
                LOGGER.debug('redirect depends on ssl-certificate')
                required_resources.append(('ssl-certificate', resource.domain.sslCertificate))
            elif self._res_type == 'service':
                req_r_type = resource.template.resourceType.lower()
                with ApiClient(**CONFIG.apigw) as api:
                    if resource.template.__class__.__name__ == 'HttpServer':
                        LOGGER.debug(f'{resource.name} service depends on application servers')
                        for each in cnstr.get_services_by_template_type('ApplicationServer'):
                            required_resources.append(('service', each))
                    else:
                        LOGGER.debug(f'{resource.name} service depends on {req_r_type}')
                        required_resources.extend([(req_r_type, r) for r in
                                                   api.resource(req_r_type).filter(serviceId=resource.id).get() or []])
            elif self._res_type == 'database':
                LOGGER.debug('database depends on database-user')
                required_resources.extend([('database-user', u) for u in resource.databaseUsers])
        return [r for r in required_resources if r[1].switchedOn]

    def get_affected_resources(self, resource=None):
        resources = [resource] if resource else self._resources
        affected_resources = list()
        for resource in resources:
            with ApiClient(**CONFIG.apigw) as api:
                if self._res_type == 'database-user':
                    LOGGER.debug('database-user affects database')
                    affected_resources.extend([('database', db) for db in
                                               api.Database().filter(databaseUserId=resource.id).get()])
                elif self._res_type == 'ssl-certificate':
                    LOGGER.debug('ssl-certificate affects website and redirect')
                    domain = api.Domain().find(sslCertificateId=resource.id).get()
                    website = api.Website().find(domainId=domain.id).get()
                    redirect = api.Redirect().find(domainId=domain.id).get()
                    if website:
                        affected_resources.append(('website', website))
                    if redirect:
                        affected_resources.append(('redirect', redirect))
                elif self._res_type == 'service' and resource.template.resourceType == 'WEBSITE':
                    http_proxy = cnstr.get_http_proxy_service()
                    if http_proxy:
                        affected_resources.append(('service', http_proxy.spec))
        return [r for r in affected_resources if r[1].switchedOn]


class Executor:
    __new_task_queue = queue.Queue()
    __failed_tasks = dict()
    pool_dump_template = '{}/{{}}.pkl'.format(getattr(CONFIG, 'executor.task_dump_dir', '/var/cache/te'))

    def __init__(self):
        self._stopping = False
        self._shutdown_wait = False
        self._command_task_pool = ThreadPoolExecutorStackTraced(CONFIG.max_workers.command)
        self._command_task_pool.name = 'command_task_pool'
        self._long_command_task_pool = ThreadPoolExecutorStackTraced(CONFIG.max_workers.command // 2)
        self._long_command_task_pool.name = 'long_command_task_pool'
        self._query_task_pool = ThreadPoolExecutorStackTraced(CONFIG.max_workers.query)
        self._query_task_pool.name = 'query_task_pool'
        self._backup_files_task_pool = ThreadPoolExecutorStackTraced(CONFIG.max_workers.backup.files)
        self._backup_files_task_pool.name = 'backup_files_task_pool'
        self._backup_dbs_task_pool = ThreadPoolExecutorStackTraced(CONFIG.max_workers.backup.dbs)
        self._backup_dbs_task_pool.name = 'backup_dbs_task_pool'
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
            return cls.__failed_tasks[task.actid].get('failcount', 0)
        return 0

    @classmethod
    def _save_failed_task(cls, task):
        failcount = cls._get_task_failcount(task) + 1
        cls.__failed_tasks[task.actid] = {'task': task, 'failcount': failcount}

    @classmethod
    def _load_failed_task(cls, action_identity):
        if action_identity in cls.__failed_tasks.keys():
            return cls.__failed_tasks[action_identity].get('task')

    @classmethod
    def _forget_failed_task(cls, task):
        del cls.__failed_tasks[task.actid]

    def select_pool(self, task):
        return {True: self._query_task_pool,
                task.action in ('create', 'update', 'delete'): self._command_task_pool,
                'oldServerName' in task.params.keys(): self._long_command_task_pool,
                task.action == 'backup': self._backup_files_task_pool,
                task.action == 'backup' and task.res_type == 'database': self._backup_dbs_task_pool}[True]

    def select_reporter(self, task):
        if task.origin.__name__ == 'AMQPListener' and task.action in ('create', 'update', 'delete'):
            return cnstr.get_reporter('amqp')
        elif task.action == 'backup':
            return cnstr.get_reporter('alerta')
        elif task.action in ('quota_report', 'malware_report'):
            return cnstr.get_reporter('https')
        else:
            return cnstr.get_reporter('null')

    def select_reported_properties(self, task):
        return {'quota_report': ['quotaUsed'],
                'malware_report': ['infectedFiles']}[task.action]

    def create_subtasks(self, task, resources):
        subtasks = list()
        last_idx = len(resources) - 1
        for idx, resource in enumerate(resources):
            params = copy.copy(task.params)
            params.update({'resource': resource})
            suffix = resource.name
            tag = task.tag if idx == last_idx else f'{task.tag}.{suffix}'
            if (hasattr(resource, 'quota') and resource.quota == 0) or not resource.switchedOn:
                continue
            if hasattr(resource, 'domain'):
                suffix = f'{suffix}@{resource.domain.name}'
            subtasks.append(Task(tag=tag,
                                 origin=task.origin,
                                 opid=task.opid,
                                 actid=f'{task.actid}.{suffix}',
                                 res_type=task.res_type,
                                 action=task.action,
                                 params=params))
            del params
        return subtasks

    def spawn_subtask(self, task):
        in_queue = self.get_new_task_queue()
        in_queue.put(task)

    def build_processing_sequence(self, res_type, resource, action, params):
        sequence = []
        processor = cnstr.get_resprocessor(res_type, resource, params)
        res_builder = ResourceBuilder(res_type)
        required_resources = res_builder.get_required_resources(resource) + [
            (to_lower_dashed(e.get('@type')), {k: v for k, v in e.items() if k != '@type'})
             for e in params.get('ovs', {}).get('requiredResources', [])
        ]
        if not params.get('isolated'):
            for req_r_type, req_resource in required_resources:
                req_r_params = {'required_for': (res_type, resource)}
                req_r_params.update(params.get('paramsForRequiredResources', {}))
                sequence.extend(self.build_processing_sequence(req_r_type, req_resource, 'update', req_r_params))
        sequence.append((processor, getattr(processor, action)))
        if not params.get('isolated'):
            causer_resource = resource if 'required_for' not in params.keys() else params['required_for'][1]
            affected_resources = res_builder.get_affected_resources(resource) + [
                (to_lower_dashed(e.get('@type')), {k: v for k, v in e.items() if k != '@type'})
                 for e in params.get('ovs', {}).get('affectedResources', [])
            ]
            for aff_r_type, aff_resource in [(t, r) for t, r in affected_resources if r.id != causer_resource.id]:
                aff_r_params = {'caused_by': (res_type, resource)}
                aff_r_params.update(params.get('paramsForAffectedResources', {}))
                processor = cnstr.get_resprocessor(aff_r_type, aff_resource, params=aff_r_params)
                sequence.append((processor, getattr(processor, 'update')))
            sequence_mapping = collections.OrderedDict()
            for processor, method in sequence:
                k = processor.resource.id + method.__name__
                sequence_mapping[k] = (processor, method)
            sequence = list(sequence_mapping.values())
        return sequence

    def process_task(self, task):
        task.params['started'] = datetime.datetime.now().isoformat()
        set_thread_name('OPERATION IDENTITY: {0.opid} ACTION IDENTITY: {0.actid}'.format(task))
        failcount = task.params.get('failcount', 0)
        if failcount - 1 >= task.params.get('maxRetries', CONFIG.task.max_retries):
            LOGGER.warning(f'Currently processed task had failed {failcount} times before, giving up')
            self.finish_task(task, TaskState.FAILED)
            return
        elif failcount > 0:
            delay = failcount if failcount < 60 else 60
            LOGGER.warning(f'Currently processed task had failed {failcount} times before, sleeping for {delay}s')
            time.sleep(delay)
        if not task.params.get('resource'):
            res_builder = ResourceBuilder(task.res_type,
                                          task.params.get('ovs', {}).get('resource'),
                                          task.params.get('objRef'))
            if len(res_builder.resources) == 0:
                LOGGER.info(f'There is no {task.res_type} resources here')
                return
            if len(res_builder.resources) > 1:
                for subtask in self.create_subtasks(task, res_builder.resources):
                    if task.params.get('exec_type') == 'parallel':
                        self.spawn_subtask(subtask)
                    else:
                        self.process_task(subtask)
                task.tag = None
                return
            else:
                task.params['resource'] = res_builder.resources[0]
        if task.action in ('create', 'update', 'delete'):
            sequence = self.build_processing_sequence(task.res_type, task.params['resource'], task.action, task.params)
            for processor, method in sequence:
                LOGGER.debug(f'Calling {method}')
                method()
                if processor.extra_services and hasattr(processor.extra_services, 'http_proxy'):
                    task.params['httpProxyIp'] = processor.extra_services.http_proxy.socket.http.address
        elif task.action == 'backup':
            backuper = cnstr.get_backuper(task.res_type, task.params['resource'])
            backuper.backup()
        else:
            collector = cnstr.get_rescollector(task.res_type, task.params['resource'])
            task.params['data'] = dict()
            ttl = task.params.get('interval') or 1
            ttl -= 1
            for property in self.select_reported_properties(task):
                task.params['data'][property] = collector.get_property(property, cache_ttl=ttl)
        self.finish_task(task, TaskState.DONE)

    def finish_task(self, task, report_state):
        resource = task.params.get('resource')
        task.state = report_state
        reporter = self.select_reporter(task)
        report = reporter.create_report(task)
        if task.action == 'malware_report' and resource:
            infected_sign = int(bool(report.get('infectedFiles') or resource.infected)) * 2 - 1
            ProcessWatchdog.get_uids_queue().put(resource.uid * infected_sign)
        if report and not any(report.values()):
            LOGGER.debug(f'Discarding empty report: {report}')
        else:
            LOGGER.info(f'Sending report {report} using {reporter.__class__.__name__}')
            reporter.send_report()
        task.state = TaskState.DONE
        LOGGER.info(f'Done with task {task}')

    def run(self):
        set_thread_name('Executor')
        in_queue = self.get_new_task_queue()
        for pool in (self._command_task_pool, self._long_command_task_pool,
                     self._query_task_pool, self._backup_files_task_pool, self._backup_dbs_task_pool):
            filename = self.pool_dump_template.format(pool.name)
            if os.path.exists(filename):
                LOGGER.info(f'Restoring {pool.name} tasks from disk')
                try:
                    with open(filename, 'rb') as f:
                        for task in pickle.load(f):
                            in_queue.put(task)
                            LOGGER.info(f'Task restored: {task}')
                except Exception as e:
                    LOGGER.error(f'Failed to restore tasks from {filename}: {e}')
                os.unlink(filename)

        while not self._stopping:
            try:
                task = in_queue.get(timeout=.2)
                pool = self.select_pool(task)
                task.params = {**task.params, **getattr(self._load_failed_task(task.actid), 'params', {})}
                task.params['failcount'] = self._get_task_failcount(task)
                task.state = TaskState.PROCESSING
                future = pool.submit(self.process_task, task)
                self._future_to_task_map[future] = task
                LOGGER.debug('Task processing submitted to pool, max workers: {0}, '
                             'current queue size: {1}'.format(pool._max_workers, pool._work_queue.qsize()))
            except queue.Empty:
                future_to_task_map = copy.copy(self._future_to_task_map)
                for future, task in future_to_task_map.items():
                    if future.done():
                        exc = future.exception()
                        if exc:
                            task.state = TaskState.FAILED
                            task.params['last_exception'] = {'message': str(exc), 'class': exc.__class__.__name__}
                            self._save_failed_task(task)
                        elif self._get_task_failcount(task) > 0:
                            self._forget_failed_task(task)
                        if task.tag:
                            out_queue = task.origin.get_processed_task_queue()
                            out_queue.put(task)
                        del self._future_to_task_map[future]
                del future_to_task_map
        LOGGER.info('Shutting all pools down {}'
                    'waiting for workers'.format({True: '', False: 'not '}[self._shutdown_wait]))
        for pool in (self._command_task_pool, self._long_command_task_pool,
                     self._query_task_pool, self._backup_files_task_pool, self._backup_dbs_task_pool):
            tasks = [pair[1] for pair in pool.dump_work_queue(lambda i: i[1].origin is not AMQPListener)]
            if tasks:
                filename = self.pool_dump_template.format(pool.name)
                LOGGER.info(f'Dumping {len(tasks)} tasks from {pool.name} to disk: {filename}')
                with open(filename, 'wb') as f:
                    pickle.dump(tasks, f)
            pool.shutdown(wait=self._shutdown_wait)
            LOGGER.debug(f'{pool} is shut down')

    def stop(self, wait=False):
        self._shutdown_wait = wait
        self._stopping = True
