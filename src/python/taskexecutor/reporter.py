import abc
import json
import re

import alertaclient.api as alerta
from kombu import Connection, Exchange, Queue

from taskexecutor.config import CONFIG
from taskexecutor.httpsclient import ApiClient
from taskexecutor.logger import LOGGER
from taskexecutor.task import TaskState
from taskexecutor.utils import asdict, asdict_rec, to_camel_case, to_lower_dashed

__all__ = ['AMQPReporter', 'HttpsReporter', 'AlertaReporter', 'NullReporter']


class Reporter(metaclass=abc.ABCMeta):
    def __init__(self):
        self._report = dict()

    @abc.abstractmethod
    def create_report(self, task):
        pass

    @abc.abstractmethod
    def send_report(self):
        pass


class AMQPReporter(Reporter):
    def __init__(self):
        super().__init__()
        self._task = None
        self.next_te = None

    @property
    def _report_to_next_te(self):
        return self.next_te and self._task.res_type == 'website'

    @staticmethod
    def humanize_error(text, class_name, user_defined_cmds):
        if CONFIG.profile == 'dev': return text
        if class_name == 'ContainerCommandExecutionError' and user_defined_cmds: return text
        if class_name == 'ConatinerCommandTimedOut':
            return f'Выполнение команды прервано после истечения тайм-аута: {text}'
        if class_name == 'CommandExecutionError':
            rsync_notfound = re.findall(r'^STDERR: rsync: change_dir '
                                        r'"/slice/.+/.+/ids/[a-z0-9]+(/.+)"'
                                        r'.+failed: No such file or directory', text, flags=re.MULTILINE)
            if rsync_notfound:
                return f'Путь {rsync_notfound[0]} не найден в архиве'
        return 'Внутренняя ошибка сервера'

    def create_report(self, task):
        self._task = task
        params = task.params
        if 'ovsId' not in params: LOGGER.error('No ovsId in params')
        if 'success' in params: del params['success']
        self._report['operationIdentity'] = task.opid
        self._report['actionIdentity'] = task.actid
        self._report['objRef'] = params['objRef']
        self.next_te = params.pop('oldServerName', None)
        self._report['params'] = {
            'success': bool(task.state ^ TaskState.FAILED),
            'ovsId': params.get('ovsId')
        }
        if 'last_exception' in params:
            err_message = params['last_exception'].get('message')
            err_class = params['last_exception'].get('class')
            user_defined_cmds = {k: v for k, v in params.items() if v in ('appUpdateCommands', 'appInstallCommands')}
            self._report['params']['errorMessage'] = self.humanize_error(err_message, err_class, user_defined_cmds)
            self._report['params']['exceptionClass'] = params['last_exception'].get('class')
        LOGGER.debug(f'Report to next TE: {self._report_to_next_te}')
        if self._report_to_next_te:
            for k in ('resource', 'dataPostprocessorType', 'dataPostprocessorArgs', 'app_server'):
                if k in params: del params[k]
            params['paramsForRequiredResources'] = {'forceSwitchOff': True}
            if 'httpProxyIp' in params: params['newHttpProxyIp'] = params['httpProxyIp']
            self._report['params'] = params
        self._report = asdict_rec(self._report)
        return self._report

    def send_report(self):
        url = 'amqp://{0.user}:{0.password}@{0.host}:{0.port}//'.format(CONFIG.amqp)
        exchange = '{0.res_type}.{0.action}'.format(self._task)
        routing_key = (self._task.params['provider'].replace('-', '.')
                       if not self._report_to_next_te
                       else 'te.{}'.format(self.next_te))
        provider = self._task.params['provider'] if self._report_to_next_te else 'te'
        LOGGER.info(f'Publishing to {exchange} exchange with {routing_key} routing key, '
                    f'headers: provider={provider}, payload: {self._report}')
        queue = Queue(name=f'te.{CONFIG.hostname}.{exchange}.report',
                      auto_delete=True,
                      expires=3,
                      exchange=Exchange(exchange, type='topic'),
                      routing_key=routing_key)
        with Connection(url, heartbeat=CONFIG.amqp.heartbeat_interval) as conn:
            producer = conn.Producer()
            producer.publish(json.dumps(self._report),
                             content_type='application/json',
                             retry=True,
                             exchange=queue.exchange,
                             routing_key=queue.routing_key,
                             headers={'provider': provider},
                             declare=[queue])


class HttpsReporter(Reporter):
    def __init__(self):
        super().__init__()
        self._task = None
        self._resource = None

    def create_report(self, task):
        self._task = task
        self._resource = task.params.get('resource')
        if self._resource: self._report = task.params.get('data')
        return self._report

    def send_report(self):
        if not self._resource:
            LOGGER.warning('Attepmted to send report without resource: {0._report}, task: {0._task}'.format(self))
            return
        with ApiClient(**CONFIG.apigw) as api:
            Resource = getattr(api, to_camel_case(self._task.res_type))
            endpoint = '{}/{}'.format(self._resource.id, to_lower_dashed(self._task.action))
            Resource(endpoint).post(json.dumps(self._report))


class AlertaReporter(Reporter):
    def __init__(self):
        super().__init__()
        self._alerta = alerta.Client(**asdict(CONFIG.alerta))

    def create_report(self, task):
        success = bool(task.state ^ TaskState.FAILED)
        attributes = dict(publicParams=[],
                          tag=task.tag,
                          origin=str(task.origin),
                          opid=task.opid,
                          actid=task.actid,
                          res_type=task.res_type,
                          action=task.action,
                          hostname=CONFIG.hostname)
        try:
            resource = task.params.pop('resource')
            task.params['hmsResource'] = asdict(resource)
        except KeyError:
            pass
        attributes.update(task.params)
        self._report = dict(environment='HMS',
                            service=['taskexecutor'],
                            resource=task.actid,
                            event='task.finished',
                            value={True: 'Ok', False: 'Failed'}[success],
                            text='Done' if success else task.params.get('last_exception', 'Failed'),
                            severity={True: 'Ok', False: 'Minor'}[success],
                            hostname=CONFIG.hostname,
                            attributes=asdict_rec(attributes))
        return self._report

    def send_report(self):
        try:
            self._alerta.send_alert(**self._report)
        except Exception as e:
            LOGGER.error(f'Failed to send report to Alerta: {e}')


class NullReporter(Reporter):
    def create_report(self, task):
        return

    def send_report(self):
        pass
