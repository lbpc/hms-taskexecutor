import abc
import datetime
import queue
import time
from itertools import product

import schedule
from kombu import Connection, Exchange, Queue
from kombu.mixins import ConsumerMixin

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.task import Task, TaskState
from taskexecutor.utils import set_thread_name, to_lower_dashed, asdict, rgetattr

__all__ = ['AMQPListener', 'TimeListener']


class ContextValidationError(Exception):
    pass


class Listener(metaclass=abc.ABCMeta):
    __processed_task_queue = None

    def __init__(self, new_task_queue):
        self._new_task_queue = new_task_queue

    @classmethod
    @abc.abstractmethod
    def get_processed_task_queue(cls):
        pass

    @abc.abstractmethod
    def listen(self):
        pass

    @abc.abstractmethod
    def take_event(self, message, context):
        pass

    @abc.abstractmethod
    def stop(self):
        pass


class AMQPListener(Listener, ConsumerMixin):
    __processed_task_queue = queue.Queue()

    def __init__(self, new_task_queue):
        super().__init__(new_task_queue)
        self.connect_max_retries = CONFIG.amqp.connection_attempts
        self.should_stop = False
        self.connection = None
        self._messages = {}

    @classmethod
    def get_processed_task_queue(cls):
        return cls.__processed_task_queue

    def _register_message(self, body, message):
        self._messages[message.delivery_tag] = message

    def on_iteration(self):
        internal_queue = self.get_processed_task_queue()
        while internal_queue.qsize() > 0:
            task = internal_queue.get_nowait()
            if task.tag in self._messages:
                msg = self._messages.pop(task.tag)
                if task.state is TaskState.DONE:
                    msg.ack()
                elif task.state is TaskState.FAILED:
                    msg.requeue()
                else:
                    LOGGER.warning(f"Task with unexpected state found in 'processed' queue: {task}")
            else:
                LOGGER.warning(f"Task with unseen tag found in 'processed' queue: {task}")

    def get_consumers(self, Consumer, channel):
        identity = f'te.{CONFIG.hostname}'
        exc_type = CONFIG.amqp.exchange_type
        rk = rgetattr(CONFIG, 'amqp.consumer_routing_key', identity)
        queues = [Queue(name=f'{identity}.{exc}', exchange=Exchange(exc, type=exc_type), routing_key=rk)
                  for exc in ('.'.join(p) for p in product(CONFIG.enabled_resources, ('create', 'update', 'delete')))]
        return [Consumer(queues=queues, callbacks=[self.take_event, self._register_message])]

    def listen(self):
        set_thread_name('AMQPListener')
        url = 'amqp://{0.user}:{0.password}@{0.host}:{0.port}//'.format(CONFIG.amqp)
        transport_options = {'client_properties': {'connection_name': f'taskexecutor@{CONFIG.hostname}'}}
        with Connection(url, heartbeat=CONFIG.amqp.heartbeat_interval, transport_options=transport_options) as conn:
            self.connection = conn
            self.run()

    def take_event(self, message, context):
        res_type, action = context.delivery_info.get('exchange', '.').split('.')
        message['params']['objRef'] = message.pop('objRef')
        message['params']['provider'] = context.headers['provider']
        set_thread_name('OPERATION IDENTITY: {} '
                        'ACTION IDENTITY: {}'.format(message['operationIdentity'], message['actionIdentity']))
        task = Task(tag=context.delivery_tag,
                    origin=self.__class__,
                    opid=message["operationIdentity"],
                    actid=message["actionIdentity"],
                    res_type=res_type,
                    action=action,
                    params=message["params"])
        self._new_task_queue.put(task)
        LOGGER.debug(f'New task created: {task}')
        set_thread_name('AMQPListener')

    def stop(self):
        self.should_stop = True


class TimeListener(Listener):
    __processed_task_queue = queue.Queue()

    def __init__(self, new_task_queue):
        super().__init__(new_task_queue)
        self._stopping = False
        self._futures = dict()

    @classmethod
    def get_processed_task_queue(cls):
        return cls.__processed_task_queue

    def _schedule(self):
        for action, res_types in asdict(CONFIG.schedule).items():
            for res_type, params in asdict(res_types).items():
                res_type = to_lower_dashed(res_type)
                if res_type in CONFIG.enabled_resources:
                    context = {'res_type': res_type, 'action': action}
                    message = {'params': asdict(params)}
                    if hasattr(params, 'daily') and params.daily:
                        if not hasattr(params, 'at'):
                            LOGGER.warning(f"Invalid schedule definition for {res_types}: "
                                           f"{params}, 'at' time needs to be specified")
                            continue
                        job = schedule.every().day.at(params.at).do(self.take_event, message, context)
                    else:
                        if not hasattr(params, 'interval'):
                            LOGGER.warning(f'Invalid schedule definition for {res_type}: {params}, '
                                           'interval needs to be specified')
                            continue
                        job = schedule.every(params.interval).seconds.do(self.take_event, message, context)
                    LOGGER.debug(job)

    def listen(self):
        set_thread_name('TimeListener')
        self._schedule()
        while not self._stopping:
            schedule.run_pending()
            queue = self.get_processed_task_queue()
            while queue.qsize() > 0:
                task = queue.get_nowait()
                if task.state is TaskState.FAILED:
                    LOGGER.warning(f'Got failed scheduled task: {task}')
                    self._new_task_queue.put(task)
            sleep_interval = abs(schedule.idle_seconds()) if schedule.jobs else 10
            if not self._stopping:
                time.sleep(sleep_interval if sleep_interval < 1 else 1)

    def take_event(self, message, context):
        action_id = '{}.{}'.format(context['res_type'], context['action'])
        task = Task(tag='{}.{}'.format(datetime.datetime.now().isoformat(), action_id),
                    origin=self.__class__,
                    opid='LOCAL-SCHED',
                    actid=action_id,
                    res_type=context['res_type'],
                    action=context['action'],
                    params=message['params'])
        self._new_task_queue.put(task)
        LOGGER.debug(f'New task created from locally scheduled event: {task}')

    def stop(self):
        schedule.clear()
        self._stopping = True
