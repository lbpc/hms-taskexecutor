import functools
import json
import abc
import itertools
import pika
import pika.exceptions
import time
import schedule
import queue
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.task
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


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
    def take_event(self, context, message):
        pass

    @abc.abstractmethod
    def stop(self):
        pass


class AMQPListener(Listener):
    __processed_task_queue = queue.Queue()

    def __init__(self, new_task_queue):
        super().__init__(new_task_queue)
        self._exchange_list = list(itertools.product(CONFIG.enabled_resources, ("create", "update", "delete")))
        self._connection = None
        self._channel = None
        self._on_cancel_callback_is_set = False
        self._closing = False
        self._consumer_tag = None
        self._url = "amqp://{0.user}:{0.password}@{0.host}:{0.port}/%2F" \
                    "?heartbeat_interval={0.heartbeat_interval}" \
                    "&connection_attempts={0.connection_attempts}" \
                    "&retry_delay={0.retry_delay}".format(CONFIG.amqp)

    @classmethod
    def get_processed_task_queue(cls):
        return cls.__processed_task_queue

    def _connect(self):
        return pika.SelectConnection(pika.URLParameters(self._url), self._on_connection_open)

    def _on_connection_open(self, unused_connection):
        self._add_on_connection_close_callback()
        self._open_channel()

    def _add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self._on_connection_closed)

    def _on_connection_closed(self, unused_connection, unused_reply_code, unused_reply_text):
        if self._closing:
            self._connection.ioloop._stopping = True
        else:
            self._reconnect()

    def _reconnect(self):
        if not self._closing:
            time.sleep(CONFIG.amqp.connection_timeout)
            self.listen()

    def _open_channel(self):
        self._connection.channel(on_open_callback=self._on_channel_open)

    def _on_channel_open(self, channel):
        self._channel = channel
        self._add_on_channel_close_callback()
        for exchange in self._exchange_list:
            self._setup_exchange("{0}.{1}".format(*exchange))

    def _add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self._on_channel_closed)

    def _on_channel_closed(self, unused_channel, unused_reply_code, unused_reply_text):
        self._connection.close()

    def _setup_exchange(self, exchange_name):
        queue_name = "{0}.{1}".format(CONFIG.amqp.consumer_routing_key, exchange_name)
        self._channel.exchange_declare(
            callback=functools.partial(self._on_exchange_declareok, queue_name=queue_name, exchange_name=exchange_name),
            name=exchange_name,
            type=CONFIG.amqp.exchange_type,
            durable=True
        )

    def _on_exchange_declareok(self, unused_frame, queue_name, exchange_name):
        self._setup_queue(queue_name, exchange_name)

    def _setup_queue(self, queue_name, exchange_name):
        self._channel.queue_declare(callback=functools.partial(self._on_queue_declareok,
                                                               queue_name=queue_name,
                                                               exchange_name=exchange_name),
                                    queue=queue_name,
                                    durable=True,
                                    auto_delete=False)

    def _on_queue_declareok(self, unused_method_frame, queue_name, exchange_name):
        self._channel.queue_bind(functools.partial(self._on_bindok,
                                                   queue_name=queue_name,
                                                   exchange_name=exchange_name),
                                 queue_name,
                                 exchange_name,
                                 CONFIG.amqp.consumer_routing_key)

    def _on_bindok(self, unused_frame, queue_name, exchange_name):
        self._start_consuming(queue_name, exchange_name)

    def _start_consuming(self, queue_name, exchange_name):
        if not self._on_cancel_callback_is_set:
            self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            consumer_callback=functools.partial(self._on_message, exchange_name=exchange_name),
            queue=queue_name
        )

    def _add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)
        self._on_cancel_callback_is_set = True

    def _on_consumer_cancelled(self, unused_method_frame):
        if self._channel:
            self._channel.close()

    def _on_message(self, unused_channel, basic_deliver, properties, body, exchange_name):
        if exchange_name != basic_deliver.exchange:
            raise ContextValidationError("Message '{0}' came from unexpected exchange '{1}', "
                                         "expected '{2}'".format(body, basic_deliver.exchange, exchange_name))
        context = dict(zip(("res_type", "action"), exchange_name.split(".")))
        context["delivery_tag"] = basic_deliver.delivery_tag
        context["provider"] = properties.headers["provider"]
        self.take_event(context, body)

    def _acknowledge_message(self, delivery_tag):
        try:
            self._channel.basic_ack(delivery_tag)
        except pika.exceptions.ConnectionClosed:
            time.sleep(CONFIG.amqp.retry_delay + 1)
            self._channel.basic_ack(delivery_tag)

    def _reject_message(self, delivery_tag):
        try:
            self._channel.basic_nack(delivery_tag)
        except pika.exceptions.ConnectionClosed:
            time.sleep(CONFIG.amqp.retry_delay + 1)
            self._channel.basic_nack(delivery_tag)

    def _stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)

    def _on_cancelok(self, unused_frame):
        self._close_channel()

    def _close_channel(self):
        self._channel.close()

    def _close_connection(self):
        self._connection.close()

    def listen(self):
        taskexecutor.utils.set_thread_name("AMQPListener")
        self._connection = self._connect()
        queue = self.get_processed_task_queue()
        while not self._connection.ioloop._stopping:
            while queue.qsize() > 0:
                task = queue.get_nowait()
                method = {task.state is taskexecutor.task.DONE: self._acknowledge_message,
                          task.state is taskexecutor.task.FAILED: self._reject_message}[True]
                method(task.tag)
            self._connection.ioloop.poll()
            self._connection.ioloop.process_timeouts()

    def take_event(self, context, message):
        message = json.loads(message.decode("UTF-8"))
        message["params"]["objRef"] = message["objRef"]
        message["params"]["provider"] = context["provider"]
        message.pop("objRef")
        taskexecutor.utils.set_thread_name("OPERATION IDENTITY: {0[operationIdentity]} "
                                           "ACTION IDENTITY: {0[actionIdentity]}".format(message))
        task = taskexecutor.task.Task(tag=context["delivery_tag"],
                                      origin=self.__class__,
                                      opid=message["operationIdentity"],
                                      actid=message["actionIdentity"],
                                      res_type=context["res_type"],
                                      action=context["action"],
                                      params=message["params"])
        self._new_task_queue.put(task)
        LOGGER.debug("New task created: {}".format(task))
        taskexecutor.utils.set_thread_name("AMQPListener")

    def stop(self):
        self._closing = True
        self._stop_consuming()
        return not self._connection


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
        for action, res_types in CONFIG.schedule._asdict().items():
            for res_type, params in res_types._asdict().items():
                res_type = taskexecutor.utils.to_lower_dashed(res_type)
                if res_type in CONFIG.enabled_resources:
                    context = {"res_type": res_type, "action": action}
                    message = {"params": dict(params._asdict())}
                    job = schedule.every(params.interval).seconds.do(self.take_event, context, message)
                    LOGGER.debug(job)

    def listen(self):
        taskexecutor.utils.set_thread_name("TimeListener")
        self._schedule()
        while not self._stopping:
            schedule.run_pending()
            queue = self.get_processed_task_queue()
            while queue.qsize() > 0:
                task = queue.get_nowait()
                if task.state == taskexecutor.task.FAILED:
                    LOGGER.warning("Got failed scheduled task: {}")
                    del task
            sleep_interval = abs(schedule.idle_seconds()) if schedule.jobs else 10
            if not self._stopping:
                LOGGER.debug("Sleeping for {} s".format(sleep_interval))
                time.sleep(sleep_interval)

    def take_event(self, context, message):
        action_id = "{0}.{1}".format(context["res_type"], context["action"])
        task = taskexecutor.task.Task(tag=action_id,
                                      origin=self.__class__,
                                      opid="LOCAL-SCHED",
                                      actid=action_id,
                                      res_type=context["res_type"],
                                      action=context["action"],
                                      params=message["params"])
        self._new_task_queue.put(task)
        LOGGER.debug("New task created from locally scheduled event: {}".format(task))

    def stop(self):
        schedule.clear()
        self._stopping = True


class Builder:
    def __new__(cls, listener_type):
        ListenerClass = {"amqp": AMQPListener,
                         "time": TimeListener}.get(listener_type)
        if not ListenerClass:
            raise BuilderTypeError("Unknown Listener type: {}".format(listener_type))
        return ListenerClass
