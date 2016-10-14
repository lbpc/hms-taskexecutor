import functools
import json
from abc import ABCMeta, abstractmethod
from itertools import product
import pika
from taskexecutor.config import CONFIG
from taskexecutor.executor import Executor, Executors
from taskexecutor.task import Task
from taskexecutor.utils import set_thread_name
from taskexecutor.logger import LOGGER


class Listener(metaclass=ABCMeta):
    @abstractmethod
    def listen(self):
        pass

    @abstractmethod
    def take_event(self, context, message):
        pass

    @abstractmethod
    def create_task(self, opid, actid, res_type, action, params):
        pass

    @abstractmethod
    def pass_task(self, task, callback, args):
        pass

    @abstractmethod
    def stop(self):
        pass


class AMQPListener(Listener):
    def __init__(self):
        self._futures_tags_map = dict()
        self._exchange_list = list(product(CONFIG.enabled_resources,
                                           ["create", "update", "delete"]))
        self._connection = None
        self._channel = None
        self._on_cancel_callback_is_set = False
        self._closing = False
        self._consumer_tag = None
        self._url = "amqp://{0.user}:{0.password}@{0.host}:5672/%2F" \
                    "?heartbeat_interval={0.heartbeat_interval}" \
                    "&connection_attempts={0.connection_attempts}" \
                    "&retry_delay={0.retry_delay}".format(CONFIG.amqp)

    def _connect(self):
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self._on_connection_open)

    def _on_connection_open(self, unused_connection):
        self._add_on_connection_close_callback()
        self._open_channel()

    def _add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self._on_connection_closed)

    def _on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop._stopping = True
        else:
            self._connection.add_timeout(CONFIG.amqp.connection_timeout,
                                         self._reconnect)

    def _reconnect(self):
        self._connection.ioloop._stopping = True
        if not self._closing:
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

    def _on_channel_closed(self, channel, reply_code, reply_text):
        self._connection.close()

    def _setup_exchange(self, exchange_name):
        _queue_name = "{0}.{1}".format(CONFIG.amqp.consumer_routing_key,
                                       exchange_name)
        self._channel.exchange_declare(
            functools.partial(self._on_exchange_declareok,
                              queue_name=_queue_name,
                              exchange_name=exchange_name),
            exchange_name,
            CONFIG.amqp.exchange_type
        )

    def _on_exchange_declareok(self, unused_frame, queue_name, exchange_name):
        self._setup_queue(queue_name, exchange_name)

    def _setup_queue(self, queue_name, exchange_name):
        self._channel.queue_declare(
            callback=functools.partial(self._on_queue_declareok,
                                       queue_name=queue_name,
                                       exchange_name=exchange_name),
            queue=queue_name,
            durable=True,
            auto_delete=False
        )

    def _on_queue_declareok(self, method_frame, queue_name, exchange_name):
        self._channel.queue_bind(
            functools.partial(self._on_bindok,
                              queue_name=queue_name,
                              exchange_name=exchange_name),
            queue_name,
            exchange_name,
            CONFIG.amqp.consumer_routing_key
        )

    def _on_bindok(self, unused_frame, queue_name, exchange_name):
        self._start_consuming(queue_name, exchange_name)

    def _start_consuming(self, queue_name, exchange_name):
        if not self._on_cancel_callback_is_set:
            self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(
            consumer_callback=functools.partial(self._on_message,
                                                exchange_name=exchange_name),
            queue=queue_name
        )

    def _add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)
        self._on_cancel_callback_is_set = True

    def _on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def _on_message(self, unused_channel, basic_deliver, properties, body,
                    exchange_name):
        context = dict(zip(("res_type", "action"), exchange_name.split(".")))
        context["delivery_tag"] = basic_deliver.delivery_tag
        self.take_event(context, body)

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def _reject_message(self, delivery_tag):
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
        set_thread_name("AMQPListener")
        self._connection = self._connect()
        self._connection.ioloop._stopping = False
        while not self._connection.ioloop._stopping:
            for future, tag in self._futures_tags_map.copy().items():
                if not future.running():
                    if future.exception():
                        LOGGER.error(future.exception())
                        self._reject_message(tag)
                    del self._futures_tags_map[future]
            self._connection.ioloop.poll()
            self._connection.ioloop.process_timeouts()

    def take_event(self, context, message):
        message = json.loads(message.decode("UTF-8"))
        message["params"]["objRef"] = message["objRef"]
        message.pop("objRef")
        task = self.create_task(message["operationIdentity"],
                                message["actionIdentity"],
                                context["res_type"],
                                context["action"],
                                message["params"])
        future = self.pass_task(task=task,
                                callback=self.acknowledge_message,
                                args=(context["delivery_tag"],))
        self._futures_tags_map[future] = context["delivery_tag"]

    def create_task(self, opid, actid, res_type, action, params):
        set_thread_name("OPERATION IDENTITY: {0} "
                        "ACTION IDENTITY: {1}".format(opid, actid))
        task = Task(opid, actid, res_type, action, params)
        LOGGER.info("New task created: {}".format(task))
        set_thread_name("AMQPListener")
        return task

    def pass_task(self, task, callback, args):
        executors = Executors()
        executor = Executor(task, callback, args)
        return executors.pool.submit(executor.process_task)

    def stop(self):
        self._closing = True
        self._stop_consuming()
        self._connection.ioloop._stopping = True


class ListenerBuilder:
    def __new__(cls, listener_type):
        if listener_type == "amqp":
            return AMQPListener
        else:
            raise ValueError("Unknown Listener type: {}".format(listener_type))

