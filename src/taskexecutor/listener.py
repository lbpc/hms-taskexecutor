import functools
import json
import abc
import itertools
import pika
import time
import schedule
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.executor
import taskexecutor.constructor
import taskexecutor.task
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class ContextValidationError(Exception):
    pass


class Listener(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def listen(self):
        pass

    @abc.abstractmethod
    def take_event(self, context, message):
        pass

    @abc.abstractmethod
    def create_task(self, opid, actid, res_type, action, params):
        pass

    @abc.abstractmethod
    def pass_task(self, task, callback, args):
        pass

    @abc.abstractmethod
    def stop(self):
        pass


class AMQPListener(Listener):
    def __init__(self):
        self._futures_tags_mapping = dict()
        self._exchange_list = list(itertools.product(CONFIG.enabled_resources, ("create", "update", "delete")))
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
        return pika.SelectConnection(pika.URLParameters(self._url), self._on_connection_open)

    def _on_connection_open(self, unused_connection):
        self._add_on_connection_close_callback()
        self._open_channel()

    def _add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self._on_connection_closed)

    def _on_connection_closed(self, unused_connection, unused_reply_code, unused_reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop._stopping = True
        else:
            self._connection.add_timeout(CONFIG.amqp.connection_timeout, self._reconnect)

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

    def _on_channel_closed(self, unused_channel, unused_reply_code, unused_reply_text):
        self._connection.close()

    def _setup_exchange(self, exchange_name):
        queue_name = "{0}.{1}".format(CONFIG.amqp.consumer_routing_key, exchange_name)
        self._channel.exchange_declare(
            functools.partial(self._on_exchange_declareok, queue_name=queue_name, exchange_name=exchange_name),
            exchange_name,
            CONFIG.amqp.exchange_type
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
        taskexecutor.utils.set_thread_name("AMQPListener")
        self._connection = self._connect()
        while not self._connection.ioloop._stopping:
            for future, tag in self._futures_tags_mapping.copy().items():
                if not future.running():
                    if future.exception():
                        LOGGER.error(future.exception())
                        self._reject_message(tag)
                    del self._futures_tags_mapping[future]
            self._connection.ioloop.poll()
            self._connection.ioloop.process_timeouts()

    def take_event(self, context, message):
        message = json.loads(message.decode("UTF-8"))
        message["params"]["objRef"] = message["objRef"]
        message["params"]["provider"] = context["provider"]
        message.pop("objRef")
        task = self.create_task(message["operationIdentity"],
                                message["actionIdentity"],
                                context["res_type"],
                                context["action"],
                                message["params"])
        future = self.pass_task(task=task, callback=self.acknowledge_message, args=(context["delivery_tag"],))
        self._futures_tags_mapping[future] = context["delivery_tag"]

    def create_task(self, opid, actid, res_type, action, params):
        taskexecutor.utils.set_thread_name("OPERATION IDENTITY: {0} ACTION IDENTITY: {1}".format(opid, actid))
        task = taskexecutor.task.Task(opid, actid, res_type, action, params)
        LOGGER.info("New task created: {}".format(task))
        taskexecutor.utils.set_thread_name("AMQPListener")
        return task

    def pass_task(self, task, callback, args):
        constructor = taskexecutor.constructor.Constructor()
        executors_pool = constructor.get_command_executors_pool()
        executor = taskexecutor.executor.Executor(task, callback, args)
        return executors_pool.submit(executor.process_task)

    def stop(self):
        self._closing = True
        self._stop_consuming()
        return not self._connection


class TimeListener(Listener):
    def __init__(self):
        self._stopping = False
        self._futures = list()

    def _schedule(self):
        for action, res_types in vars(CONFIG.schedule).items():
            for res_type, params in vars(res_types).items():
                res_type = taskexecutor.utils.to_lower_dashed(res_type)
                if res_type in CONFIG.enabled_resources:
                    context = {"res_type": res_type, "action": action}
                    message = {"params": params}
                    job = schedule.every(params.interval).seconds.do(self.take_event, context, message)
                    LOGGER.info(job)

    def listen(self):
        taskexecutor.utils.set_thread_name("TimeListener")
        self._schedule()
        while not self._stopping:
            schedule.run_pending()
            for future in self._futures:
                if not future.running():
                    if future.exception():
                        LOGGER.error(future.exception())
                    self._futures.remove(future)
            if schedule.jobs and not self._stopping:
                time.sleep(abs(schedule.idle_seconds()))
            else:
                time.sleep(.1)

    def take_event(self, context, message):
        task = self.create_task(None, None, context["res_type"], context["action"], message["params"])
        self._futures.append(self.pass_task(task=task, callback=None, args=None))

    def create_task(self, opid, actid, res_type, action, params):
        task = taskexecutor.task.Task(opid, actid, res_type, action, vars(params))
        LOGGER.info("New task created from locally scheduled event: {}".format(task))
        return task

    def pass_task(self, task, callback, args):
        constructor = taskexecutor.constructor.Constructor()
        executors_pool = constructor.get_query_executors_pool()
        executor = taskexecutor.executor.Executor(task)
        return executors_pool.submit(executor.process_task)

    def stop(self):
        schedule.clear()
        self._stopping = True


class Builder:
    def __new__(cls, listener_type):
        if listener_type == "amqp":
            return AMQPListener
        elif listener_type == "time":
            return TimeListener
        else:
            raise BuilderTypeError("Unknown Listener type: {}".format(listener_type))
