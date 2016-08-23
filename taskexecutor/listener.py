import functools
import pika
import json
import time
from abc import ABCMeta, abstractmethod
from itertools import product

from taskexecutor.config import CONFIG
from taskexecutor.executor import Executor, Executors
from taskexecutor.logger import LOGGER
from taskexecutor.task import Task


class Listener(metaclass=ABCMeta):
	@abstractmethod
	def listen(self):
		pass

	@abstractmethod
	def take_event(self, context, message):
		pass

	@abstractmethod
	def create_task(self, id, res_type, action, params):
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
		self._exchange_iterator = product(CONFIG["enabled_resources"],
		                                  CONFIG["enabled_actions"])
		self._connection = None
		self._channel = None
		self._on_cancel_callback_is_set = False
		self._closing = False
		self._consumer_tag = None
		self._url = "amqp://{user}:{password}@{host}:5672/%2F" \
		            "?heartbeat_interval={heartbeat_interval}" \
		            "&connection_attempts={connection_attempts}" \
		            "&retry_delay={retry_delay}".format_map(
				CONFIG["amqp"])

	def connect(self):
		return pika.SelectConnection(pika.URLParameters(self._url),
		                             self.on_connection_open,
		                             stop_ioloop_on_close=False)

	def on_connection_open(self, unused_connection):
		self.add_on_connection_close_callback()
		self.open_channel()

	def add_on_connection_close_callback(self):
		self._connection.add_on_close_callback(self.on_connection_closed)

	def on_connection_closed(self, connection, reply_code, reply_text):
		self._channel = None
		if self._closing:
			self._connection.ioloop.stop()
		else:
			self._connection.add_timeout(CONFIG["amqp"]["connection_timeout"],
			                             self.reconnect)

	def reconnect(self):
		self._connection.ioloop.stop()
		if not self._closing:
			self._connection = self.connect()
			self._connection.ioloop.start()

	def open_channel(self):
		self._connection.channel(on_open_callback=self.on_channel_open)

	def on_channel_open(self, channel):
		self._channel = channel
		self.add_on_channel_close_callback()
		while True:
			try:
				self.setup_exchange(
						"{0}.{1}".format(*next(self._exchange_iterator))
				)
			except StopIteration:
				break

	def add_on_channel_close_callback(self):
		self._channel.add_on_close_callback(self.on_channel_closed)

	def on_channel_closed(self, channel, reply_code, reply_text):
		self._connection.close()

	def setup_exchange(self, exchange_name):
		self._channel.exchange_declare(
				functools.partial(self.on_exchange_declareok,
				                  queue_name=exchange_name,
				                  exchange_name=exchange_name),
				exchange_name,
				CONFIG["amqp"]["exchange_type"]
		)

	def on_exchange_declareok(self, unused_frame, queue_name, exchange_name):
		self.setup_queue(queue_name, exchange_name)

	def setup_queue(self, queue_name, exchange_name):
		self._channel.queue_declare(
				callback=functools.partial(self.on_queue_declareok,
				                  queue_name=queue_name,
				                  exchange_name=exchange_name),
				queue=queue_name,
				durable=True,
				auto_delete=False
		)

	def on_queue_declareok(self, method_frame, queue_name, exchange_name):
		self._channel.queue_bind(
				functools.partial(self.on_bindok,
				                  queue_name=queue_name,
				                  exchange_name=exchange_name),
				queue_name,
				exchange_name,
				CONFIG["amqp"]["consumer_routing_key"]
		)

	def on_bindok(self, unused_frame, queue_name, exchange_name):
		self.start_consuming(queue_name, exchange_name)

	def start_consuming(self, queue_name, exchange_name):
		if not self._on_cancel_callback_is_set:
			self.add_on_cancel_callback()
		self._consumer_tag = self._channel.basic_consume(
				consumer_callback=functools.partial(self.on_message, exchange_name=exchange_name),
				queue=queue_name
		)

	def add_on_cancel_callback(self):
		self._channel.add_on_cancel_callback(self.on_consumer_cancelled)
		self._on_cancel_callback_is_set = True

	def on_consumer_cancelled(self, method_frame):
		if self._channel:
			self._channel.close()

	def on_message(self, unused_channel, basic_deliver, properties, body, exchange_name):
		context = dict(zip(("res_type", "action"), exchange_name.split(".")))
		context["delivery_tag"] = basic_deliver.delivery_tag
		self.take_event(context, body)

	def acknowledge_message(self, delivery_tag):
		self._channel.basic_ack(delivery_tag)

	def reject_message(self, delivery_tag):
		self._channel.basic_nack(delivery_tag)

	def stop_consuming(self):
		if self._channel:
			self._channel.basic_cancel(self.on_cancelok, self._consumer_tag)

	def on_cancelok(self, unused_frame):
		self.close_channel()

	def close_channel(self):
		self._channel.close()

	def listen(self):
		self._connection = self.connect()
		self._connection.ioloop._stopping = False
		while not self._connection.ioloop._stopping:
			for future, tag in self._futures_tags_map.copy().items():
				if not future.running():
					if future.exception():
						self.reject_message(tag)
					del self._futures_tags_map[future]
			self._connection.ioloop.poll()
			self._connection.ioloop.process_timeouts()

	def stop(self):
		self._closing = True
		self.stop_consuming()
		self._connection.ioloop._stopping = True

	def close_connection(self):
		self._connection.close()

	def take_event(self, context, message):
		message = json.loads(message.decode("UTF-8"))
		message["params"]["objRef"] = message["objRef"]
		message.pop("objRef")
		task = self.create_task(message["opId"],
		                 context["res_type"],
		                 context["action"],
		                 message["params"])
		future = self.pass_task(task=task,
		                        callback=self.acknowledge_message,
		                        args=(context["delivery_tag"],))
		self._futures_tags_map[future] = context["delivery_tag"]

	def create_task(self, id, res_type, action, params):
		task = Task(id, res_type, action, params)
		LOGGER.info("New task created: {}".format(task))
		return task

	def pass_task(self, task, callback, args):
		executors = Executors()
		executor = Executor(task, callback, args)
		return executors.pool.submit(executor.process_task)


class ListenerBuilder:
	def __new__(self, type):
		if type == "amqp":
			return AMQPListener()
		else:
			raise ValueError("Unknown Listener type: {}".format(type))
