from abc import ABCMeta, abstractmethod
import pika
import json

from taskexecutor.config import CONFIG

class Reporter(metaclass=ABCMeta):
	def __init__(self):
		self._report = dict()

	@abstractmethod
	def create_report(self, task):
		pass

	@abstractmethod
	def send_report(self):
		pass


class AMQPReporter(Reporter):
	def __init__(self):
		super().__init__()
		self._url = "amqp://{user}:{password}@{host}:5672/%2F" \
		            "?heartbeat_interval={heartbeat_interval}".format_map(
				CONFIG["amqp"])
		self._connection = None
		self._channel = None
		self._exchange = "REPORT"
		self._routing_key = "REPORT"

	def create_report(self, task):
		self._report["opId"] = task.id
		self._report["objRef"] = task.params["objRef"]
		return self._report

	def connnect(self):
		return pika.BlockingConnection(pika.URLParameters(self._url))

	def open_channel(self):
		return self._connection.channel()

	def close_channel(self):
		self._channel.close()

	def declare_exchange(self, exchange, type):
		self._channel.exchange_declare(exchange=exchange,
		                               type=type,
		                               auto_delete=False)

	def publish_message(self, message):
		self._channel.basic_publish(exchange=self._exchange,
		                            routing_key=self._routing_key,
		                            body=message)

	def report_to_json(self):
		return json.dumps(self._report)

	def send_report(self):
		self._connection = self.connnect()
		self._channel = self.open_channel()
		self.declare_exchange(self._exchange, CONFIG["amqp"]["exchange_type"])
		self.publish_message(self.report_to_json())
		self.close_channel()


class ReporterBuilder:
	def __new__(self, type):
		if type == "amqp":
			return AMQPReporter()
		else:
			raise ValueError("Unknown Reporter type: {}".format(type))