import unittest
import unittest.mock
import socket
import threading
import pika
import json
import time
from mixins import PythonDockerTestMixin, ContainerNotReady
import taskexecutor.task


class TestAMQPListener(PythonDockerTestMixin, unittest.TestCase):
    CONTAINER_IMAGE = "rabbitmq"
    AMQP_LISTENER_THREAD = None

    @classmethod
    def container_ready_callback(cls, container_data):
        cls.CONTAINER_ADDR = container_data['NetworkSettings']['IPAddress']
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.connect((cls.CONTAINER_ADDR, 5672))
            except:
                raise ContainerNotReady("RabbitMQ did not answer on 5672 port")

    def _start_and_wait_for_amqp_listener_thread(self):
        if not self.AMQP_LISTENER_THREAD:
            self.AMQP_LISTENER_THREAD = threading.Thread(
                    target=self.amqp_listener.listen,
                    daemon=True
            )
            self.AMQP_LISTENER_THREAD.start()
            print("THREAD STARTED")
        counter = 0
        while not self.amqp_listener._consumer_tag:
            print("WAITING FOR LISTENER", counter)
            if counter == 50:
                raise RuntimeError("AMQPListener failed to start")
            counter += 1
            time.sleep(.1)

    def _send_amqp_message(self, exchange, message_body):
        _params = pika.ConnectionParameters(
                host=self.mock_config.CONFIG.amqp.host,
                credentials=pika.credentials.PlainCredentials(
                        self.mock_config.CONFIG.amqp.user,
                        self.mock_config.CONFIG.amqp.password
                )
        )
        _connection = pika.BlockingConnection(_params)
        _channel = _connection.channel()
        _channel.exchange_declare(
                exchange=exchange,
                exchange_type=self.mock_config.CONFIG.amqp.exchange_type
        )
        _channel.basic_publish(
                exchange=exchange,
                routing_key=self.mock_config.CONFIG.amqp.consumer_routing_key,
                body=message_body,
                properties=pika.BasicProperties(headers={"provider": "rc-user"})
        )
        _channel.close()

    def setUp(self):
        self.mock_config = unittest.mock.MagicMock()
        self.mock_config.CONFIG.amqp = unittest.mock.Mock(
            spec_set=["user",
                      "password",
                      "host",
                      "exchange_type",
                      "consumer_routing_key",
                      "connection_attempts",
                      "retry_delay",
                      "heartbeat_interval",
                      "connection_timeout"]
        )
        self.mock_config.CONFIG.amqp.user = "guest"
        self.mock_config.CONFIG.amqp.password = "guest"
        self.mock_config.CONFIG.amqp.host = self.CONTAINER_ADDR
        self.mock_config.CONFIG.amqp.exchange_type = "topic"
        self.mock_config.CONFIG.amqp.consumer_routing_key = "te.web99"
        self.mock_config.CONFIG.amqp.connection_attempts = 1
        self.mock_config.CONFIG.amqp.retry_delay = 5
        self.mock_config.CONFIG.amqp.heartbeat_interval = 30
        self.mock_config.CONFIG.amqp.connection_timeout = 5
        self.mock_config.CONFIG.enabled_resources = ["unix-account",
                                                     "database-user",
                                                     "database",
                                                     "website",
                                                     "sslcertificate"]
        self.mock_executor = unittest.mock.Mock()
        self.mock_executor_executor = unittest.mock.Mock()
        self.mock_executor.Executor = unittest.mock.Mock(
            return_value=self.mock_executor_executor
        )
        self.mock_executor.Executor.process_task = unittest.mock.Mock()
        self.mock_executors_instance = unittest.mock.Mock()
        self.mock_executor.Executors = unittest.mock.Mock(
                return_value=self.mock_executors_instance
        )
        self.mock_future = unittest.mock.Mock()
        self.mock_executors_instance.pool.submit = \
            unittest.mock.Mock(return_value=self.mock_future)
        modules = {'taskexecutor.config': self.mock_config,
                   'taskexecutor.executor': self.mock_executor}
        self.module_patcher = unittest.mock.patch.dict('sys.modules', modules)
        self.module_patcher.start()
        import taskexecutor.listener
        self.amqp_listener = taskexecutor.listener.AMQPListener()

    def tearDown(self):
        if self.AMQP_LISTENER_THREAD:
            self.AMQP_LISTENER_THREAD.join(timeout=1)
        self.module_patcher.stop()

    def test_listen(self):
        test_string = 'Test string {}"":,'
        self.amqp_listener._on_message = unittest.mock.create_autospec(
                self.amqp_listener._on_message
        )
        self._start_and_wait_for_amqp_listener_thread()
        counter = 0
        for msg_number, exchange in enumerate(("unix-account.create",
                                               "database-user.update",
                                               "website.delete"), 1):
            self._send_amqp_message(exchange, test_string)
            while self.amqp_listener._on_message.call_count < msg_number:
                if counter == 50:
                    raise RuntimeError("AMQPListener did not recieve message")
                counter += 1
                time.sleep(.1)
            self.assertEqual(
                self.amqp_listener._on_message.call_args[1]["exchange_name"],
                exchange
            )
            self.assertEqual(
                self.amqp_listener._on_message.call_args[0][1].exchange,
                exchange
            )
            self.assertEqual(
                self.amqp_listener._on_message.call_args[0][2].headers[
                    "provider"],
                "rc-user"
            )
            self.assertEqual(self.amqp_listener._on_message.call_args[0][3],
                             test_string.encode("UTF-8"))
            self.assertEqual(self.amqp_listener._on_message.call_count,
                             msg_number)

    def test_take_event(self):
        test_operationIdentity = "testOpId"
        test_actionIdentity = "testActId"
        test_objRef = "http://host/path/to/resource"
        test_params = {"provider": "rc-user", "objRef": test_objRef}
        test_message = json.dumps({"operationIdentity": test_operationIdentity,
                                   "actionIdentity": test_actionIdentity,
                                   "objRef": test_objRef,
                                   "params": test_params}).encode("UTF-8")
        test_context = {"res_type": "unix-account",
                        "action": "create",
                        "delivery_tag": 1,
                        "provider": "rc-user"}
        mock_task = unittest.mock.Mock()
        mock_future = unittest.mock.Mock()
        self.amqp_listener.create_task = unittest.mock.create_autospec(
            self.amqp_listener.create_task,
            return_value=mock_task
        )
        self.amqp_listener.pass_task = unittest.mock.create_autospec(
            self.amqp_listener.pass_task,
            return_value=mock_future
        )
        self.amqp_listener.take_event(test_context, test_message)
        test_params.update(objRef=test_objRef)
        self.amqp_listener.create_task.assert_called_once_with(
            test_operationIdentity,
            test_actionIdentity,
            test_context["res_type"],
            test_context["action"],
            test_params
        )
        self.amqp_listener.pass_task.assert_called_once_with(
            mock_task,
            self.amqp_listener.acknowledge_message,
            args=(test_context["delivery_tag"],)
        )
        assert mock_future in self.amqp_listener._futures_tags_map
        self.assertEqual(self.amqp_listener._futures_tags_map[mock_future],
                         test_context["delivery_tag"])

    def test_create_task(self):
        self.amqp_listener.set_thread_name = unittest.mock.Mock()
        test_operationIdentity = "testOpId"
        test_actionIdentity = "testActId"
        test_res_type = "unix-account"
        test_action = "create"
        test_params = {"provider": "rc-user",
                       "objRef": "http://host/path/to/resource"}
        task = self.amqp_listener.create_task(test_operationIdentity,
                                              test_actionIdentity,
                                              test_res_type,
                                              test_action,
                                              test_params)
        self.assertIsInstance(task, taskexecutor.task.Task)
        self.assertEqual(task.opid, test_operationIdentity)
        self.assertEqual(task.actid, test_actionIdentity)
        self.assertEqual(task.res_type, test_res_type)
        self.assertEqual(task.action, test_action)
        self.assertEqual(task.params["provider"], test_params["provider"])
        self.assertEqual(task.params["objRef"], test_params["objRef"])

    def test_pass_task(self):
        mock_task = unittest.mock.Mock()
        test_callback = lambda x: True
        test_args = ("testarg",)
        future = self.amqp_listener.pass_task(mock_task,
                                              test_callback,
                                              test_args)
        self.mock_executor.Executors.assert_called_once_with()
        self.mock_executor.Executor.assert_called_once_with(mock_task,
                                                            test_callback,
                                                            test_args)
        self.assertEqual(self.mock_executors_instance.method_calls, [
            unittest.mock.call.pool.submit(
                self.mock_executor_executor.process_task)])
        self.assertEqual(future, self.mock_future)

    def test_stop(self):
        self._start_and_wait_for_amqp_listener_thread()
        self.assertTrue(self, self.amqp_listener.stop())


if __name__ == '__main__':
    unittest.main()
