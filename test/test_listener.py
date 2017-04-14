import unittest
import unittest.mock
import queue
import json
import sys
import pika


class TestAMQPListener(unittest.TestCase):
    def setUp(self):
        self.mock_config = unittest.mock.MagicMock()
        self.mock_config.CONFIG.amqp = unittest.mock.Mock(spec_set=["user",
                                                                    "password",
                                                                    "host",
                                                                    "exchange_type",
                                                                    "consumer_routing_key",
                                                                    "connection_attempts",
                                                                    "retry_delay",
                                                                    "heartbeat_interval",
                                                                    "connection_timeout"])
        self.mock_config.CONFIG.amqp.user = "guest"
        self.mock_config.CONFIG.amqp.password = "guest"
        self.mock_config.CONFIG.amqp.host = "127.0.0.1"
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
                                                     "ssl-certificate"]
        sys.modules["taskexecutor.config"] = self.mock_config
        import taskexecutor.task
        import taskexecutor.listener
        self.mock_task = unittest.mock.MagicMock(spec=taskexecutor.task.Task)
        self.mock_new_task_queue = unittest.mock.MagicMock(spec=queue.Queue)
        self.amqp_listener = taskexecutor.listener.AMQPListener(self.mock_new_task_queue)

    def no_test_listen(self):
        self.poll_count = 0

        def ioloop_poll_side_effect():
            if self.poll_count == 0:
                self.amqp_listener._futures_tags_mapping = {self.mock_future: 61}
            self.poll_count += 1

        def ioloop_process_timeouts_side_effect():
            if self.poll_count == 1:
                self.mock_future.running.return_value = False
            elif self.poll_count == 2:
                self.assertFalse(self.amqp_listener._reject_message.called)
                self.mock_future.exception.return_value = "EXCEPTION!11"
            elif self.poll_count == 3:
                self.assertTrue(self.amqp_listener._reject_message.called_once_with(61))
                self.assertEqual(self.mock_future.exception(), "EXCEPTION!11")
                mock_connection.ioloop._stopping = True

        self.mock_future.running = unittest.mock.Mock(return_value=True)
        self.mock_future.exception = unittest.mock.Mock(return_value=False)
        mock_connection = unittest.mock.Mock(spec=pika.adapters.select_connection.SelectConnection)
        mock_connection.ioloop = unittest.mock.Mock(spec=pika.adapters.select_connection.IOLoop)
        self.amqp_listener._connect = unittest.mock.Mock(return_value=mock_connection)
        self.amqp_listener._reject_message = unittest.mock.Mock()
        mock_connection.ioloop.poll = unittest.mock.Mock(side_effect=ioloop_poll_side_effect)
        mock_connection.ioloop._stopping = False
        mock_connection.ioloop.process_timeouts = unittest.mock.Mock(side_effect=ioloop_process_timeouts_side_effect)
        self.amqp_listener.listen()
        self.amqp_listener._connect.assert_called_once_with()
        self.assertEqual(mock_connection.ioloop.poll.call_count, 3)
        self.assertFalse(self.amqp_listener._futures_tags_mapping)

    def no_test_take_event(self):
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
        self.amqp_listener.create_task = unittest.mock.create_autospec(self.amqp_listener.create_task,
                                                                       return_value=self.mock_task)
        self.amqp_listener.pass_task = unittest.mock.create_autospec(self.amqp_listener.pass_task,
                                                                     return_value=self.mock_future)
        self.amqp_listener.take_event(test_context, test_message)
        test_params.update(objRef=test_objRef)
        self.amqp_listener.create_task.assert_called_once_with(test_operationIdentity,
                                                               test_actionIdentity,
                                                               test_context["res_type"],
                                                               test_context["action"],
                                                               test_params)
        self.amqp_listener.pass_task.assert_called_once_with(self.mock_task,
                                                             self.amqp_listener._acknowledge_message,
                                                             args=(test_context["delivery_tag"],))
        self.assertEqual(self.amqp_listener._futures_tags_mapping[self.mock_future], test_context["delivery_tag"])

    def test_stop(self):
        pass


if __name__ == '__main__':
    unittest.main()
