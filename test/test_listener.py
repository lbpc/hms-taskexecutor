import unittest
from unittest.mock import Mock
import queue
import json
import sys
import pika
from .mock_config import mock_config

sys.modules['taskexecutor.config'] = mock_config

import taskexecutor.task
import taskexecutor.listener


class TestAMQPListener(unittest.TestCase):
    def setUp(self):

        self.mock_task = Mock(spec=taskexecutor.task.Task)
        self.mock_task.state = 0
        self.mock_new_task_queue = Mock(spec=queue.Queue)
        self.amqp_listener = taskexecutor.listener.AMQPListener(self.mock_new_task_queue)

    def test_listen(self):
        self.poll_count = 0

        def decrement_processed_task_queue_qsize(_):
            self.mock_processed_task_queue.qsize.return_value -= 1

        def ioloop_poll_side_effect():
            if self.poll_count == 0:
                self.mock_processed_task_queue.qsize.return_value = 1
                self.mock_task.tag = 42
                self.mock_task.state = 2
                self.assertTrue(self.amqp_listener._acknowledge_message.called_once_with(42))
            elif self.poll_count == 1:
                self.mock_processed_task_queue.qsize.return_value = 1
                self.mock_task.tag = 666
                self.assertTrue(self.amqp_listener._reject_message.called_once_with(666))
                self.mock_task.state = 3
            else:
                self.mock_connection.ioloop._stopping = True
            self.poll_count += 1

        self.mock_connection = Mock(spec=pika.adapters.select_connection.SelectConnection)
        self.mock_connection.ioloop = Mock(spec=pika.adapters.select_connection.IOLoop)
        self.mock_processed_task_queue = Mock(spec=queue.Queue)
        self.mock_processed_task_queue.qsize = Mock(return_value=0)
        self.mock_processed_task_queue.get_nowait = Mock(return_value=self.mock_task)
        self.amqp_listener._connect = Mock(return_value=self.mock_connection)
        self.amqp_listener._reject_message = Mock(side_effect=decrement_processed_task_queue_qsize)
        self.amqp_listener._acknowledge_message = Mock(side_effect=decrement_processed_task_queue_qsize)
        self.amqp_listener.get_processed_task_queue = Mock(return_value=self.mock_processed_task_queue)
        self.mock_connection.ioloop.poll = Mock(side_effect=ioloop_poll_side_effect)
        self.mock_connection.ioloop._stopping = False
        self.mock_connection.ioloop.process_timeouts = Mock()
        self.amqp_listener.listen()
        self.assertEqual(self.amqp_listener._connect.call_count, 1)
        self.assertEqual(self.mock_connection.ioloop.poll.call_count, 3)

    def test_take_event(self):
        test_operationIdentity = 'testOpId'
        test_actionIdentity = 'testActId'
        test_objRef = 'http://host/path/to/resource'
        test_params = {'provider': 'rc-user', 'objRef': test_objRef}
        test_message = json.dumps({'operationIdentity': test_operationIdentity,
                                   'actionIdentity': test_actionIdentity,
                                   'objRef': test_objRef,
                                   'params': test_params}).encode('UTF-8')
        test_context = {'res_type': 'unix-account',
                        'action': 'create',
                        'delivery_tag': 1,
                        'provider': 'rc-user'}
        self.amqp_listener._new_task_queue = self.mock_new_task_queue
        self.amqp_listener.take_event(test_context, test_message)
        self.assertEqual(self.mock_new_task_queue.put.call_count, 1)
        self.assertEqual(self.mock_new_task_queue.put.call_args[0][0].tag, test_context['delivery_tag'])
        self.assertEqual(self.mock_new_task_queue.put.call_args[0][0].origin, self.amqp_listener.__class__)
        self.assertEqual(self.mock_new_task_queue.put.call_args[0][0].opid, test_operationIdentity)
        self.assertEqual(self.mock_new_task_queue.put.call_args[0][0].actid, test_actionIdentity)
        self.assertEqual(self.mock_new_task_queue.put.call_args[0][0].res_type, test_context['res_type'])
        self.assertEqual(self.mock_new_task_queue.put.call_args[0][0].action, test_context['action'])
        self.assertEqual(self.mock_new_task_queue.put.call_args[0][0].params, test_params)

    def test_stop(self):
        self.amqp_listener._stop_consuming = Mock()
        self.amqp_listener.stop()
        self.assertEqual(self.amqp_listener._stop_consuming.call_count, 1)


if __name__ == '__main__':
    unittest.main()
