import unittest
from unittest.mock import patch, Mock
import queue
from kombu import Queue, Exchange, Message
from .mock_config import CONFIG

from taskexecutor.task import Task, TaskState
from taskexecutor.listener import AMQPListener


class TestAMQPListener(unittest.TestCase):
    def test_get_processed_task_queue(self):
        q1 = AMQPListener(Mock()).get_processed_task_queue()
        q2 = AMQPListener(Mock()).get_processed_task_queue()
        self.assertIs(q1, q2)
        self.assertIsInstance(q1, queue.Queue)

    @patch('taskexecutor.listener.AMQPListener.run')
    @patch('taskexecutor.listener.Connection')
    def test_listen(self, mock_connection, mock_run):
        CONFIG.hostname = 'web99'
        CONFIG.amqp.user = 'rabbituser'
        CONFIG.amqp.password = 'rabbitpassword'
        CONFIG.amqp.host = 'rabbithost'
        CONFIG.amqp.port = 95672
        CONFIG.amqp.heartbeat_interval = 42
        listener = AMQPListener(Mock())
        listener.listen()
        mock_connection.assert_called_once_with('amqp://rabbituser:rabbitpassword@rabbithost:95672//',
                                                heartbeat=42,
                                                transport_options={'client_properties': {
                                                    'connection_name': 'taskexecutor@web99'
                                                }})
        mock_run.assert_called_once_with()

    def test_get_consumers(self):
        CONFIG.hostname = 'web99'
        CONFIG.enabled_resources = ['unix-account', 'website']
        CONFIG.amqp.consumer_routing_key = 'te.rk'
        CONFIG.amqp.exchange_type = 'direct'
        listener = AMQPListener(Mock())
        mock_consumer_class = Mock()
        mock_consumer_class.return_value = 'dummy'
        self.assertEqual(listener.get_consumers(mock_consumer_class, Mock()), ['dummy'])
        mock_consumer_class.assert_called_once_with(
            queues=[
                Queue(name='te.web99.unix-account.create',
                      exchange=Exchange('unix-account.create', 'direct'), routing_key='te.rk'),
                Queue(name='te.web99.unix-account.update',
                      exchange=Exchange('unix-account.update', 'direct'), routing_key='te.rk'),
                Queue(name='te.web99.unix-account.delete',
                      exchange=Exchange('unix-account.delete', 'direct'), routing_key='te.rk'),
                Queue(name='te.web99.website.create',
                      exchange=Exchange('website.create', 'direct'), routing_key='te.rk'),
                Queue(name='te.web99.website.update',
                      exchange=Exchange('website.update', 'direct'), routing_key='te.rk'),
                Queue(name='te.web99.website.delete',
                      exchange=Exchange('website.delete', 'direct'), routing_key='te.rk')
            ],
            callbacks=[listener.take_event, listener._register_message]
        )

    def test_register_message(self):
        listener = AMQPListener(Mock())
        msg1 = Message(body=b'json',
                       delivery_tag=1,
                       delivery_info={'exchange': 'website.create'},
                       headers={'provider': 'rc-user'})
        msg2 = Message(body=b'json',
                       delivery_tag=2,
                       delivery_info={'exchange': 'website.delete'},
                       headers={'provider': 'rc-user'})
        listener._register_message(b'json', msg1)
        msg1.delivery_info['exchange'] = 'website.update'
        listener._register_message(b'json', msg1)
        listener._register_message(b'json', msg2)
        self.assertEqual(listener._messages, {1: msg1, 2: msg2})

    def test_take_event(self):
        message = {'operationIdentity': 'testOpId',
                   'actionIdentity': 'testActId',
                   'objRef': 'http://host/path/to/resource',
                   'params': {'param': {'param': 'params'}}}
        context = Message(body=message,
                          delivery_tag=1,
                          delivery_info={'exchange': 'website.create'},
                          headers={'provider': 'rc-user'})
        new_task_queue = Mock(spec=queue.Queue)
        listener = AMQPListener(new_task_queue)
        listener.take_event(message, context)
        new_task_queue.put.assert_called_once_with(Task(
            tag=1,
            origin=AMQPListener,
            opid='testOpId',
            actid='testActId',
            res_type='website',
            action='create',
            params={'param': {'param': 'params'},
                    'objRef': 'http://host/path/to/resource',
                    'provider': 'rc-user'}
        ))

    def test_on_iteration(self):
        listener = AMQPListener(Mock())
        msg1 = Mock(spec=Message)
        msg1.delivery_tag = 1
        msg2 = Mock(spec=Message)
        msg2.delivery_tag = 2
        task1 = Mock(spec=Task)
        task1.tag = 1
        task1.state = TaskState.DONE
        task2 = Mock(spec=Task)
        task2.tag = 2
        task2.state = TaskState.FAILED
        listener._register_message(None, msg1)
        internal_queue = listener.get_processed_task_queue()
        internal_queue.put(task1)
        listener.on_iteration()
        self.assertEqual(listener._messages, {})
        msg1.ack.assert_called_once()
        msg1.requeue.assert_not_called()
        msg1.reset_mock()
        listener._register_message(None, msg1)
        listener._register_message(None, msg2)
        internal_queue.put(task2)
        listener.on_iteration()
        self.assertEqual(listener._messages, {1: msg1})
        msg1.ack.assert_not_called()
        msg1.requeue.assert_not_called()
        msg2.ack.assert_not_called()
        msg2.requeue.assert_called_once()
        task1.tag = 3
        internal_queue.put(task1)
        self.assertEqual(listener._messages, {1: msg1})
        msg1.ack.assert_not_called()
        msg1.requeue.assert_not_called()
        task1.tag = 1
        task1.state = TaskState.NEW
        internal_queue.put(task1)
        self.assertEqual(listener._messages, {1: msg1})
        msg1.ack.assert_not_called()
        msg1.requeue.assert_not_called()

    def test_stop(self):
        listener = AMQPListener(Mock())
        listener.stop()
        self.assertTrue(listener.should_stop)
