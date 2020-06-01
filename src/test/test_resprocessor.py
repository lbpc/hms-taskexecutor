import unittest
from unittest.mock import Mock, patch, call
from itertools import permutations
from functools import partial

from .mock_config import CONFIG

CONFIG.opservice.config_templates_cache = '/nowhere/cache'

from taskexecutor.resprocessor import DatabaseProcessor
from taskexecutor.opservice import DatabaseServer


class TestDatabaseProcessor(unittest.TestCase):
    @patch('taskexecutor.resprocessor.ResProcessor._process_data')
    def test_create(self, mock_process_data):
        CONFIG.hostname = 'web99'
        CONFIG.database.default_allowed_networks = ['127.0.0.1']
        mock_service = Mock(spec=DatabaseServer)
        mock_service.normalize_addrs.return_value = ['user@127.0.0.1/255.255.255.255']
        db = Mock()
        db.name = 'b12345'
        user1 = Mock()
        user1.name = 'u12345'
        user1.allowedIPAddresses = ['172.16.100.1', '10.10.0.1']
        user2 = Mock()
        user2.name = 'u54321'
        user2.allowedIPAddresses = ['192.168.0.1']
        db.databaseUsers = [user1, user2]
        DatabaseProcessor(db, mock_service, {}).create()
        mock_service.create_database.assert_called_once_with('b12345')
        self.assertTrue(call(['172.16.100.1', '10.10.0.1', '127.0.0.1']) in mock_service.normalize_addrs.call_args_list)
        self.assertTrue(call(['192.168.0.1', '127.0.0.1']) in mock_service.normalize_addrs.call_args_list)
        self.assertEqual(mock_service.normalize_addrs.call_count, 2)
        self.assertTrue(call('b12345', 'u12345', ['user@127.0.0.1/255.255.255.255'])
                        in mock_service.allow_database_access.call_args_list)
        self.assertTrue(call('b12345', 'u54321', ['user@127.0.0.1/255.255.255.255'])
                        in mock_service.allow_database_access.call_args_list)
        self.assertEqual(mock_service.allow_database_access.call_count, 2)
        mock_process_data.assert_called_once_with('mysql://web99/b12345',
                                                  'mysql://web99/b12345',
                                                  {'name': 'b12345', 'dataType': 'database', 'dbServer': mock_service})
        # and with explicit uris
        mock_process_data.reset_mock()
        DatabaseProcessor(db, mock_service, {'datasourceUri': 'mysql://backup/01011970/b12345',
                                             'datadestinationUri': 'mysql://localhost/b12345'}).create()
        mock_process_data.assert_called_once_with('mysql://backup/01011970/b12345',
                                                  'mysql://localhost/b12345',
                                                  {'name': 'b12345', 'dataType': 'database', 'dbServer': mock_service})

    @patch('taskexecutor.resprocessor.DatabaseProcessor.update')
    @patch('taskexecutor.resprocessor.ResProcessor._process_data')
    def test_create_existing(self, mock_process_data, mock_update):
        mock_service = Mock(spec=DatabaseServer)
        processor = DatabaseProcessor(Mock(), mock_service, {})
        processor.op_resource = Mock()
        processor.create()
        mock_service.assert_not_called()
        mock_process_data.assert_not_called()
        mock_update.assert_called_once()

    @patch('taskexecutor.resprocessor.ResProcessor._process_data')
    def test_update(self, mock_process_data):
        CONFIG.hostname = 'web99'
        CONFIG.database.default_allowed_networks = ['127.0.0.1']
        mock_service = Mock(spec=DatabaseServer)
        mock_service.normalize_addrs = lambda x: x
        user1 = Mock()
        user1.name = 'u12345'
        user1.allowedIPAddresses = ['172.16.100.1', '10.10.0.1']
        # user1prime has the same name and different addresses
        user1prime = Mock()
        user1prime.name = 'u12345'
        user1prime.allowedIPAddresses = ['10.10.0.1', '8.8.8.8']
        user2 = Mock()
        user2.name = 'u23456'
        user2.allowedIPAddresses = ['192.168.0.1']
        user3 = Mock()
        user3.name = 'u34567'
        user3.allowedIPAddresses = []
        db = Mock()
        db.name = 'b12345'
        db.databaseUsers = [user1, user2]
        actual_db = Mock()
        actual_db.name = 'b12345'
        actual_db.databaseUsers = [user1prime, user3]
        processor = DatabaseProcessor(db, mock_service, {'datasourceUri': 'mysql://backup/01011970/b12345',
                                                         'datadestinationUri': 'mysql://localhost/b12345'})
        processor.op_resource = actual_db
        processor.update()
        self.assertTrue(call('b12345', 'u12345', ['172.16.100.1', '127.0.0.1'])
                        in mock_service.allow_database_access.call_args_list
                        or
                        call('b12345', 'u12345', ['127.0.0.1', '172.16.100.1'])
                        in mock_service.allow_database_access.call_args_list)
        self.assertTrue(call('b12345', 'u23456', ['127.0.0.1', '192.168.0.1'])
                        in mock_service.allow_database_access.call_args_list
                        or
                        call('b12345', 'u23456', ['192.168.0.1', '127.0.0.1'])
                        in mock_service.allow_database_access.call_args_list)
        self.assertEqual(mock_service.allow_database_access.call_count, 2)
        self.assertTrue(call('b12345', 'u12345', ['8.8.8.8']) in mock_service.deny_database_access.call_args_list)
        self.assertTrue(call('b12345', 'u34567', ['127.0.0.1']) in mock_service.deny_database_access.call_args_list)
        self.assertEqual(mock_service.deny_database_access.call_count, 2)
        mock_process_data.assert_called_once_with('mysql://backup/01011970/b12345',
                                                  'mysql://localhost/b12345',
                                                  {'name': 'b12345', 'dataType': 'database', 'dbServer': mock_service})
        # and with disabled writes
        db.writable = False
        mock_service.reset_mock()
        mock_process_data.reset_mock()
        processor = DatabaseProcessor(db, mock_service, {})
        processor.op_resource = actual_db
        processor.update()
        self.assertTrue(call('b12345', 'u12345', ['172.16.100.1', '127.0.0.1'])
                        in mock_service.allow_database_reads.call_args_list
                        or
                        call('b12345', 'u12345', ['127.0.0.1', '172.16.100.1'])
                        in mock_service.allow_database_reads.call_args_list)
        self.assertTrue(call('b12345', 'u23456', ['192.168.0.1', '127.0.0.1'])
                        in mock_service.allow_database_reads.call_args_list
                        or
                        call('b12345', 'u23456', ['127.0.0.1', '192.168.0.1'])
                        in mock_service.allow_database_reads.call_args_list)
        self.assertTrue(mock_service.deny_database_writes.call_args_list == [call('b12345', 'u12345', ['10.10.0.1'])])
        self.assertTrue(call('b12345', 'u12345', ['8.8.8.8']) in mock_service.deny_database_access.call_args_list)
        self.assertTrue(call('b12345', 'u34567', ['127.0.0.1']) in mock_service.deny_database_access.call_args_list)
        self.assertEqual(mock_service.deny_database_access.call_count, 2)
        mock_process_data.assert_called_once()
        # and now with switched off user1 and deleted user2
        mock_service.reset_mock()
        mock_process_data.reset_mock()
        db.databaseUsers = [user1, user2]
        actual_db.databaseUsers = [user1, user2, user3]
        user1.switchedOn = False
        processor = DatabaseProcessor(db, mock_service, {'delete': user2})
        processor.op_resource = actual_db
        processor.update()
        mock_service.allow_database_access.assert_not_called()
        self.assertTrue(filter(lambda c: c in mock_service.deny_database_access.call_args_list,
                               map(partial(call, 'b12345', 'u12345'),
                                   map(list, permutations(['8.8.8.8', '10.10.0.1', '127.0.0.1'])))))
        self.assertTrue(call('b12345', 'u23456', ['192.168.0.1', '127.0.0.1'])
                        in mock_service.deny_database_access.call_args_list
                        or
                        call('b12345', 'u23456', ['127.0.0.1', '192.168.0.1'])
                        in mock_service.deny_database_access.call_args_list)
        self.assertTrue(call('b12345', 'u34567', ['127.0.0.1']) in mock_service.deny_database_access.call_args_list)
        self.assertEqual(mock_service.deny_database_access.call_count, 3)


    @patch('taskexecutor.resprocessor.DatabaseProcessor.create')
    @patch('taskexecutor.resprocessor.ResProcessor._process_data')
    def test_update_non_existing(self, mock_process_data, mock_create):
        mock_service = Mock(spec=DatabaseServer)
        processor = DatabaseProcessor(Mock(), mock_service, {})
        processor.op_resource = None
        processor.update()
        mock_service.assert_not_called()
        mock_process_data.assert_not_called()
        mock_create.assert_called_once()

    @patch('taskexecutor.resprocessor.DatabaseProcessor.create')
    @patch('taskexecutor.resprocessor.ResProcessor._process_data')
    def test_update_delete_data_first(self, mock_process_data, mock_create):
        db = Mock()
        db.name = 'b12345'
        mock_service = Mock(spec=DatabaseServer)
        processor = DatabaseProcessor(db, mock_service, {'dataSourceParams': {'deleteExtraneous': True}})
        processor.op_resource = None
        processor.update()
        mock_service.assert_not_called()
        mock_service.drop_database.assert_called_once_with('b12345')
        mock_process_data.assert_not_called()
        mock_create.assert_called_once()

    def test_delete(self):
        mock_service = Mock(spec=DatabaseServer)
        db = Mock()
        db.name = 'b12345'
        db.databaseUsers = []
        user1 = Mock()
        user1.name = 'u12345'
        user1.allowedIPAddresses = ['8.8.8.8']
        user2 = Mock()
        user2.name = 'u54321'
        user2.allowedIPAddresses = ['127.0.0.1']
        actual_db = Mock()
        actual_db.name = 'b12345'
        actual_db.databaseUsers = [user1, user2]
        processor = DatabaseProcessor(db, mock_service, {})
        processor.op_resource = actual_db
        processor.delete()
        self.assertTrue(call('b12345', 'u12345', ['8.8.8.8']) in mock_service.deny_database_access.call_args_list)
        self.assertTrue(call('b12345', 'u54321', ['127.0.0.1']) in mock_service.deny_database_access.call_args_list)
        self.assertEqual(mock_service.deny_database_access.call_count, 2)
        mock_service.drop_database.assert_called_once_with('b12345')
        # and without actual db
        mock_service.reset_mock()
        processor.op_resource = None
        processor.delete()
        mock_service.assert_not_called()

