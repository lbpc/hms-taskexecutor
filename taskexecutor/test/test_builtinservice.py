import unittest
import sys
from textwrap import dedent
from pyfakefs.fake_filesystem_unittest import TestCase
from .mock_config import mock_config

sys.modules['taskexecutor.config'] = mock_config

from taskexecutor.builtinservice import LinuxUserManager


class TestLinuxUserManager(TestCase):
    def setUp(self):
        self.setUpPyfakefs()

    def test_create_group(self):
        etc_group_path = '{}/group'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        self.fs.create_file(etc_group_path, contents=dedent("""
            u185128:x:34450:
            u185130:x:29411:
        """).lstrip())
        etc_gshadow_path = '{}/gshadow'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        self.fs.create_file(etc_gshadow_path, contents=dedent(""" 
            u185128:!::
            u185130:!::
        """).lstrip())
        m = LinuxUserManager()
        m.create_group('u185131', gid=50700)
        etc_group = self.fs.get_object(etc_group_path)
        self.assertEqual(etc_group.stat_result.st_mode, 0o100644)
        self.assertEqual(etc_group.contents, dedent("""
            u185128:x:34450:
            u185130:x:29411:
            u185131:x:50700:
        """).lstrip())
        etc_gshadow = self.fs.get_object(etc_gshadow_path)
        self.assertEqual(etc_gshadow.stat_result.st_mode, 0o100640)
        self.assertEqual(etc_gshadow.contents, dedent(""" 
            u185128:!::
            u185130:!::
            u185131:!::
        """).lstrip())

    def test_create_group_existing(self):
        etc_group_path = '{}/group'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        self.fs.create_file(etc_group_path, contents=dedent("""
            u185128:x:34450:
            u185131:x:50700:
            u185130:x:29411:
        """).lstrip())
        etc_gshadow_path = '{}/gshadow'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        self.fs.create_file(etc_gshadow_path, contents=dedent(""" 
            u185128:!::
            u185131:!::
            u185130:!::
        """).lstrip())
        m = LinuxUserManager()
        m.create_group('u185131', gid=50700)
        etc_group = self.fs.get_object(etc_group_path)
        self.assertEqual(etc_group.stat_result.st_mode, 0o100644)
        self.assertEqual(etc_group.contents, dedent("""
            u185128:x:34450:
            u185131:x:50700:
            u185130:x:29411:
        """).lstrip())
        etc_gshadow = self.fs.get_object(etc_gshadow_path)
        self.assertEqual(etc_gshadow.stat_result.st_mode, 0o100640)
        self.assertEqual(etc_gshadow.contents, dedent(""" 
            u185128:!::
            u185131:!::
            u185130:!::
        """).lstrip())

    def test_create_group_first_time(self):
        etc_group_path = '{}/group'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        etc_gshadow_path = '{}/gshadow'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        m = LinuxUserManager()
        m.create_group('testgroup', gid=1000)
        etc_group = self.fs.get_object(etc_group_path)
        self.assertEqual(etc_group.stat_result.st_mode, 0o100644)
        self.assertEqual(etc_group.contents, 'testgroup:x:1000:\n')
        etc_gshadow = self.fs.get_object(etc_gshadow_path)
        self.assertEqual(etc_gshadow.stat_result.st_mode, 0o100640)
        self.assertEqual(etc_gshadow.contents, 'testgroup:!::\n')

    def test_create_group_empty_files(self):
        etc_group_path = '{}/group'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        etc_gshadow_path = '{}/gshadow'.format(mock_config.CONFIG.builtinservice.sysconf_dir)
        for each in (etc_group_path, etc_gshadow_path): self.fs.create_file(each)
        m = LinuxUserManager()
        m.create_group('testgroup', gid=1000)
        etc_group = self.fs.get_object(etc_group_path)
        self.assertEqual(etc_group.stat_result.st_mode, 0o100644)
        self.assertEqual(etc_group.contents, 'testgroup:x:1000:\n')
        etc_gshadow = self.fs.get_object(etc_gshadow_path)
        self.assertEqual(etc_gshadow.stat_result.st_mode, 0o100640)
        self.assertEqual(etc_gshadow.contents, 'testgroup:!::\n')



    def test_create_user(self):...
    def test_delete_user(self):...
    def test_set_quota(self):...
    def test_get_quota(self):...
    def test_get_cpuacct(self):...
    def test_create_authorized_keys(self):...
    def test_kill_user_processes(self):...
    def test_set_shell(self):...
    def test_set_comment(self):...
    def test_change_uid(self):...