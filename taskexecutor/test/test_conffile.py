import unittest
from unittest.mock import Mock, PropertyMock, patch, mock_open, call
from collections.abc import Callable
from textwrap import dedent
import os
import sys
import jinja2.environment
from .mock_config import mock_config

sys.modules['taskexecutor.config'] = mock_config

from taskexecutor.conffile import *
from taskexecutor.conffile import PropertyValidationError, NoSuchLine


class TestConfigFile(unittest.TestCase):
    @patch('os.path', autospec=os.path)
    def test_exists(self, mock_path):
        mock_path.abspath = Mock(return_value='file.conf')
        config = ConfigFile('file.conf', 1000, 0o755)
        config.exists()
        mock_path.abspath.assert_called_once_with('file.conf')
        mock_path.exists.assert_called_once_with('file.conf')

    @patch('os.path.exists')
    def test_body_not_exist(self, mock_exists):
        mock_exists.return_value = False
        config = ConfigFile('file.conf', 1000, 0o755)
        self.assertEqual(config.body, '')
        config.body = 'qwerty'
        self.assertEqual(config.body, 'qwerty')
        del config.body
        self.assertEqual(config.body, '')

    @patch('os.path.exists')
    @patch('builtins.open', mock_open(read_data='qwerty'))
    def test_body_exist(self, mock_exists):
        mock_exists.return_value = True
        config = ConfigFile('file.conf', 1000, 0o755)
        self.assertEqual(config.body, 'qwerty')
        del config.body
        self.assertEqual(config.body, 'qwerty')
        config.body = 'asdf'
        self.assertEqual(config.body, 'asdf')

    @patch('os.makedirs')
    def test_backup_file_path(self, mock_makedirs):
        config = ConfigFile('/opt/etc/passwd', 0, 0o644)
        tmp_file = '{}/opt/etc/passwd'.format(mock_config.CONFIG.conffile.tmp_dir)
        tmp_dir = '{}/opt/etc'.format(mock_config.CONFIG.conffile.tmp_dir)
        self.assertEqual(config._backup_file_path, tmp_file)
        mock_makedirs.assert_called_once_with(tmp_dir, exist_ok=True)

    @patch('os.chown')
    @patch('os.chmod')
    @patch('os.makedirs')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_write_new(self, mock_open, mock_exists, mock_makedirs, mock_chmod, mock_chown):
        mock_exists.return_value = False
        config = ConfigFile('/opt/etc/passwd', 0, 0o644)
        config.body = "root:x:0:0:root:/root:/bin/bash\n"
        config.write()
        mock_open().write.assert_called_once_with("root:x:0:0:root:/root:/bin/bash\n")
        mock_makedirs.assert_called_once_with('/opt/etc')
        mock_chmod.assert_called_once_with('/opt/etc/passwd', 0o644)
        mock_chown.assert_called_once_with('/opt/etc/passwd', 0, 0)

    @patch('taskexecutor.conffile.ConfigFile._backup_file_path', new_callable=PropertyMock)
    @patch('os.chown')
    @patch('os.chmod')
    @patch('shutil.move')
    @patch('os.path.exists')
    @patch('builtins.open', new_callable=mock_open)
    def test_write_existing(self, mock_open, mock_exists, mock_move, mock_chmod, mock_chown, mock_backup):
        mock_exists.return_value = True
        mock_backup.return_value = '/tmp/opt/etc/passwd'
        config = ConfigFile('/opt/etc/passwd', 0, 0o644)
        config.body = "root:x:0:0:root:/root:/bin/bash\n"
        config.write()
        mock_move.assert_called_once_with('/opt/etc/passwd', '/tmp/opt/etc/passwd')
        mock_open().write.assert_called_once_with("root:x:0:0:root:/root:/bin/bash\n")
        mock_chmod.assert_called_once_with('/opt/etc/passwd', 0o644)
        mock_chown.assert_called_once_with('/opt/etc/passwd', 0, 0)


    @patch('taskexecutor.conffile.ConfigFile._backup_file_path', new_callable=PropertyMock)
    @patch('os.makedirs')
    @patch('shutil.move')
    @patch('os.path.exists')
    def test_revert(self, mock_exists, mock_move, mock_makedirs, mock_backup):
        mock_exists.return_value = True
        bad_file = '{}/_opt_etc_passwd'.format(mock_config.CONFIG.conffile.bad_confs_dir)
        mock_backup.return_value = '/tmp/opt/etc/passwd'
        config = ConfigFile('/opt/etc/passwd', 0, 0o644)
        config.revert()
        mock_makedirs.assert_called_once_with(mock_config.CONFIG.conffile.bad_confs_dir, exist_ok=True)
        mock_move.assert_has_calls((call('/opt/etc/passwd', bad_file),
                                    call('/tmp/opt/etc/passwd', '/opt/etc/passwd')))

    @patch('taskexecutor.conffile.ConfigFile._backup_file_path', new_callable=PropertyMock)
    @patch('shutil.move')
    @patch('os.path.exists')
    def test_revert_no_backup(self, mock_exists, mock_move, mock_backup):
        mock_exists.return_value = False
        bad_file = '{}/_opt_etc_passwd'.format(mock_config.CONFIG.conffile.bad_confs_dir)
        mock_backup.return_value = '/tmp/opt/etc/passwd'
        config = ConfigFile('/opt/etc/passwd', 0, 0o644)
        config.revert()
        mock_move.assert_called_once_with('/opt/etc/passwd', bad_file)

    @patch('taskexecutor.conffile.ConfigFile._backup_file_path', new_callable=PropertyMock)
    @patch('os.unlink')
    @patch('os.path.exists')
    def test_confirm(self, mock_exists, mock_unlink, mock_backup):
        mock_backup.return_value = 'backup.conf'
        mock_exists.return_value = True
        config = ConfigFile('file.conf', 0, 0o644)
        config.confirm()
        mock_exists.assert_called_once_with('backup.conf')
        mock_unlink.assert_called_once_with('backup.conf')

    @patch('taskexecutor.conffile.ConfigFile.write')
    @patch('taskexecutor.conffile.ConfigFile.confirm')
    def test_save(self, mock_confirm, mock_write):
        config = ConfigFile('file.conf', 0, 0o644)
        config.save()
        mock_write.assert_called_once_with()
        mock_confirm.assert_called_once_with()

    @patch('os.unlink')
    @patch('os.path.exists')
    def test_delete(self, mock_exists, mock_unlink):
        mock_exists.return_value = True
        config = ConfigFile('/opt/etc/passwd', 0, 0o644)
        config.body = 'qwerty'
        config.delete()
        mock_unlink.assert_called_once_with('/opt/etc/passwd')
        mock_exists.return_value = False
        self.assertEqual(config.body, '')

    @patch('os.path.exists')
    def test_delete_not_exist(self, mock_exists):
        mock_exists.return_value = False
        config = ConfigFile('/opt/etc/passwd', 0, 0o644)
        config.body = 'qwerty'
        config.delete()
        self.assertEqual(config.body, '')


class TestTemplatedConfigFile(unittest.TestCase):
    def test_setup_jinja2_env(self):
        env = TemplatedConfigFile._setup_jinja2_env()
        self.assertIsInstance(env, jinja2.environment.Environment)

        f = env.filters.get('path_join')
        self.assertIsInstance(f, Callable)
        self.assertEqual(f(('foo', 'bar', 'baz')), 'foo/bar/baz')
        self.assertEqual(f(('/foo', 'bar', 'baz')), '/foo/bar/baz')
        self.assertEqual(f(('foo', 'bar', 'baz/')), 'foo/bar/baz/')
        self.assertEqual(f(('foo', '/bar', 'baz')), '/bar/baz')
        self.assertEqual(f(('/foo', 'bar', '/baz')), '/baz')

        f = env.filters.get('normpath')
        self.assertIsInstance(f, Callable)
        self.assertEqual(f('/foo/bar/baz/'), '/foo/bar/baz')
        self.assertEqual(f('/foo//bar///baz////'), '/foo/bar/baz')
        self.assertEqual(f('/////foo/bar/baz'), '/foo/bar/baz')
        self.assertEqual(f('//foo/bar/baz'), '//foo/bar/baz')
        self.assertEqual(f('/foo/bar/..'), '/foo')
        self.assertEqual(f('/foo/bar/../..'), '/')
        self.assertEqual(f('/foo/bar/../../../../..'), '/')
        self.assertEqual(f('/foo/../../../../../bar'), '/bar')
        self.assertEqual(f('foo/../../../bar'), '../../bar')
        self.assertEqual(f('foo/../..'), '..')
        self.assertEqual(f('..'), '..')
        self.assertEqual(f('../..'), '../..')
        self.assertEqual(f('/путь с пробелами/и/	табуляцией'), '/путь с пробелами/и/\tтабуляцией')

        f = env.filters.get('punycode')
        self.assertIsInstance(f, Callable)
        self.assertEqual(f('домен.рф'), 'xn--d1acufc.xn--p1ai')
        self.assertEqual(f('example.com'), 'example.com')

        f = env.filters.get('dirname')
        self.assertIsInstance(f, Callable)
        self.assertEqual(f('/path/to/file'), '/path/to')
        self.assertEqual(f('path/to/file'), 'path/to')
        self.assertEqual(f('file'), '')
        self.assertEqual(f('./file'), '.')
        self.assertEqual(f('../file'), '..')
        self.assertEqual(f('.'), '')
        self.assertEqual(f('..'), '')

    def test_render_template(self):
        config = TemplatedConfigFile('file.conf', 0, 0o777)
        config.template = '{{ spam }}{% for each in eggs %} {{ each }}{% endfor %}{{ nothing }}'
        config.render_template(spam=-1, eggs=range(2), parrot=3)
        self.assertEqual(config.body, '-1 0 1')

    def test_render_template_unset(self):
        config = TemplatedConfigFile('file.conf', 0, 0o777)
        self.assertRaises(PropertyValidationError, config.render_template)
        self.assertRaises(PropertyValidationError, config.render_template, some='useless', keyword='args')


class TestLineBasedConfigFile(unittest.TestCase):
    def setUp(self):
        self.config = LineBasedConfigFile('file.conf', 0, 0o777)

    def test_has_line(self):
        self.config.body = dedent("""
            mary
            had
            a
            little
            lamb
        """).lstrip()
        self.assertTrue(self.config.has_line('mary'))
        self.assertTrue(self.config.has_line('a'))
        self.assertFalse(self.config.has_line('fox'))
        self.assertFalse(self.config.has_line('Mary'))

    def test_get_lines(self):
        self.config.body = dedent("""
            Rented a tent, a tent, a tent;
            Rented a tent, a tent, a tent.
            Rented a tent!
            Rented a tent!
            Rented a, rented a tent.
        """).lstrip()
        self.assertEqual(self.config.get_lines('Rented a tent'), ['Rented a tent, a tent, a tent;',
                                                                  'Rented a tent, a tent, a tent.',
                                                                  'Rented a tent!',
                                                                  'Rented a tent!'])
        self.assertEqual(self.config.get_lines('.*tent', count=3), ['Rented a tent, a tent, a tent;',
                                                                    'Rented a tent, a tent, a tent.',
                                                                    'Rented a tent!'])
        self.assertEqual(self.config.get_lines('.*tent', count=0), [])
        self.assertEqual(self.config.get_lines('.*tent', count=-1), ['Rented a tent, a tent, a tent;',
                                                                     'Rented a tent, a tent, a tent.',
                                                                     'Rented a tent!',
                                                                     'Rented a tent!',
                                                                     'Rented a, rented a tent.'])
        self.assertEqual(self.config.get_lines('.*;', count=9), ['Rented a tent, a tent, a tent;'])
        self.assertEqual(self.config.get_lines(r'^Rented\s{1}(a tent(,|;|.)\s*){3}'),
                                               ['Rented a tent, a tent, a tent;',
                                                'Rented a tent, a tent, a tent.'])

    def test_add_line(self):
        self.config.body = '1'
        self.config.add_line('2')
        self.assertEqual(self.config.body, '1\n2')
        self.assertNotEquals(self.config.body, '1\n2\n')
        self.config.add_line('\n')
        self.assertEqual(self.config.body, '1\n2\n\n')

    def test_remove_line(self):
        self.config.body = dedent("""
            spam
            bacon
            spam
            spam
            egg
        """).lstrip()
        self.config.remove_line('egg')
        self.assertEqual(self.config.body, dedent("""
            spam
            bacon
            spam
            spam
        """).lstrip())
        self.config.remove_line('spam')
        self.assertEqual(self.config.body, dedent("""
            bacon
            spam
            spam
        """).lstrip())
        self.config.remove_line('bacon\n')
        self.assertEqual(self.config.body, dedent("""
            spam
            spam
        """).lstrip())
        self.assertRaises(NoSuchLine, self.config.remove_line, 'ham')

    def test_replace_line(self):
        self.config.body = dedent("""
            sausage
            bacon
            baked beans
            egg
            brandy
        """).lstrip()
        self.config.replace_line('egg', 'spam')
        self.assertEqual(self.config.body, dedent("""
            sausage
            bacon
            baked beans
            spam
            brandy
        """).lstrip())
        self.config.replace_line('ba.+', 'spam')
        self.assertEqual(self.config.body, dedent("""
            sausage
            spam
            baked beans
            spam
            brandy
        """).lstrip())
        self.config.replace_line(r'.*', 'spam')
        self.assertEqual(self.config.body, dedent("""
            spam
            spam
            baked beans
            spam
            brandy
        """).lstrip())
        self.config.replace_line('baked beans\n',
                                 'lobster thermidor aux crevettes with a Mornay sauce, garnished with truffle paté')
        self.assertEqual(self.config.body, dedent("""
            spam
            spam
            lobster thermidor aux crevettes with a Mornay sauce, garnished with truffle paté
            spam
            brandy
        """).lstrip())
        self.config.replace_line(r'.*', 'spam', count=4)
        self.assertEqual(self.config.body, dedent("""
            spam
            spam
            spam
            spam
            brandy
        """).lstrip())
        self.config.replace_line(r'.+', 'spam', count=-1)
        self.assertEqual(self.config.body, dedent("""
            spam
            spam
            spam
            spam
            spam
        """).lstrip())
