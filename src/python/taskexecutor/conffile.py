import os
import re
import shutil
import tempfile
import jinja2
import urllib.parse
from itertools import islice

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.utils import rgetattr, asdict

__all__ = ['ConfigFile', 'LineBasedConfigFile', 'TemplatedConfigFile']


class PropertyValidationError(Exception):
    pass


class NoSuchLine(Exception):
    pass


class TooBroadCondition(Exception):
    pass


class ConfigFile:
    def __init__(self, file_path, owner_uid, mode):
        self._tmp_dir = rgetattr(CONFIG, 'conffile.tmp_dir', tempfile.gettempdir())
        self._bad_confs_dir = rgetattr(CONFIG, 'conffile.bad_confs_dir',
                                       os.path.join(tempfile.gettempdir(), 'te-bad-confs'))
        self._body = ''
        self._owner_uid = owner_uid
        self._mode = mode
        self.file_path = os.path.abspath(file_path)

    @property
    def tmp_dir(self):
        return self._tmp_dir

    @property
    def bad_confs_dir(self):
        return self._bad_confs_dir

    @property
    def body(self):
        if not self._body and self.exists:
            LOGGER.debug(f'Reading {self.file_path} contents')
            with open(self.file_path, 'r') as f: self._body = f.read()
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    @body.deleter
    def body(self):
        self._body = ''

    @property
    def exists(self):
        return os.path.exists(self.file_path)

    @property
    def _backup_file_path(self):
        backup_path = os.path.join(self.tmp_dir, self.file_path.lstrip('/'))
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        return backup_path

    def write(self):
        dir_path = os.path.dirname(self.file_path)
        if dir_path and not os.path.exists(dir_path):
            LOGGER.warning(f'There is no {dir_path} found, creating')
            os.makedirs(dir_path)
        if os.path.exists(self.file_path):
            LOGGER.debug(f'Backing up {self.file_path} file as {self._backup_file_path}')
            shutil.move(self.file_path, self._backup_file_path)
        LOGGER.debug(f'Saving {self.file_path} file')
        with open(self.file_path, 'w') as f:
            f.write(self.body)
        if self._mode: os.chmod(self.file_path, self._mode)
        if self._owner_uid is not None: os.chown(self.file_path, self._owner_uid, self._owner_uid)

    def revert(self):
        bad_conf_path = os.path.join(self.bad_confs_dir, self.file_path.replace('/', '_'))
        if os.path.exists(self._backup_file_path):
            LOGGER.warning(f'Reverting {self.file_path} from {self._backup_file_path}, '
                           f'{self.file_path} will be saved as {bad_conf_path}')
            os.makedirs(self.bad_confs_dir, exist_ok=True)
            shutil.move(self.file_path, bad_conf_path)
            shutil.move(self._backup_file_path, self.file_path)
        else:
            LOGGER.warning(f'No backed up version found, moving {self.file_path} to {bad_conf_path}')
            shutil.move(self.file_path, bad_conf_path)

    def confirm(self):
        if os.path.exists(self._backup_file_path):
            LOGGER.debug(f'Removing {self._backup_file_path}')
            try:
                os.unlink(self._backup_file_path)
            except FileNotFoundError as e:
                LOGGER.warning(f'Could not delete file, ERROR: {e}')

    def save(self):
        self.write()
        self.confirm()

    def delete(self):
        LOGGER.debug(f'Deleting {self.file_path} file')
        if os.path.exists(self.file_path):
            os.unlink(self.file_path)
        else:
            LOGGER.warning(f"{self.file_path} doesn't exists")
        del self.body


class TemplatedConfigFile(ConfigFile):
    def __init__(self, file_path, owner_uid, mode):
        super().__init__(file_path, owner_uid, mode)
        self.template = None

    @staticmethod
    def _setup_jinja2_env():
        jinja2_env = jinja2.Environment(trim_blocks=True, lstrip_blocks=True, extensions=['jinja2.ext.do'])
        jinja2_env.filters['path_join'] = lambda paths: os.path.join(*paths)
        jinja2_env.filters['punycode'] = lambda domain: domain.encode('idna').decode()
        jinja2_env.filters['normpath'] = lambda path: os.path.normpath(path)
        jinja2_env.filters['dirname'] = lambda path: os.path.dirname(path)
        jinja2_env.filters['items'] = lambda obj: asdict(obj).items()
        jinja2_env.filters['urlencode'] = lambda url: urllib.parse.quote_plus(url)
        return jinja2_env

    def render_template(self, **kwargs):
        if not self.template:
            raise PropertyValidationError('Template is not set')
        jinja2_env = self._setup_jinja2_env()
        self.body = jinja2_env.from_string(self.template).render(**kwargs)


class LineBasedConfigFile(ConfigFile):
    def __init__(self, file_path, owner_uid, mode):
        super().__init__(file_path, owner_uid, mode)

    def has_line(self, line):
        return line in self.body.split('\n')

    def get_lines(self, regex, count=-1):
        pattern = re.compile(regex)
        matched = (l for l in self.body.split('\n') if pattern.match(l))
        if count < 0: return list(matched)
        return list(islice(matched, count))

    def get_line(self, regex, lenient=False, default=None):
        matched = self.get_lines(regex)
        if not any((matched, default)):
            raise NoSuchLine(f'Not a single line matched the regular expression /{regex}/')
        elif len(matched) > 1 and not lenient:
            raise TooBroadCondition('More than one line matched the regular expression /{}/:\n'
                                    '{}'.format(regex, '\n'.join(matched)))
        else:
            return next(iter(matched), default)

    def add_line(self, line=''):
        LOGGER.debug(f"Adding '{line}' to {self.file_path}")
        if line.endswith('\n'): line = line[::-1].replace('\n', '', 1)[::-1]
        list = self.body.split('\n')
        if list and not list[-1] and line: list.pop(-1)
        list.append(line)
        self.body = '\n'.join(list)

    def remove_line(self, line):
        LOGGER.debug(f"Removing '{line}' from {self.file_path}")
        list = self.body.split('\n')
        try:
            list.remove(line.rstrip('\n'))
        except ValueError:
            raise NoSuchLine(line)
        self.body = '\n'.join(list)

    def replace_line(self, regex, new_line, count=1):
        list = self.body.split('\n')
        for idx, line in enumerate(list):
            if count != 0 and (re.match(regex, line) or re.match(regex, line + '\n')):
                LOGGER.debug(f"Replacing '{line}' by '{new_line}' in {self.file_path}")
                del list[idx]
                list.insert(idx, new_line)
                count -= 1
        self.body = '\n'.join(list)
