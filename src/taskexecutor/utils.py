import os
import re
import subprocess
from functools import wraps
from threading import RLock
from threading import current_thread
from jinja2 import Environment, FileSystemLoader
from taskexecutor.logger import LOGGER

LOCKS = {}


class ConfigFile:
    def __init__(self, abs_path, owner_uid=None, mode=None):
        self._file_path = None
        self._enabled_path = None
        self._body = None
        self._owner_uid = owner_uid
        self._mode = mode
        self.file_path = abs_path

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, value):
        self._file_path = value

    @file_path.deleter
    def file_path(self):
        del self._file_path

    @property
    def enabled_path(self):
        if not self._enabled_path and "sites-available" in self.file_path:
            return self.file_path.replace("available", "enabled")
        else:
            return self._enabled_path

    @enabled_path.setter
    def enabled_path(self, value):
        self._enabled_path = value

    @enabled_path.deleter
    def enabled_path(self):
        del self._enabled_path

    @property
    def body(self):
        if not self._body:
            self._read_file()
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    @body.deleter
    def body(self):
        del self._body

    def _read_file(self):
        with open(self._file_path, "r") as f:
            self._body = f.read()

    def _body_as_list(self):
        return self.body.split("\n")

    def write(self):
        if os.path.exists(self.file_path):
            LOGGER.info("Backing up {0} file as {0}.old".format(self.file_path))
            os.rename(self.file_path, "{}.old".format(self.file_path))
        LOGGER.info("Saving {} file".format(self.file_path))
        with open(self.file_path, "w") as f:
            f.write(self.body)
        if self._mode:
            os.chmod(self.file_path, self._mode)
        if self._owner_uid:
            os.chown(self.file_path, self._owner_uid, self._owner_uid)

    def enable(self):
        if not self.enabled_path:
            raise ValueError("enabled_path property is not set ")
        LOGGER.info("Linking {0} to {1}".format(self.file_path,
                                                self.enabled_path))
        try:
            os.symlink(self.file_path, self.enabled_path)
        except FileExistsError:
            if os.path.islink(self.enabled_path) and \
                            os.readlink(self.enabled_path) == self.file_path:
                LOGGER.info(
                        "Symlink {} already exists".format(self.enabled_path)
                )
            else:
                raise

    def disable(self):
        if not self.enabled_path:
            raise ValueError("enabled_path property is not set ")
        LOGGER.info("Unlinking {}".format(self.enabled_path))
        os.unlink(self.enabled_path)

    def has_line(self, line):
        return line in self._body_as_list()

    def get_lines(self, regex, count=-1):
        _list = self._body_as_list()
        _ret_list = list()
        for line in _list:
            if count != 0 and re.match(regex, line):
                _ret_list.append(line)
                count -= 1
        return _ret_list

    def add_line(self, line):
        LOGGER.info("Adding '{0}' to {1}".format(line, self.file_path))
        _list = self._body_as_list().append(line)
        self.body = "\n".join(_list)

    def remove_line(self, line):
        LOGGER.info("Removing '{0}' from {1}".format(line, self.file_path))
        _list = self._body_as_list().remove(line)
        self.body = "\n".join(_list)

    def replace_line(self, regex, new_line, count=1):
        _list = self._body_as_list()
        for idx, line in enumerate(_list):
            if count != 0 and re.match(regex, line):
                LOGGER.info("Replacing '{0}' by '{1}' "
                            "in {2}".format(line, new_line, self.file_path))
                del _list[idx]
                _list.insert(idx, new_line)
                count -= 1
        self.body = "\n".join(_list)

    def revert(self):
        LOGGER.warning("Reverting {0} from {0}.old, {0} will be saved as "
                       "/tmp/te_{1}".format(self.file_path,
                                            self.file_path.replace("/", "_")))
        os.rename(self.file_path,
                  "/tmp/te_{}".format(self.file_path.replace("/", "_")))
        os.rename("{}.old".format(self.file_path), self.file_path)

    def confirm(self):
        if os.path.exists("{}.old".format(self.file_path)):
            LOGGER.info("Removing {}.old".format(self.file_path))
            os.unlink("{}.old".format(self.file_path))

    def save(self):
        self.write()
        self.confirm()

    def delete(self):
        LOGGER.info("Deleting {} file".format(self.file_path))
        os.unlink(self.file_path)
        del self.body


def exec_command(command):
    LOGGER.info("Running shell command: {}".format(command))
    with subprocess.Popen(command,
                          stderr=subprocess.PIPE,
                          shell=True,
                          executable="/bin/bash") as proc:
        stderr = proc.stderr.read()
        proc.communicate()
        ret_code = proc.returncode
    if ret_code != 0:
        LOGGER.error(
                "Command '{0}' returned {1} code".format(command, ret_code))
        if stderr:
            LOGGER.error("STDERR: {}".format(stderr.decode("UTF-8")))
        raise Exception("Failed to execute command '{}'".format(command))


def set_apparmor_mode(mode, binary):
    LOGGER.info("Applying {0} AppArmor mode on {1}".format(mode, binary))
    exec_command("aa-{0} {1}".format(mode, binary))


def render_template(template_name, **kwargs):
    template_env = Environment(
            loader=FileSystemLoader("./templates"),
            lstrip_blocks=True,
            trim_blocks=True)
    template = template_env.get_template(template_name)

    return template.render(**kwargs)


def set_thread_name(name):
    current_thread().name = name


def synchronized(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        if f not in LOCKS.keys():
            LOCKS[f] = RLock()
        with LOCKS[f]:
            return f(self, *args, **kwargs)

    return wrapper
