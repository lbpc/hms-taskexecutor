import subprocess
import pymysql
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from threading import RLock
from threading import current_thread
from traceback import format_exc
from jinja2 import Environment, FileSystemLoader
from taskexecutor.logger import LOGGER

LOCKS = {}


class ThreadPoolExecutorStackTraced(ThreadPoolExecutor):
    def submit(self, f, *args, **kwargs):
        return super(ThreadPoolExecutorStackTraced, self).submit(
                self._function_wrapper, f, *args, **kwargs)

    @staticmethod
    def _function_wrapper(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            raise Exception(format_exc())


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
