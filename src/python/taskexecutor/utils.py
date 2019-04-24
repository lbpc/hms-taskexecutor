import concurrent.futures
import time
import traceback
import re
import subprocess
import functools
import threading

from taskexecutor.logger import LOGGER

LOCKS = {}


class CommandExecutionError(Exception):
    pass


class ThreadPoolExecutorStackTraced(concurrent.futures.ThreadPoolExecutor):
    def submit(self, f, *args, **kwargs):
        return super(ThreadPoolExecutorStackTraced, self).submit(self._function_wrapper, f, *args, **kwargs)

    @staticmethod
    def _function_wrapper(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            LOGGER.error("{}EOT".format(traceback.format_exc()))
            raise e


def exec_command(command, shell="/bin/bash", pass_to_stdin=None, return_raw_streams=False, raise_exc=True):
    LOGGER.debug("Running shell command: {}".format(command))
    proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True, executable=shell)
    if return_raw_streams:
        return proc.stdout, proc.stderr
    if pass_to_stdin:
        stdin = pass_to_stdin
        for method in ("read", "encode"):
            if hasattr(stdin, method):
                stdin = getattr(stdin, method)()
        stdout, stderr = proc.communicate(input=stdin)
    else:
        stdout, stderr = proc.communicate()
    ret_code = proc.returncode
    if ret_code != 0 and raise_exc:
        raise CommandExecutionError("Failed to execute command '{}'\n"
                                    "CODE: {}\n"
                                    "STDOUT: {}"
                                    "STDERR: {}".format(command, ret_code, stdout.decode(), stderr.decode()))
    if not raise_exc:
        return ret_code, stdout, stderr
    return stdout.decode("UTF-8")


def set_apparmor_mode(mode, binary):
    LOGGER.debug("Applying {0} AppArmor mode on {1}".format(mode, binary))
    exec_command("aa-{0} {1}".format(mode, binary))


def repquota(args, shell="/bin/bash"):
    quota = dict()
    stdout = exec_command("repquota -{}".format(args), shell=shell)
    for line in stdout.split("\n"):
        parsed_line = list(filter(None, line.replace("\t", " ").split(" ")))
        if len(parsed_line) == 10 and  \
                parsed_line[1] in ("--", "+-", "-+", "++"):
            parsed_line.pop(1)
            normalized_line = [
                int(field) * 1024 if 1 < idx < 8 and idx != 4
                else int(field.strip("#").replace("-", "0"))
                for idx, field in enumerate(parsed_line)
            ]
            quota[normalized_line[0]] = {
                "block_limit": {
                    "used": normalized_line[1],
                    "soft": normalized_line[2],
                    "hard": normalized_line[3],
                    "grace": normalized_line[4]
                },
                "file_limit": {
                    "used": normalized_line[5],
                    "soft": normalized_line[6],
                    "hard": normalized_line[7],
                    "grace": normalized_line[8]
                }
            }

    return quota


def set_thread_name(name):
    threading.current_thread().name = name


def to_camel_case(name):
    return re.sub(
            r"([^A-Za-z0-9])*", "",
            (re.sub(r"([A-Za-z0-9])+", lambda m: m.group(0).capitalize(), name))
    )


def to_lower_dashed(name):
    return re.sub(
            "([a-z0-9])([A-Z])", r"\1-\2",
            re.sub("(.)([A-Z][a-z]+)", r"\1-\2", name)
    ).lower().replace("_", "-")


def to_snake_case(name):
    return re.sub(
            "([a-z0-9])([A-Z])", r"\1_\2",
            re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    ).lower()


def synchronized(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        global LOCKS
        if f not in LOCKS.keys():
            LOCKS[f] = threading.RLock()
        with LOCKS[f]:
            return f(self, *args, **kwargs)

    return wrapper

def timed(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        start = time.time()
        f(self, *args, **kwargs)
        duration = time.time() - start
        logger = {duration < 2: LOGGER.debug,
                  2 <= duration < 3: LOGGER.info,
                  3 <= duration: LOGGER.warn}[True]
        logger("{} execution took {} seconds".format(f, duration))

    return wrapper
