import re
import subprocess
import functools
import threading
from taskexecutor.logger import LOGGER

LOCKS = {}


class CommandExecutionError(Exception):
    pass


def exec_command(command, shell="/bin/bash", pass_to_stdin=None):
    LOGGER.info("Running shell command: {}".format(command))
    with subprocess.Popen(command,
                          stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE,
                          shell=True,
                          executable=shell) as proc:
        if pass_to_stdin:
            stdout, stderr = proc.communicate(input=pass_to_stdin.encode("UTF-8"))
        else:
            stdout, stderr = proc.communicate()
        ret_code = proc.returncode
    if ret_code != 0:
        LOGGER.error(
                "Command '{0}' returned {1} code".format(command, ret_code))
        if stderr:
            LOGGER.error("STDERR: {}".format(stderr.decode("UTF-8")))
        raise CommandExecutionError("Failed to execute command '{}'".format(command))

    return stdout.decode("UTF-8")


def set_apparmor_mode(mode, binary):
    LOGGER.info("Applying {0} AppArmor mode on {1}".format(mode, binary))
    exec_command("aa-{0} {1}".format(mode, binary))


def repquota(args, shell="/bin/bash"):
    quota = dict()
    stdout = exec_command("repquota -{}".format(args), shell=shell)
    for line in stdout.split("\n"):
        parsed_line = list(filter(None, line.split(" ")))
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


def to_lower_dashed(name):
    return re.sub(
            "([a-z0-9])([A-Z])", r"\1-\2",
            re.sub("(.)([A-Z][a-z]+)", r"\1-\2", name)
    ).lower().replace("_", "-")


def synchronized(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        if f not in LOCKS.keys():
            LOCKS[f] = threading.RLock()
        with LOCKS[f]:
            return f(self, *args, **kwargs)

    return wrapper
