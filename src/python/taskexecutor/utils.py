import concurrent.futures
import collections
import copy
import hashlib
import time
import traceback
import re
import subprocess
import functools
import threading
import queue
from numbers import Number

from taskexecutor.logger import LOGGER

LOCKS = {}
TYPES_MAPPING = {}


class CommandExecutionError(Exception):
    pass


class ThreadPoolExecutorStackTraced(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, max_workers):
        self._name = None
        super().__init__(max_workers=max_workers)

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, name):
        self._name = name

    @staticmethod
    def _function_wrapper(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            LOGGER.error("{}EOT".format(traceback.format_exc()))
            raise e

    def submit(self, f, *args, **kwargs):
        return super(ThreadPoolExecutorStackTraced, self).submit(self._function_wrapper, f, *args, **kwargs)

    def _get_workqueue_items(self):
        while True:
            try:
                yield self._work_queue.get(block=False)
            except queue.Empty:
                break

    def dump_work_queue(self, filter_fn):
        return (i.args for i in self._get_workqueue_items() if filter_fn(i.args))


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
        return ret_code, stdout.decode(), stderr.decode()
    return stdout.decode()


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


def namedtuple_reduce(instance):
    dct = instance._asdict()
    return namedtuple_from_mapping, (dct, instance.__class__.__name__)


def cleanup_types_mapping():
    this_hour = time.time() // 3600 * 3600
    for each in [k for k in TYPES_MAPPING.keys() if int(k[-10:]) < this_hour]:
        del TYPES_MAPPING[each]


def namedtuple_from_mapping(mapping, type_name="Something"):
    cleanup_types_mapping()
    class_key = hashlib.sha1(
            (mapping.get("@type", "") + " ".join([str(k) for k in mapping.keys()])).encode()
    ).hexdigest() + str(int(time.time()) // 3600 * 3600)
    if "@type" in mapping.keys():
        type_name = mapping.pop("@type")
    for k, v in mapping.items():
        if not k.isidentifier():
            mapping[re.sub('\W|^\d', '_', k).lstrip('_')] = v
            del mapping[k]
    ApiObject = TYPES_MAPPING.get(class_key)
    if not ApiObject:
        ApiObject = collections.namedtuple(type_name, mapping.keys())
        ApiObject.__reduce__ = namedtuple_reduce
        TYPES_MAPPING[class_key] = ApiObject
    return ApiObject(**mapping)


def dict_merge(target, *args, overwrite=False):
    if len(args) > 1:
        for obj in args:
            dict_merge(target, obj, overwrite=overwrite)
        return target

    obj = args[0]
    if not isinstance(obj, dict):
        return obj
    for k, v in obj.items():
        if k in target and isinstance(target[k], dict):
            dict_merge(target[k], v, overwrite=overwrite)
        elif k in target.keys() and overwrite:
            target[k] = v
        elif k not in target.keys():
            target[k] = copy.deepcopy(v)
    return target


def to_namedtuple(maybe_mapping):
    if isinstance(maybe_mapping, collections.Mapping):
        for k, v in maybe_mapping.items():
            maybe_mapping[k] = to_namedtuple(v)
        return namedtuple_from_mapping(maybe_mapping)
    return maybe_mapping


def cast_to_numeric_recursively(dct):
    for k, v in dct.items():
        if isinstance(v, dict):
            cast_to_numeric_recursively(v)
        elif isinstance(v, str) and re.match("^[\d]+$", v):
            dct[k] = int(v)
        elif isinstance(v, str) and re.match("^[\d]?\.[\d]+$", v):
            dct[k] = float(v)
    return dct


def comma_separated_to_list(dct):
    for k, v in dct.items():
        if isinstance(v, dict):
            comma_separated_to_list(v)
        elif isinstance(v, str) and "," in v:
            dct[k] = [e.strip() for e in v.split(",")]
    return dct


def object_hook(dct, extra, overwrite, expand, comma, numcast):
    dct = cast_to_numeric_recursively(dct) if numcast else dct
    if extra and numcast:
        extra = cast_to_numeric_recursively(extra)
    if comma:
        dct = comma_separated_to_list(dct)
    if expand:
        new_dct = dict()
        for key in dct.keys():
            dict_merge(new_dct, functools.reduce(lambda x, y: {y: x}, reversed(key.split(".")), dct[key]),
                       overwrite=overwrite)
        if extra and all(k in new_dct.keys() for k in extra.keys()):
            dict_merge(new_dct, extra, overwrite=overwrite)
        return to_namedtuple(new_dct)
    else:
        if extra and all(k in dct.keys() for k in extra.keys()):
            dict_merge(dct, extra, overwrite=overwrite)
        return namedtuple_from_mapping(dct)


def attrs_to_env(obj):
    res = {}
    for name in dir(obj):
        possible_names = (name,
                          name.upper(),
                          to_snake_case(name),
                          to_snake_case(name).upper(),
                          to_lower_dashed(name),
                          to_lower_dashed(name).upper())
        attr = getattr(obj, name)
        if not name.startswith("_") and not callable(attr) and name != "env":
            for n in possible_names:
                res["${}".format(n)] = str(attr)
                res["${{{}}}".format(n)] = str(attr)
                if not isinstance(attr, Number):
                    for k, v in attrs_to_env(attr).items():
                        k = k.lstrip("$").strip("{}")
                        for n in possible_names:
                            res["${}_{}".format(n, k)] = str(v)
                            res["${{{}_{}}}".format(n, k)] = str(v)
    return res