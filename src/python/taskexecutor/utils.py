import concurrent.futures
import copy
import hashlib
import queue
import re
import os
import subprocess
import threading
import time
import traceback
from collections import namedtuple
from collections.abc import Iterable, Iterator, Mapping
from functools import partial, reduce, wraps
from itertools import product, chain
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
        return (i.args for i in self._get_workqueue_items() if i and filter_fn(i.args))


def rgetattr(obj, path, *default):
    attrs = path.split('.')
    try:
        return reduce(getattr, attrs, obj)
    except AttributeError:
        if default:
            return default[0]
        raise


def exec_command(command, shell='/bin/bash', pass_to_stdin=None, return_raw_streams=False, raise_exc=True, env=None):
    env = env or {}
    env['PATH'] = os.environ.get('PATH', '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin')
    env['SSL_CERT_FILE'] = os.environ.get('SSL_CERT_FILE', '')
    LOGGER.debug(f'Running shell command: {command}; env: {env}')
    stdin = subprocess.PIPE
    if hasattr(pass_to_stdin, 'read'):
        stdin = pass_to_stdin
    proc = subprocess.Popen(command, stdin=stdin, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            shell=True, executable=shell, env=env)
    if return_raw_streams:
        return proc.stdout, proc.stderr
    if hasattr(pass_to_stdin, 'encode'):
        stdin = pass_to_stdin.encode()
        stdout, stderr = proc.communicate(input=stdin)
    else:
        stdout, stderr = proc.communicate()
    ret_code = proc.returncode
    if ret_code != 0 and raise_exc:
        raise CommandExecutionError(f"Failed to execute command '{command}'\n"
                                    f"CODE: {ret_code}\n"
                                    f"STDOUT: {stdout.decode()}\n"
                                    f"STDERR: {stderr.decode()}")
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
        if len(parsed_line) == 10 and \
                parsed_line[1] in ("--", "+-", "-+", "++"):
            parsed_line.pop(1)
            normalized_line = [
                int(field) * 1024 if 1 < idx < 8 and idx != 4
                else int(field.strip("#").replace("-", "0"))
                for idx, field in enumerate(parsed_line)
            ]
            quota[normalized_line[0]] = {
                "block_limit": {
                    "used":  normalized_line[1],
                    "soft":  normalized_line[2],
                    "hard":  normalized_line[3],
                    "grace": normalized_line[4]
                },
                "file_limit":  {
                    "used":  normalized_line[5],
                    "soft":  normalized_line[6],
                    "hard":  normalized_line[7],
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
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        global LOCKS
        if f not in LOCKS.keys():
            LOCKS[f] = threading.RLock()
        with LOCKS[f]:
            return f(self, *args, **kwargs)

    return wrapper


def timed(f):
    @wraps(f)
    def wrapper(self, *args, **kwargs):
        start = time.time()
        f(self, *args, **kwargs)
        duration = time.time() - start
        logger = {duration < 2:      LOGGER.debug,
                  2 <= duration < 3: LOGGER.info,
                  3 <= duration:     LOGGER.warn}[True]
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
        ApiObject = namedtuple(type_name, mapping.keys())
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
    if isinstance(maybe_mapping, Mapping):
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
            dct[k] = [e.strip() for e in v.split(",") if e]
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
            dict_merge(new_dct, reduce(lambda x, y: {y: x}, reversed(key.split(".")), dct[key]),
                       overwrite=overwrite)
        if extra and all(k in new_dct.keys() for k in extra.keys()):
            dict_merge(new_dct, extra, overwrite=overwrite)
        return to_namedtuple(new_dct)
    else:
        if extra and all(k in dct.keys() for k in extra.keys()):
            dict_merge(dct, extra, overwrite=overwrite)
        return namedtuple_from_mapping(dct)


def is_namedtuple(obj):
    return (isinstance(obj, tuple) and
            callable(getattr(obj, '_asdict', None)) and
            getattr(obj, '_fields', None) is not None)


def name_variations(name):
    name = str(name)
    snake = to_snake_case(name)
    dash = to_lower_dashed(name)
    return {name, name.upper(), snake, snake.upper(), dash, dash.upper()}


def attrs_to_env(obj, sigils=True, brackets=True, exclude_names=('env',), exclude_attrs=()):
    result = {}
    templates = ['${}' if sigils else '{}']
    if brackets and sigils:
        templates.append('${{{}}}')
    elif brackets:
        templates.append('{{{}}}')
    get_value = partial(getattr, obj)
    if isinstance(obj, dict):
        names = obj.keys()
        get_value = lambda n: obj.get(n)
    elif isinstance(obj, (str, Number)):
        names = ()
    else:
        names = filter(lambda n: n not in exclude_names and not n.startswith('_'), dir(obj))
    exc = exclude_attrs + (obj,)
    for name in names:
        try:
            attr = get_value(name)
            if callable(attr) or attr in exclude_attrs: continue
            if isinstance(attr, (str, Number)):
                value = str(attr)
                result.update({t.format(n): value
                               for t, n in product(templates, name_variations(name))})
            elif isinstance(attr, (Iterable, Iterator)) and not isinstance(attr, dict) and not is_namedtuple(attr):
                if isinstance(attr, set): attr = sorted(attr)
                attr = tuple(attr)
                if not attr: continue
                if all((isinstance(e, (str, Number)) for e in attr)):
                    result.update({t.format(n): ','.join(map(str, attr))
                                   for t, n in product(templates, name_variations(name))})
                else:
                    elements = (attrs_to_env({i: e}, sigils=False, brackets=False, exclude_attrs=exc).items() for i, e
                                in enumerate(filter(None, attr)))
                    result.update(
                            {t.format(f'{n}_{k}'): v
                             for t, n, (k, v) in product(templates, name_variations(name), chain(*elements))}
                    )
            else:
                result.update({t.format(f'{n}_{k}'): v for t, n, (k, v)
                               in product(templates,
                                          name_variations(name),
                                          attrs_to_env(attr, sigils=False, brackets=False, exclude_attrs=exc).items())})
                continue
        except Exception as e:
            LOGGER.warning(f'Failed to convert {obj}.{name} to env variable: {e}')
    return result


def asdict(obj):
    if hasattr(obj, '_asdict') and callable(obj._asdict):
        return obj._asdict()
    elif isinstance(obj, dict):
        return obj
    else:
        return vars(obj)


def asdict_rec(obj):
    res = {}
    for k, v in asdict(obj).items():
        try:
            res[k] = asdict_rec(v)
        except TypeError:
            res[k] = v
        return res

