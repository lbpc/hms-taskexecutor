import http.client
import json
import random
import re
from functools import reduce
from collections import Mapping, namedtuple
from copy import deepcopy
from urllib.parse import urlencode

from taskexecutor.logger import LOGGER


class HttpClient:
    def __init__(self, addr, port):
        self._addr = addr
        self._port = port
        self._uri = None
        self._default_headers = dict()

    def __enter__(self):
        LOGGER.info("Connecting to {0}:{1}".format(self._addr, self._port))
        self._connection = http.client.HTTPConnection(
                "{0}:{1}".format(self._addr, self._port))
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()

    @property
    def uri(self):
        return self._uri

    @uri.setter
    def uri(self, value):
        self._uri = value

    @uri.deleter
    def uri(self):
        del self._uri

    @staticmethod
    def decode_response(resp_bytes):
        return resp_bytes.decode("UTF-8")

    def post(self, body, uri=None, headers=None):
        raise NotImplementedError

    def get(self, uri=None, headers=None):
        _uri = uri or self.uri
        _headers = headers or self._default_headers
        self._connection.request("GET", _uri, headers=_headers)
        return self.process_response("GET", self._connection.getresponse())

    def put(self, body, uri=None, headers=None):
        raise NotImplementedError

    def delete(self, uri=None, headers=None):
        raise NotImplementedError

    def process_response(self, method, response):
        if response.status // 100 == 5:
            raise Exception("{0} failed, HTTP server returned "
                            "{1.status} {1.reason}".format(method, response))
        return HttpClient.decode_response(response.read())


class ApiClient(HttpClient):
    def __init__(self, addr, port, service="rc"):
        super().__init__(addr, port)
        self._service = service

    def _build_resource(self, res_name, res_id):
        self.uri = "/{0}/{1}/{2}".format(self._service, res_name, res_id)
        return self

    def _build_collection(self, res_name, query=None):
        if query:
            self.uri = "/{0}/{1}?{2}".format(self._service,
                                              res_name,
                                              urlencode(query))
        else:
            self.uri = "/{0}/{1}".format(self._service, res_name)
        return self

    def process_response(self, method, response):
        if method == "GET":
            if response.status != 200:
                raise Exception("GET failed, REST server returned "
                                "{0.status} {0.reason}".format(response))
            _json_str = self.decode_response(response.read())
            if len(_json_str) == 0:
                raise Exception(
                    "GET failed, REST server returned empty response")
            _resource = ApiObjectTranslator(_json_str)
            return _resource.as_object()

    def __getattr__(self, name):
        def wrapper(res_id=None, query=None):
            if res_id:
                return self._build_resource(name, res_id)
            elif query:
                return self._build_collection(name, query)
            else:
                return self._build_collection(name)

        return wrapper


class ConfigServerClient(HttpClient):
    def __init__(self, addr, port):
        super().__init__(addr, port)
        self._extra_attrs = dict()

    @property
    def extra_attrs(self):
        return self._extra_attrs

    @extra_attrs.setter
    def extra_attrs(self, lst):
        for prop in lst:
            _attr, _value = prop.split("=")
            tree = self._extra_attrs
            for idx, k in enumerate(_attr.split(".")):
                if idx != len(_attr.split("."))-1:
                    tree = tree.setdefault(k, {})
                else:
                    tree[k] = _value

    @extra_attrs.deleter
    def extra_attrs(self):
        del self._extra_attrs

    def process_response(self, method, response):
        if method == "GET":
            if response.status != 200:
                raise Exception("GET failed, config server returned "
                                "{0.status} {0.reason}".format(response))
            _json_str = self.decode_response(response.read())
            if len(_json_str) == 0:
                raise Exception(
                        "GET failed, config server returned empty response")
            _result = ApiObjectTranslator(_json_str)
            if self.extra_attrs:
                return _result.as_object(extra_attrs=self.extra_attrs,
                                         expand_dot_separated=True,
                                         overwrite=True)
            else:
                return _result.as_object(expand_dot_separated=True)

    def get_property_sources_list(self, name, profile):
        self.uri = "/{0}/{1}".format(name, profile)
        _list = [s.source for s in self.get().propertySources]
        LOGGER.info("Got {}".format(_list))
        return _list

    def get_property_source(self, name, profile, source_name):
        self.uri = "/{0}/{1}".format(name, profile)
        _names_available = list()
        for source in self.get().propertySources:
            if source.name == source_name:
                LOGGER.info("Got {}".format(source))
                return source
            else:
                _names_available.append(source.name)
        raise KeyError("No such property source name: {0}, "
                       "available names: {1}".format(source_name,
                                                     _names_available))

class EurekaClient(HttpClient):
    def __init__(self, addr, port):
        super().__init__(addr, port)
        self._default_headers = {"Accept": "application/json"}

    def get_instances_list(self, application_name):
        self.uri = "/eureka/apps/{}".format(application_name)
        LOGGER.info("Requested application: {0}, "
                    "URI path: {1}".format(application_name, self.uri))
        _result = self.get()
        LOGGER.info("Got {}".format(_result))
        _instance_list = list()
        if _result and \
                        "application" in _result.keys() and \
                        "instance" in _result["application"].keys() and \
                _result["application"]["instance"] and \
                isinstance(_result["application"]["instance"], list) and \
                        len(_result["application"]["instance"]) > 0:
            for instance in _result["application"]["instance"]:
                if instance["status"] == "UP":
                    _instance_list.append(
                            namedtuple("EurekaService", "socket timestamp")(
                                socket = {
                                    "addr": instance["ipAddr"],
                                    "port": instance["port"]["$"]
                                },
                                timestamp = instance["leaseInfo"][
                                                     "serviceUpTimestamp"] / 1000
                            )
                    )
        return _instance_list

    def process_response(self, method, response):
        if method == "GET":
            if response.status != 200:
                LOGGER.error("GET failed, Eureka server returned "
                             "{0.status} {0.reason}".format(response))
                return None
            _json_str = self.decode_response(response.read())
            if len(_json_str) == 0:
                LOGGER.error("GET failed, "
                             "Eureka server returned empty response")
                return None
            return ApiObjectTranslator(_json_str).as_dict()

    def get_random_instance(self, application_name):
        _all_instances = self.get_instances_list(application_name)
        if _all_instances:
            return random.choice(_all_instances)
        else:
            return None


class ApiObjectTranslator:
    def __init__(self, json_string):
        self._json_string = json_string

    @staticmethod
    def dict_merge(target, *args, overwrite=False):
        if len(args) > 1:
            for obj in args:
                ApiObjectTranslator.dict_merge(target, obj, overwrite=overwrite)
            return target

        obj = args[0]
        if not isinstance(obj, dict):
            return obj
        for k, v in obj.items():
            if k in target and isinstance(target[k], dict):
                ApiObjectTranslator.dict_merge(target[k], v,
                                               overwrite=overwrite)
            elif k in target.keys() and overwrite:
                target[k] = v
            elif k not in target.keys():
                target[k] = deepcopy(v)
        return target

    @staticmethod
    def to_namedtuple(mapping):
        if isinstance(mapping, Mapping):
            for k, v in mapping.items():
                mapping[k] = ApiObjectTranslator.to_namedtuple(v)
            return ApiObjectTranslator.namedtuple_from_mapping(mapping)
        return mapping

    @staticmethod
    def namedtuple_from_mapping(mapping, name="ApiObject"):
        for k, v in mapping.items():
            if not k.isidentifier():
                mapping[re.sub('\W|^(?=\d)', '_', k)] = v
                del mapping[k]
        return namedtuple(name, mapping.keys())(**mapping)

    @staticmethod
    def cast_numeric_recursively(dct):
        for k, v in dct.items():
            if isinstance(v, dict):
                ApiObjectTranslator.cast_numeric_recursively(v)
            elif isinstance(v, str) and re.match("^[\d]+$", v):
                dct[k] = int(v)
            elif isinstance(v, str) and re.match("^[\d]?\.[\d]+$", v):
                dct[k] = float(v)
        return dct

    @staticmethod
    def object_hook(dct, extra, overwrite, expand):
        dct = ApiObjectTranslator.cast_numeric_recursively(dct)
        if expand:
            _new_dct = dict()
            for key in dct.keys():
                ApiObjectTranslator.dict_merge(_new_dct,
                                               reduce(lambda x, y: {y: x},
                                                      reversed(key.split(".")),
                                                      dct[key]),
                                               overwrite=overwrite)
            if extra and all(k in _new_dct.keys() for k in extra.keys()):
                ApiObjectTranslator.dict_merge(_new_dct, extra,
                                               overwrite=overwrite)
            return ApiObjectTranslator.to_namedtuple(_new_dct)
        else:
            if extra and all(k in dct.keys() for k in extra.keys()):
                ApiObjectTranslator.dict_merge(dct, extra, overwrite=overwrite)
            return ApiObjectTranslator.namedtuple_from_mapping(dct)

    def as_object(self, extra_attrs=None,
                  overwrite=False, expand_dot_separated=False):
        return json.loads(self._json_string,
                          object_hook=lambda d: ApiObjectTranslator.object_hook(
                              d, extra_attrs, overwrite, expand_dot_separated))

    def as_dict(self):
        return self.cast_numeric_recursively(json.loads(self._json_string))
