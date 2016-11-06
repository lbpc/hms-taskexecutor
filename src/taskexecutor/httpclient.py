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
    def __init__(self, address, port):
        self._address = address
        self._port = port
        self._uri_path = None
        self._default_headers = dict()

    def __enter__(self):
        LOGGER.debug("Connecting to {0}:{1}".format(self._address, self._port))
        self._connection = http.client.HTTPConnection(
            "{0}:{1}".format(self._address, self._port),
            timeout=30
        )
        self.authorize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()

    @property
    def uri_path(self):
        return self._uri_path or ""

    @uri_path.setter
    def uri_path(self, value):
        self._uri_path = value

    @uri_path.deleter
    def uri_path(self):
        del self._uri_path

    @staticmethod
    def decode_response(resp_bytes):
        return resp_bytes.decode("UTF-8")

    def post(self, body, uri_path=None, headers=None):
        _uri_path = uri_path or self.uri_path
        _headers = headers or self._default_headers
        self._connection.request("POST", _uri_path, body=body, headers=_headers)
        LOGGER.debug("Performing POST request by URI path {0} "
                     "with following data: '{1}'".format(_uri_path, body))
        return self.process_response("POST", self._connection.getresponse())

    def get(self, uri_path=None, headers=None):
        _uri_path = uri_path or self.uri_path
        _headers = headers or self._default_headers
        LOGGER.debug("Performing GET request by URI path {}".format(_uri_path))
        self._connection.request("GET", _uri_path, headers=_headers)
        return self.process_response("GET", self._connection.getresponse())

    def put(self, body, uri_path=None, headers=None):
        raise NotImplementedError

    def delete(self, uri_path=None, headers=None):
        raise NotImplementedError

    def authorize(self):
        LOGGER.debug("Plain HTTP connection without authorization required")

    def process_response(self, method, response):
        if response.status // 100 == 5:
            raise Exception("{0} failed, HTTP server returned "
                            "{1.status} {1.reason}".format(method, response))
        return HttpClient.decode_response(response.read())


class ApiClient(HttpClient):
    def __init__(self, address, port, user=None, password=None):
        super().__init__(address, port)
        self._default_headers = {"Content-Type": "application/json"}
        self._access_token = None
        self._user = user
        self._password = password

    def authorize(self):
        if self._user and self._password:
            post_data = urlencode({"grant_type": "password",
                                   "username": self._user,
                                   "password": self._password,
                                   "client_id": "service",
                                   "client_secret": "service_secret"})
            headers = {"X-Requested-With": "XMLHttpRequest",
                       "Content-Type": "application/x-www-form-urlencoded"}
            resp = self.post(post_data,
                             uri_path="/oauth/token",
                             headers=headers)
            self._access_token = json.loads(resp)["access_token"]
            self._default_headers.update(
                {"Authorization": "Bearer {}".format(self._access_token)}
            )
        else:
            super().authorize()

    def _build_resource(self, res_name, res_id):
        self.uri_path = "{0}/{1}/{2}".format(self.uri_path, res_name, res_id)

    def _build_collection(self, res_name, query=None):
        if query:
            self.uri_path = "{0}/{1}?{2}".format(self.uri_path,
                                                 res_name,
                                                 urlencode(query))
        else:
            self.uri_path = "{0}/{1}".format(self.uri_path, res_name)

    def process_response(self, method, response):
        self.uri_path = None
        if method == "GET":
            if response.status != 200:
                raise Exception(
                        "GET failed, REST server returned "
                        "{0.status} {0.reason} {1}".format(response,
                                                           response.read())
                )
            _json_str = self.decode_response(response.read())
            if len(_json_str) == 0:
                raise Exception(
                    "GET failed, REST server returned empty response")
            _resource = ApiObjectTranslator(_json_str)
            return _resource.as_object()
        elif method == "POST":
            if response.status // 100 != 2:
                LOGGER.error(
                        "POST failed, REST server returned "
                        "{0.status} {0.reason} {1}".format(response,
                                                           response.read())
                )
                return None
            return self.decode_response(response.read())

    def __getattr__(self, name):
        def wrapper(res_id=None, query=None):
            if res_id:
                self._build_resource(name, res_id)
            elif query:
                self._build_collection(name, query)
            else:
                self._build_collection(name)
            return self

        return wrapper


class ConfigServerClient(HttpClient):
    def __init__(self, address, port):
        super().__init__(address, port)
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
                if idx != len(_attr.split(".")) - 1:
                    tree = tree.setdefault(k, {})
                else:
                    tree[k] = _value

    @extra_attrs.deleter
    def extra_attrs(self):
        del self._extra_attrs

    def process_response(self, method, response):
        if method == "GET":
            if response.status != 200:
                raise Exception(
                        "GET failed, config server returned "
                        "{0.status} {0.reason} {1}".format(response,
                                                           response.read())
                )
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
        self.uri_path = "/{0}/{1}".format(name, profile)
        _list = [s.source for s in self.get().propertySources]
        return _list

    def get_property_source(self, name, profile, source_name):
        self.uri_path = "/{0}/{1}".format(name, profile)
        _names_available = list()
        for source in self.get().propertySources:
            if source.name == source_name:
                return source
            else:
                _names_available.append(source.name)
        raise KeyError("No such property source name: {0}, "
                       "available names: {1}".format(source_name,
                                                     _names_available))


class EurekaClient(HttpClient):
    def __init__(self, sockets_list):
        self._sockets_list = sockets_list
        self._uri_path = None
        self._default_headers = dict()
        self._default_headers = {"Accept": "application/json"}

    def __enter__(self):
        _address, _port = self._sockets_list[0]
        for tries in range(len(self._sockets_list) * 2):
            try:
                self._connection = http.client.HTTPConnection(
                        "{0}:{1}".format(_address, _port),
                        timeout=30
                )
                return self
            except:
                LOGGER.warning("Trying next Eureka server, "
                               "tries: {}".format(tries))
                self._sockets_list.append(self._sockets_list.pop(0))
        LOGGER.error("All Eureka instances appear to be down, give up")

    def get_instances_list(self, application_name):
        self.uri_path = "/eureka/apps/{}".format(application_name)
        LOGGER.debug("Requested application: {0}, "
                     "URI path: {1}".format(application_name, self.uri_path))
        _result = self.get()
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
                            namedtuple("EurekaService",
                                       "name address port timestamp")(
                                    name=instance["app"].lower(),
                                    address=instance["ipAddr"],
                                    port=instance["port"]["$"],
                                    timestamp=instance["leaseInfo"][
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
