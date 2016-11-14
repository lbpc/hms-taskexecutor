from abc import ABCMeta, abstractmethod
import http.client
import json
import re
import time
from base64 import b64decode
from functools import reduce
from collections import Mapping, namedtuple
from copy import deepcopy
from urllib.parse import urlencode
from taskexecutor.logger import LOGGER


class HttpsClient(metaclass=ABCMeta):
    def __init__(self, host, port, user, password):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._uri_path = None

    def __enter__(self):
        LOGGER.debug("Connecting to {0}:{1}".format(self._host, self._port))
        self._connection = http.client.HTTPSConnection(
            "{0}:{1}".format(self._host, self._port),
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

    @abstractmethod
    def authorize(self):
        pass

    @abstractmethod
    def post(self, body, uri_path=None, headers=None):
        pass

    @abstractmethod
    def get(self, uri_path=None, headers=None):
        pass

    @abstractmethod
    def put(self, body, uri_path=None, headers=None):
        pass

    @abstractmethod
    def delete(self, uri_path=None, headers=None):
        pass


class ApiClient(HttpsClient):
    _headers = {"Content-Type": "application/json",
                "Accept": "application/json"}
    _access_token = None
    _expires_at = 0

    def _build_resource(self, res_name, res_id):
        self.uri_path = "{0}/{1}/{2}".format(self.uri_path, res_name, res_id)

    def _build_collection(self, res_name, query=None):
        if query:
            self.uri_path = "{0}/{1}?{2}".format(self.uri_path,
                                                 res_name,
                                                 urlencode(query))
        else:
            self.uri_path = "{0}/{1}".format(self.uri_path, res_name)

    def authorize(self):
        if not self._access_token or time.time() > ApiClient._expires_at:
            post_data = urlencode({"grant_type": "password",
                                   "username": self._user,
                                   "password": self._password,
                                   "client_id": "service",
                                   "client_secret": "service_secret"})
            headers = {"X-Requested-With": "XMLHttpRequest",
                       "Content-Type": "application/x-www-form-urlencoded"}
            resp = json.loads(self.post(post_data,
                                        uri_path="/oauth/token",
                                        headers=headers))
            ApiClient._access_token = resp["access_token"]
            ApiClient._expires_at = resp["expires_in"] + time.time()
            ApiClient._headers.update(
                    {"Authorization": "Bearer {}".format(self._access_token)}
            )

    def post(self, body, uri_path=None, headers=None):
        uri_path = uri_path or self.uri_path
        headers = headers or ApiClient._headers
        self._connection.request("POST", uri_path, body=body, headers=headers)
        LOGGER.debug("Performing POST request by URI path {0} "
                     "with following data: '{1}'".format(uri_path, body))
        response = self._connection.getresponse()
        self.uri_path = None
        if response.status // 100 != 2:
            LOGGER.error(
                    "POST failed, API gateway returned "
                    "{0.status} {0.reason} {1}".format(response,
                                                       response.read())
            )
            return None
        return self.decode_response(response.read())

    def get(self, uri_path=None, headers=None):
        uri_path = uri_path or self.uri_path
        headers = headers or ApiClient._headers
        LOGGER.debug("Performing GET request by URI path {}".format(uri_path))
        self._connection.request("GET", uri_path, headers=headers)
        response = self._connection.getresponse()
        self.uri_path = None
        if response.status != 200:
            raise Exception(
                    "GET failed, API gateway returned "
                    "{0.status} {0.reason} {1}".format(response,
                                                       response.read())
            )
        json_str = self.decode_response(response.read())
        if len(json_str) == 0:
            raise Exception(
                "GET failed, API gateway returned empty response")
        resource = ApiObjectMapper(json_str)
        return resource.as_object()

    def put(self, body, uri_path=None, headers=None):
        raise NotImplementedError

    def delete(self, uri_path=None, headers=None):
        raise NotImplementedError

    def __getattr__(self, name):
        name = re.sub("([a-z0-9])([A-Z])", r"\1-\2",
                      re.sub("(.)([A-Z][a-z]+)", r"\1-\2", name)).lower()
        def constructor(res_id=None, query=None):
            if res_id:
                self._build_resource(name, res_id)
            elif query:
                self._build_collection(name, query)
            else:
                self._build_collection(name)
            return self

        return constructor


class ConfigServerClient(ApiClient):
    def __init__(self, host, port, user, password):
        super().__init__(host, port, user, password)
        self._extra_attrs = dict()

    @property
    def extra_attrs(self):
        return self._extra_attrs

    @extra_attrs.setter
    def extra_attrs(self, lst):
        for prop in lst:
            attr, value = prop.split("=")
            tree = self._extra_attrs
            for idx, k in enumerate(attr.split(".")):
                if idx != len(attr.split(".")) - 1:
                    tree = tree.setdefault(k, {})
                else:
                    tree[k] = value

    @extra_attrs.deleter
    def extra_attrs(self):
        del self._extra_attrs

    def get(self, uri_path=None, headers=None):
        if uri_path:
            uri_path = "/configserver{}".format(uri_path)
        else:
            uri_path = "/configserver{}".format(self.uri_path)
        headers = headers or ApiClient._headers
        LOGGER.debug("Performing GET request by URI path {}".format(uri_path))
        self._connection.request("GET", uri_path, headers=headers)
        response = self._connection.getresponse()
        self.uri_path = None
        if response.status != 200:
            raise Exception(
                    "GET failed, API gateway returned "
                    "{0.status} {0.reason} {1}".format(response,
                                                       response.read())
            )
        json_str = self.decode_response(response.read())
        if len(json_str) == 0:
            raise Exception(
                    "GET failed, API gateway returned empty response")
        result = ApiObjectMapper(json_str)
        if self.extra_attrs:
            return result.as_object(extra_attrs=self.extra_attrs,
                                    expand_dot_separated=True,
                                    overwrite=True)
        else:
            return result.as_object(expand_dot_separated=True)

    def get_property_sources_list(self, name, profile):
        self.uri_path = "/{0}/{1}".format(name, profile)
        return [s.source for s in self.get().propertySources]

    def get_property_source(self, name, profile, source_name):
        self.uri_path = "/{0}/{1}".format(name, profile)
        names_available = list()
        for source in self.get().propertySources:
            if source.name == source_name:
                return source
            else:
                names_available.append(source.name)
        raise KeyError("No such property source name: {0}, "
                       "available names: {1}".format(source_name,
                                                     names_available))


class GitLabClient(HttpsClient):
    def __init__(self, host, port, private_token):
        super().__init__(host, port, user=None, password=None)
        self._private_token = private_token
        self._headers = dict()

    def authorize(self):
        self._headers = {"PRIVATE-TOKEN": self._private_token}

    def get(self, uri_path=None, headers=None):
        uri_path = uri_path or self.uri_path
        LOGGER.debug("Performing GET request by URI path {}".format(uri_path))
        self._connection.request("GET", uri_path, headers=self._headers)
        response = self._connection.getresponse()
        if response.status != 200:
            raise Exception(
                    "GET failed, GitLab returned "
                    "{0.status} {0.reason} {1}".format(response,
                                                       response.read())
            )
        json_str = self.decode_response(response.read())
        if len(json_str) == 0:
            raise Exception(
                    "GET failed, Gitlab returned empty response")
        file_obj = json.loads(json_str)
        if "content" not in file_obj.keys() or not file_obj["content"]:
            raise Exception("Requested file has no content")
        return b64decode(file_obj["content"])

    def post(self, body, uri_path=None, headers=None):
        raise NotImplementedError

    def put(self, body, uri_path=None, headers=None):
        raise NotImplementedError

    def delete(self, uri_path=None, headers=None):
        raise NotImplementedError


class ApiObjectMapper:
    def __init__(self, json_string):
        self._json_string = json_string

    @staticmethod
    def dict_merge(target, *args, overwrite=False):
        if len(args) > 1:
            for obj in args:
                ApiObjectMapper.dict_merge(target, obj, overwrite=overwrite)
            return target

        obj = args[0]
        if not isinstance(obj, dict):
            return obj
        for k, v in obj.items():
            if k in target and isinstance(target[k], dict):
                ApiObjectMapper.dict_merge(target[k], v,
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
                mapping[k] = ApiObjectMapper.to_namedtuple(v)
            return ApiObjectMapper.namedtuple_from_mapping(mapping)
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
                ApiObjectMapper.cast_numeric_recursively(v)
            elif isinstance(v, str) and re.match("^[\d]+$", v):
                dct[k] = int(v)
            elif isinstance(v, str) and re.match("^[\d]?\.[\d]+$", v):
                dct[k] = float(v)
        return dct

    @staticmethod
    def object_hook(dct, extra, overwrite, expand):
        dct = ApiObjectMapper.cast_numeric_recursively(dct)
        if expand:
            new_dct = dict()
            for key in dct.keys():
                ApiObjectMapper.dict_merge(new_dct,
                                           reduce(lambda x, y: {y: x},
                                                  reversed(key.split(".")),
                                                  dct[key]),
                                           overwrite=overwrite)
            if extra and all(k in new_dct.keys() for k in extra.keys()):
                ApiObjectMapper.dict_merge(new_dct, extra,
                                           overwrite=overwrite)
            return ApiObjectMapper.to_namedtuple(new_dct)
        else:
            if extra and all(k in dct.keys() for k in extra.keys()):
                ApiObjectMapper.dict_merge(dct, extra, overwrite=overwrite)
            return ApiObjectMapper.namedtuple_from_mapping(dct)

    def as_object(self, extra_attrs=None,
                  overwrite=False, expand_dot_separated=False):
        return json.loads(self._json_string,
                          object_hook=lambda d: ApiObjectMapper.object_hook(
                              d, extra_attrs, overwrite, expand_dot_separated))

    def as_dict(self):
        return self.cast_numeric_recursively(json.loads(self._json_string))
