import http.client
import urllib.parse
import json
import re
from functools import reduce
from collections import Mapping, namedtuple
from copy import deepcopy


class HttpClient:
    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._uri = None

    def __enter__(self):
        self._connection = http.client.HTTPConnection(
                "{0}:{1}".format(self._host, self._port))
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

    def post(self, body, uri=None):
        raise NotImplementedError

    def get(self, uri=None, **kwargs):
        _uri = uri or self._uri
        self._connection.request("GET", _uri)
        return self.process_response("GET",
                                     self._connection.getresponse(),
                                     **kwargs)

    def put(self, body, uri=None):
        raise NotImplementedError

    def delete(self, uri=None):
        raise NotImplementedError

    def process_response(self, method, response, **kwargs):
        if response.status//100 == 5:
            raise Exception("{0} failed, HTTP server returned "
                            "{1.status} {1.reason}".format(method, response))
        return HttpClient.decode_response(response.read())


class ApiClient(HttpClient):
    def __init__(self, host, port, service="rc"):
        super().__init__(host, port)
        self._service = service

    def _build_resource_uri(self, res_name, res_id):
        self._uri = "/{0}/{1}/{2}".format(self._service, res_name, res_id)
        return self

    def _build_collection_uri(self, res_name, query=None):
        if query:
            self._uri = "/{0}/{1}?{2}".format(self._service,
                                              res_name,
                                              urllib.parse.urlencode(query))
        else:
            self._uri = "/{0}/{1}".format(self._service, res_name)
        return self

    def process_response(self, method, response, uri=None, as_object=True,
                         extra_attrs=None):
        if method == "GET":
            if response.status != 200:
                raise Exception("GET failed, REST server returned "
                                "{0.status} {0.reason}".format(response))
            _json_str = self.decode_response(response.read())
            if len(_json_str) == 0:
                raise Exception("GET failed, REST server returned empty response")
            _resource = ApiObjectTranslator(_json_str)
            if as_object:
                return _resource.as_object(extra_attrs=extra_attrs)
            else:
                return _resource.as_dict()

    def __getattr__(self, name):
        def wrapper(res_id=None, query=None):
            if res_id:
                return self._build_resource_uri(name, res_id)
            elif query:
                return self._build_collection_uri(name, query)
            else:
                return self._build_collection_uri(name)

        return wrapper


class ApiObjectTranslator:
    def __init__(self, json_string):
        self._json_string = json_string

    @staticmethod
    def dict_merge(target, *args):
        if len(args) > 1:
            for obj in args:
                ApiObjectTranslator.dict_merge(target, obj)
            return target

        obj = args[0]
        if not isinstance(obj, dict):
            return obj
        for k, v in obj.items():
            if k in target and isinstance(target[k], dict):
                ApiObjectTranslator.dict_merge(target[k], v)
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
    def object_hook(dct, extra, expand):
        dct = ApiObjectTranslator.cast_numeric_recursively(dct)
        if expand:
            _new_dct = dict()
            for key in dct.keys():
                ApiObjectTranslator.dict_merge(_new_dct,
                                               reduce(lambda x, y: {y: x},
                                              reversed(key.split(".")),
                                              dct[key]))
            if extra and all(k in _new_dct.keys() for k in extra.keys()):
                ApiObjectTranslator.dict_merge(_new_dct, extra)
            return ApiObjectTranslator.to_namedtuple(_new_dct)
        else:
            if extra and all(k in dct.keys() for k in extra.keys()):
                ApiObjectTranslator.dict_merge(dct, extra)
            return ApiObjectTranslator.namedtuple_from_mapping(dct)

    def as_object(self, extra_attrs=None, expand_dot_separated=True):
        return json.loads(self._json_string, object_hook=lambda
            d: ApiObjectTranslator.object_hook(d, extra_attrs,
                                               expand_dot_separated))

    def as_dict(self):
        return self.cast_numeric_recursively(json.loads(self._json_string))