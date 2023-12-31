import abc
import base64
import http.client
import json
import time
import urllib.parse

from taskexecutor.logger import LOGGER
from taskexecutor.utils import to_lower_dashed, cast_to_numeric_recursively, object_hook

__all__ = ["ApiClient", "ConfigServerClient", "GitLabClient"]


class RequestError(Exception):
    pass


class ResponseError(Exception):
    pass


class HttpsClient(metaclass=abc.ABCMeta):
    def __init__(self, host, port, user, password):
        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self.uri_path = ""

    def __enter__(self):
        LOGGER.debug("Connecting to {0}:{1}".format(self._host, self._port))
        self._connection = http.client.HTTPSConnection("{0}:{1}".format(self._host, self._port), timeout=60)
        self.authorize()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._connection.close()

    @staticmethod
    def decode_response(resp_bytes):
        return resp_bytes.decode("UTF-8")

    @abc.abstractmethod
    def authorize(self):
        pass

    @abc.abstractmethod
    def post(self, body, uri_path=None, headers=None):
        pass

    @abc.abstractmethod
    def get(self, uri_path=None, headers=None):
        pass

    @abc.abstractmethod
    def put(self, body, uri_path=None, headers=None):
        pass

    @abc.abstractmethod
    def delete(self, uri_path=None, headers=None):
        pass


class ApiClient(HttpsClient):
    _headers = {"Content-Type": "application/json", "Accept": "application/json", "X-HMS-Projection": "te"}
    _access_token = None
    _expires_at = 0

    def _build_resource_uri(self, res_name, res_id):
        self.uri_path = "{0}/{1}/{2}".format(self.uri_path, res_name, res_id)

    def _build_collection_uri(self, res_name, query=None):
        if query:
            self.uri_path = "{0}/{1}?{2}".format(self.uri_path, res_name, urllib.parse.urlencode(query))
        else:
            self.uri_path = "{0}/{1}".format(self.uri_path, res_name)

    def authorize(self):
        if not self._access_token or time.time() > ApiClient._expires_at:
            post_data = urllib.parse.urlencode({"grant_type": "password",
                                                "username": self._user,
                                                "password": self._password,
                                                "client_id": "service",
                                                "client_secret": "service_secret"})
            headers = {"X-Requested-With": "XMLHttpRequest", "Content-Type": "application/x-www-form-urlencoded"}
            resp = json.loads(self.post(post_data, uri_path="/oauth/token", headers=headers))
            ApiClient._access_token = resp["access_token"]
            ApiClient._expires_at = resp["expires_in"] + time.time()
            ApiClient._headers.update({"Authorization": "Bearer {}".format(self._access_token)})

    def post(self, body, uri_path=None, headers=None):
        uri_path = uri_path or self.uri_path
        headers = headers or ApiClient._headers
        self._connection.request("POST", uri_path, body=body, headers=headers)
        LOGGER.debug("Performing POST request by URI path {0} with following data: '{1}'".format(uri_path, body))
        response = self._connection.getresponse()
        self.uri_path = ""
        if response.status // 100 != 2:
            LOGGER.error("POST failed, API gateway returned "
                         "{0.status} {0.reason} {1}".format(response, response.read()))
            return None
        return self.decode_response(response.read())

    def get(self, uri_path=None, headers=None):
        uri_path = uri_path or self.uri_path
        headers = headers or ApiClient._headers
        LOGGER.debug("Performing GET request by URI path {}".format(uri_path))
        self._connection.request("GET", uri_path, headers=headers)
        response = self._connection.getresponse()
        self.uri_path = ""
        if response.status == 404:
            LOGGER.warning("API gateway returned {0.status} {0.reason} {1}".format(response, response.read()))
            return
        if response.status != 200:
            raise RequestError("GET failed, API gateway returned "
                               "{0.status} {0.reason} {1}".format(response, response.read()))
        json_str = self.decode_response(response.read())
        if len(json_str) == 0:
            raise RequestError("GET failed, API gateway returned empty response")
        resource = ApiObjectMapper(json_str)
        return resource.as_object()

    def put(self, body, uri_path=None, headers=None):
        raise NotImplementedError

    def delete(self, uri_path=None, headers=None):
        raise NotImplementedError

    def resource(self, resource_type):
        return getattr(self, resource_type)()

    def filter(self, **query):
        self._build_collection_uri("filter", query)
        return self

    def find(self, **query):
        self._build_collection_uri("find", query)
        return self

    def __getattr__(self, name):
        name = to_lower_dashed(name)

        def constructor(res_id=None, query=None):
            if res_id:
                self._build_resource_uri(name, res_id)
            elif query:
                self._build_collection_uri(name, query)
            else:
                self._build_collection_uri(name)
            return self

        return constructor


class ConfigServerClient(ApiClient):
    def __init__(self, host, port, user, password):
        super().__init__(host, port, user, password)
        self._extra_attrs = {}

    @property
    def extra_attrs(self):
        return self._extra_attrs

    @extra_attrs.setter
    def extra_attrs(self, lst):
        for prop in lst:
            attr, *value = prop.split("=")
            value = "=".join(value)
            tree = self._extra_attrs
            for idx, k in enumerate(attr.split(".")):
                if idx != len(attr.split(".")) - 1:
                    tree = tree.setdefault(k, {})
                else:
                    tree[k] = value

    @extra_attrs.deleter
    def extra_attrs(self):
        self._extra_attrs = {}

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
            raise RequestError("GET failed, API gateway returned "
                               "{0.status} {0.reason} {1}".format(response, response.read()))
        json_str = self.decode_response(response.read())
        if len(json_str) == 0:
            raise RequestError("GET failed, API gateway returned empty response")
        result = ApiObjectMapper(json_str)
        if self.extra_attrs:
            return result.as_object(extra_attrs=self.extra_attrs, expand_dot_separated=True,
                                    comma_separated_to_list=True, overwrite=True, force_numeric=True)
        else:
            return result.as_object(expand_dot_separated=True, comma_separated_to_list=True, force_numeric=True)

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
        raise ResponseError("No such property source name: {0},"
                            "available names: {1}".format(source_name, names_available))


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
            raise RequestError("GET failed, GitLab returned {0.status} {0.reason} "
                               "{1}, URI: {2}".format(response, response.read(), uri_path))
        json_str = self.decode_response(response.read())
        if len(json_str) == 0:
            raise RequestError("GET failed, Gitlab returned empty response")
        file_obj = json.loads(json_str)
        if "content" not in file_obj.keys() or not file_obj["content"]:
            raise ResponseError("Requested file has no content")
        return self.decode_response(base64.b64decode(file_obj["content"]))

    def post(self, body, uri_path=None, headers=None):
        raise NotImplementedError

    def put(self, body, uri_path=None, headers=None):
        raise NotImplementedError

    def delete(self, uri_path=None, headers=None):
        raise NotImplementedError


class ApiObjectMapper:
    def __init__(self, json_string):
        self._json_string = json_string

    def as_object(self, extra_attrs=None, overwrite=False,
                  expand_dot_separated=False, comma_separated_to_list=False, force_numeric=False):
        return json.loads(
            self._json_string,
            object_hook=lambda d: object_hook(d, extra_attrs, overwrite, expand_dot_separated,
                                              comma_separated_to_list, force_numeric)
        )

    def as_dict(self):
        return cast_to_numeric_recursively(json.loads(self._json_string))
