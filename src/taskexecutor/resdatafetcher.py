import abc
import os
import shutil
import urllib.parse

import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class UnsupportedDstUriScheme(Exception):
    pass


class DataFetcher(metaclass=abc.ABCMeta):
    def __init__(self, src_uri, dst_uri, params):
        self._src_uri = None
        self._dst_uri = None
        self._params = params
        self.src_uri = src_uri
        self.dst_uri = dst_uri

    @property
    def src_uri(self):
        return self._src_uri

    @src_uri.setter
    def src_uri(self, value):
        self._src_uri = value

    @property
    def dst_uri(self):
        return self._dst_uri

    @dst_uri.setter
    def dst_uri(self, value):
        if urllib.parse.urlparse(value).scheme not in self.supported_dst_uri_schemes:
            raise UnsupportedDstUriScheme("Unsupported destination URI scheme for {0}. "
                                          "Supported: {1}, given URI: {2}".format(self.__class__.__name__,
                                                                                  self.supported_dst_uri_schemes,
                                                                                  value))
        self._dst_uri = value

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, value):
        self._params = value

    @property
    @abc.abstractmethod
    def supported_dst_uri_schemes(self):
        return

    @abc.abstractmethod
    def fetch(self):
        pass


class FileDataFetcher(DataFetcher):
    def __init__(self, src_uri, dst_uri, params):
        super().__init__(src_uri, dst_uri, params)
        self._src_path = urllib.parse.urlparse(self.src_uri).path
        self._dst_path = urllib.parse.urlparse(self.dst_uri).path
        self._dst_scheme = urllib.parse.urlparse(self.dst_uri).scheme

    @property
    def supported_dst_uri_schemes(self):
        return ["file", "rsync"]

    def _copy_file_to_file(self):
        if self._src_path != self._dst_path:
            for each in os.listdir(self._src_path):
                src = os.path.join(self._src_path, each)
                dst = os.path.join(self._dst_path, each)
                if os.path.isdir(src):
                    shutil.copytree(src, dst)
                else:
                    shutil.copyfile(src, dst)

    def _copy_file_to_rsync(self):
        cmd = "rsync -av {0} {1}".format(self._src_path, self.dst_uri)
        taskexecutor.utils.exec_command(cmd)

    def fetch(self):
        getattr(self, "_copy_file_to_{}".format(self._dst_scheme))()


class Builder:
    def __new__(cls, proto):
        DataFetcherClass = {"file": FileDataFetcher}.get(proto)
        if not DataFetcherClass:
            raise BuilderTypeError("Unknown data source URI scheme: {}".format(proto))
        return DataFetcherClass
