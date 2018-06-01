import abc
import itertools
import os
import shutil
import urllib.parse

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class UnsupportedDstUriScheme(Exception):
    pass


class DataFetchingError(Exception):
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
            LOGGER.info("Copiyng files from {} to {}".format(self._src_path, self._dst_path))
            for each in os.listdir(self._src_path):
                src = os.path.join(self._src_path, each)
                dst = os.path.join(self._dst_path, each)
                if os.path.isdir(src):
                    shutil.copytree(src, dst)
                else:
                    shutil.copyfile(src, dst)

    def _copy_file_to_rsync(self):
        LOGGER.info("Syncing files between {} and {}".format(self._src_path, self.dst_uri))
        cmd = "rsync -av {0} {1}".format(self._src_path, self.dst_uri)
        taskexecutor.utils.exec_command(cmd)

    def fetch(self):
        getattr(self, "_copy_file_to_{}".format(self._dst_scheme))()


class RsyncDataFetcher(DataFetcher):
    def __init__(self, src_uri, dst_uri, params):
        super().__init__(src_uri, dst_uri, params)
        self.exclude_patterns = params.get("excludePatterns")

    @property
    def supported_dst_uri_schemes(self):
        return ["file"]

    def fetch(self):
        dst_path = urllib.parse.urlparse(self.dst_uri).path
        if urllib.parse.urlparse(self.src_uri).netloc != CONFIG.localserver.name:
            LOGGER.info("Syncing files between {} and {}".format(self.src_uri, dst_path))
            cmd = "rsync {} -av {}/ {}".format("".join(map(lambda p: "--exclude {} ".format(p), self.exclude_patterns)),
                                               self.src_uri, dst_path)
            taskexecutor.utils.exec_command(cmd)


class MysqlDataFetcher(DataFetcher):
    def __init__(self, src_uri, dst_uri, params):
        super().__init__(src_uri, dst_uri, params)
        src_host_port = urllib.parse.urlparse(src_uri).netloc.split(":")
        self.src_host, self.src_port = next(itertools.zip_longest(src_host_port, (), fillvalue=CONFIG.mysql.port))
        dst_host_port = urllib.parse.urlparse(dst_uri).netloc.split(":")
        self.dst_host, self.dst_port = next(itertools.zip_longest(dst_host_port, (), fillvalue=CONFIG.mysql.port))
        self.user = params.get("user") or CONFIG.mysql.user
        self.password = params.get("password") or CONFIG.mysql.password
        self.database = urllib.parse.urlparse(src_uri).path.strip("/")

    @property
    def supported_dst_uri_schemes(self):
        return ["mysql", "file"]

    def _get_dump_streams(self):
        cmd = "mysqldump -h{0} -P{1} -u{2} -p{3} {4}".format(self.src_host, self.src_port,
                                                             self.user, self.password, self.database)
        return taskexecutor.utils.exec_command(cmd, return_raw_streams=True)

    def fetch(self):
        if self.src_uri != self.dst_uri:
            data, error = self._get_dump_streams()
            if urllib.parse.urlparse(self.dst_uri).scheme == "mysql":
                cmd = "mysql -h{0} -P{1} -u{2} -p{3} {4}".format(self.dst_host, self.dst_port,
                                                                 self.user, self.password, self.database)
                taskexecutor.utils.exec_command(cmd, pass_to_stdin=data)
            else:
                path = urllib.parse.urlparse(self.dst_uri).path
                with open(path, "w") as f:
                    f.write(data)
            error = error.read().decode("UTF-8")
            if error:
                raise DataFetchingError("Failed to dump MySQL database {}, error: {}".format(self.database, error))


class Builder:
    def __new__(cls, proto):
        DataFetcherClass = {"file": FileDataFetcher,
                            "rsync": RsyncDataFetcher,
                            "mysql": MysqlDataFetcher}.get(proto)
        if not DataFetcherClass:
            raise BuilderTypeError("Unknown data source URI scheme: {}".format(proto))
        return DataFetcherClass
