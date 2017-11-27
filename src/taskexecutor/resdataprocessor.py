import abc
import urllib.parse
import docker


class BuilderTypeError(Exception):
    pass


class UnsupportedDstUriScheme(Exception):
    pass


class DataPostprocessor(metaclass=abc.ABCMeta):
    def __init__(self, data_uri, params):
        self._data_uri = data_uri
        self._params = params

    @property
    def data_uri(self):
        return self._data_uri

    @data_uri.setter
    def data_uri(self, value):
        self._data_uri = value

    @property
    def params(self):
        return self._params

    @params.setter
    def params(self, value):
        self._params = value

    @abc.abstractmethod
    def process(self):
        pass


class DockerDataPostprocessor(DataPostprocessor):
    def process(self):
        image = self.params.get("image")
        env = self.params.get("env")
        volumes = self.params.get("volumes")
        hosts = self.params.get("hosts")
        uid = self.params.get("uid")
        docker_client = docker.from_env()
        docker_client.containers.run(image, remove=True, volumes=volumes, user=uid, environment=env, extra_hosts=hosts)


class Builder:
    def __new__(cls, postproc_type):
        DataPostprocessorClass = {"docker": DockerDataPostprocessor}.get(postproc_type)
        if not DataPostprocessorClass:
            raise BuilderTypeError("Unknown data postprocessor type: {}".format(postproc_type))
        return DataPostprocessorClass




