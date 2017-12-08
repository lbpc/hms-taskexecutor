import abc
import docker

from taskexecutor.config import CONFIG
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class UnsupportedDstUriScheme(Exception):
    pass


class DataPostprocessor(metaclass=abc.ABCMeta):
    def __init__(self, **kwargs):
        self._args = kwargs

    @property
    def args(self):
        return self._args

    @args.setter
    def args(self, value):
        self._args = value

    @abc.abstractmethod
    def process(self):
        pass


class DockerDataPostprocessor(DataPostprocessor):
    def process(self):
        image = self.args.get("image")
        env = self.args.get("env")
        volumes = {self.args.get("cwd"): {"bind": "/workdir", "mode": "rw"}}
        hosts = self.args.get("hosts")
        user = "{0}:{0}".format(self.args.get("uid", 65534))
        docker_client = docker.from_env()
        docker_client.login(**CONFIG.docker_registry._asdict())
        docker_client.images.pull(image)
        docker_client.containers.run(image, remove=True, dns=["127.0.0.1"], network_mode="host",
                                     volumes=volumes, user=user, environment=env, extra_hosts=hosts)


class StringReplaceDataProcessor(DataPostprocessor):
    @property
    def default_file_globs(self):
        return ["*.php", "*.html", "*.shtml", "*.phtml", "*.sphp", "*.ini",
                "*.conf", "*.config", "*.inc", "*.p", "*.xml", "*.settings*", ".htaccess"]

    @property
    def shell_escape_map(self):
        return {"(": r"\(",
                ")": r"\)",
                "[": r"\]",
                "]": r"\]",
                "\\": r"\\",
                "*": r"\*",
                "?": r"\?"}

    def process(self):
        cwd = self.args.get("cwd")
        file_globs = self.args.get("fileNameGlobs") or self.default_file_globs
        search_pattern = self.args.get("searchPattern")
        replace_string = self.args.get("replaceString")
        find_expr = "( {} )".format(" -or ".join(file_globs)).translate(str.maketrans(self.shell_escape_map))
        cmd = ("find -O3 {0} {1} -type f "
               "-exec grep -q -e'{2}' {{}} \; -and "
               "-exec sed -i 's/{2}/{3}/g' {{}} \;").format(cwd, find_expr, search_pattern, replace_string)
        taskexecutor.utils.exec_command(cmd)


class Builder:
    def __new__(cls, postproc_type):
        DataPostprocessorClass = {"docker": DockerDataPostprocessor,
                                  "string-replace": StringReplaceDataProcessor}.get(postproc_type)
        if not DataPostprocessorClass:
            raise BuilderTypeError("Unknown data postprocessor type: {}".format(postproc_type))
        return DataPostprocessorClass
