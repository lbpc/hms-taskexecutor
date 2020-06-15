import abc
import os
import shutil

import docker
from docker.errors import ContainerError

from taskexecutor.opservice import DatabaseServer
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.utils import exec_command, asdict

__all__ = ["DockerDataPostprocessor", "StringReplaceDataProcessor", "DataEraser"]


class PostprocessorArgumentError(Exception):
    pass


class CommandExecutionError(Exception):
    pass


class DataPostprocessor(metaclass=abc.ABCMeta):
    def __init__(self, **kwargs):
        self.args = kwargs

    @abc.abstractmethod
    def process(self):
        pass


class DockerDataPostprocessor(DataPostprocessor):
    def process(self):
        docker_client = docker.from_env()
        docker_client.login(**asdict(CONFIG.docker_registry))
        image = docker_client.images.pull(self.args['image'])
        env = self.args.get('env', {})
        volumes = {self.args.get('cwd'): {'bind': '/workdir', 'mode': 'rw'},
                   '/etc/nsswitch.conf': {'bind': '/etc/nsswitch.conf', 'mode': 'ro'}}
        hosts = self.args.get('hosts')
        user = '{0}:{0}'.format(self.args.get('uid', 65534))
        command = self.args.get('command')
        LOGGER.info(f'Runnig Docker container from {image} with net=host, volumes={volumes} '
                    f'user={user}, env={env} hosts={hosts}' + f", command={command}" if command else '')
        try:
            return docker_client.containers.run(image, remove=True, network_mode='host', volumes=volumes, user=user,
                                                environment=env, extra_hosts=hosts, command=command).decode()
        except ContainerError as e:
            raise CommandExecutionError(e.stderr.decode())


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
        cwd = self.args.get("path") or self.args.get("cwd")
        file_globs = self.args.get("fileNameGlobs") or self.default_file_globs
        search_pattern = self.args.get("searchPattern")
        replace_string = self.args.get("replaceString")
        find_expr = "( {} )".format(" -or ".join(["-name {}".format(g)
                                                  for g in file_globs])).translate(str.maketrans(self.shell_escape_map))
        LOGGER.info("Replacing '{}' pattern by '{}' "
                    "in {} files from {}".format(search_pattern, replace_string, file_globs, cwd))
        cmd = ("find -O3 {0} {1} -type f "
               "-exec grep -q -e'{2}' {{}} \; -and "
               "-exec sed -i 's#{2}#{3}#g' {{}} \;").format(cwd, find_expr, search_pattern, replace_string)
        exec_command(cmd)


class DataEraser(DataPostprocessor):
    @property
    def supported_data_types(self):
        return ["directory", "database"]

    def _erase_directory(self):
        path = self.args.get("path") or self.args.get("cwd")
        if not path:
            raise PostprocessorArgumentError("No directory path was specified")
        LOGGER.info("Removing all files from {}".format(path))
        uid = os.stat(path).st_uid
        shutil.rmtree(path)
        os.mkdir(path, mode=0o700)
        os.chown(path, uid, uid)

    def _erase_database(self):
        name = self.args.get("name")
        if not name:
            raise PostprocessorArgumentError("No database name was specified")
        db_server = self.args.get("dbServer")
        if not db_server:
            raise PostprocessorArgumentError("No database server was specified")
        if not isinstance(db_server, DatabaseServer):
            raise PostprocessorArgumentError("{} is not a database server".format(db_server))
        LOGGER.info("Dropping all data from {} database".format(name))
        db_server.drop_database(name)
        db_server.create_database(name)

    def process(self):
        type = self.args.get("dataType")
        if type not in self.supported_data_types:
            raise PostprocessorArgumentError("{} data type is not supported by {}".format(type,
                                                                                          self.__class__.__name__))
        getattr(self, "_erase_{}".format(type))()
