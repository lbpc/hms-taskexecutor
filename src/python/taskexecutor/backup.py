import abc
import os
import requests

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class BackupError(Exception):
    pass


class Backuper(metaclass=abc.ABCMeta):
    def __init__(self, resource):
        self._resource = resource

    @property
    @abc.abstractmethod
    def default_excludes(self):
        return

    @abc.abstractmethod
    def backup(self, exclude=()):
        pass


class ResticBackup(Backuper):
    @property
    def default_excludes(self):
        return ("/home/**/tmp", "/home/*/logs")

    def backup(self, exclude=()):
        dir = "/dev/null"
        if hasattr(self._resource, "homeDir"):
            dir = self._resource.homeDir
        elif hasattr(self._resource, "unixAccount") \
                and hasattr(self._resource.unixAccount, "homeDir") and hasattr(self._resource, "documentRoot"):
            dir = os.path.join(self._resource.unixAccount.homeDir, self._resource.documentRoot)
        elif hasattr(self._resource, "mailSpool"):
            dir = os.path.join(self._resource.mailSpool, self._resource.name)
        repo = os.path.basename(dir)
        if hasattr(self._resource, "mailSpool") and hasattr(self._resource, "domain"):
            repo = "{}@{}".format(self._resource.name, self._resource.domain.name)
        exclude = exclude or self.default_excludes
        base_cmd = ("RESTIC_PASSWORD={0.password} "
               "{0.binary.path} -r rest:http://restic:{0.password}@{0.host}:{0.port}/{1} ".format(CONFIG.restic, repo))
        backup_cmd = "backup {0} {1}".format("".join((" -e {}".format(e) for e in exclude)), dir)
        code, stdout, stderr = taskexecutor.utils.exec_command(base_cmd + "init")
        if code > 0 and not stderr.rstrip().endswith("already exists"):
            raise BackupError(stderr)
        code, stdout, stderr = taskexecutor.utils.exec_command(base_cmd + backup_cmd, raise_exc=False)
        if code > 0:
            raise BackupError(stderr)
        try:
            snapshot_id = stdout.strip().split("\n")[-1].split()[1]
            LOGGER.info("{} saved in {} repo".format(snapshot_id, repo))
        except IndexError:
            LOGGER.warn("{} snapshotted successfully, but no snapshot ID found, "
                        "STDOUT: {} STDERR: {}".format(repo, stdout, stderr))
        code, stdout, stderr = taskexecutor.utils.exec_command(base_cmd + " forget --keep-within 31d", raise_exc=False)
        if code > 0:
            LOGGER.warn("Failed to forget old snapshots for repo {}, "
                        "STDOUT: {} STDERR: {}".format(repo, stdout, stderr))
        try:
            requests.get("http://{}/_snapshot/{}".format(CONFIG.backup.server.names[0], repo))
        except Exception as e:
            LOGGER.warn("Failed to list snapshots on backup server: {}".format(e))


class Builder:
    def __new__(cls, res_type):
        ListenerClass = {"unix-account": ResticBackup,
                         "website": ResticBackup}.get(res_type)
        if not ListenerClass:
            raise BuilderTypeError("Unknown resource type: {}".format(res_type))
        return ListenerClass