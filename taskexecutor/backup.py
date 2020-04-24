import abc
import hashlib
import os
import re
import requests
import shutil
import shlex
import time
from psutil import pid_exists, Process

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.utils

__all__ = ["ResticBackup"]


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

    @staticmethod
    def _run_expecting_restic_lock(base_cmd, cmd):
        cmd = " " + cmd.lstrip()
        code = 1
        stdout = stderr = ""
        while code > 0:
            code, stdout, stderr = taskexecutor.utils.exec_command(base_cmd + cmd, raise_exc=False)
            matched = re.match(r".*locked.*by PID (\d+) on ([^.]+)", stderr or "")
            if code > 0 and not matched:
                break
            elif code > 0:
                pid, host = matched.groups()
                pid = int(pid)
                if host == CONFIG.hostname and (not pid_exists(pid) or Process(pid) != "restic"):
                    # Considering that repository was locked from here and PID is no longer exist,
                    # it's safe to unlock now
                    LOGGER.warn("repo is locked by PID {} from {} which is no longer running, "
                                "unlocking".format(pid, host))
                    taskexecutor.utils.exec_command(base_cmd + " unlock")
                else:
                    LOGGER.warn("repo is locked by PID {} at {}, waiting for 5s".format(pid, host))
                    time.sleep(5)
        return code, stdout.strip(), stderr.strip()

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
        repo = os.path.join("slice", hashlib.sha1(repo.encode()).hexdigest()[:2], repo)
        exclude = exclude or self.default_excludes
        restic = CONFIG.restic.binary_path if os.path.exists(CONFIG.restic.binary_path) else shutil.which('restic')
        base_cmd = ("RESTIC_PASSWORD={0.password} "
               "{1} -r rest:http://restic:{0.password}@{0.host}:{0.port}/{2} ".format(CONFIG.restic, restic, repo))
        backup_cmd = "backup {0} {1}".format("".join((" -e {}".format(shlex.quote(e)) for e in exclude)), dir)
        code, stdout, stderr = taskexecutor.utils.exec_command(base_cmd + "init", raise_exc=False)
        if code > 0 and not stderr.rstrip().endswith("already exists"):
            raise BackupError("Resic error: {}".format(stderr.strip()))
        code, stdout, stderr = self._run_expecting_restic_lock(base_cmd, backup_cmd)
        if code > 0:
            raise BackupError("Resic error: {}".format(stderr))
        try:
            snapshot_id = stdout.split("\n")[-1].split()[1]
            LOGGER.info("{} saved in {} repo".format(snapshot_id, repo))
        except IndexError:
            LOGGER.warn("{} snapshotted successfully, but no snapshot ID found in stdout, "
                        "STDOUT: {} STDERR: {}".format(repo, stdout.strip(), stderr.strip()))
        code, stdout, stderr = self._run_expecting_restic_lock(base_cmd, "forget --keep-within 31d")
        if code > 0:
            LOGGER.warn("Failed to forget old snapshots for repo {}, "
                        "STDOUT: {} STDERR: {}".format(repo, stdout, stderr))
        # XXX:
        # try:
        #     requests.get("http://{}/_snapshot/{}".format(CONFIG.backup.server.names[0], os.path.basename(repo)))
        # except Exception as e:
        #     LOGGER.warn("Failed to list snapshots on backup server: {}".format(e))
