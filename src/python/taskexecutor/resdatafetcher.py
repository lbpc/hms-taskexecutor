import abc
import shlex
import os
import shutil
import urllib.parse
import requests
import json
import giturlparse
import tempfile

import taskexecutor.constructor as cnstr
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.utils import exec_command

__all__ = ["FileDataFetcher", "RsyncDataFetcher", "MysqlDataFetcher", "HttpDataFetcher", "GitDataFetcher"]


class UnsupportedDstUriScheme(Exception):
    pass


class DataFetchingError(Exception):
    pass


class DataFetcher(metaclass=abc.ABCMeta):
    def __init__(self, src_uri, dst_uri, params):
        self.src_uri = src_uri
        self.dst_uri = dst_uri
        self.params = params

    @property
    def dst_uri(self):
        return getattr(self, "_dst_uri", None)

    @dst_uri.setter
    def dst_uri(self, value):
        if urllib.parse.urlparse(value).scheme not in self.supported_dst_uri_schemes:
            raise UnsupportedDstUriScheme("Unsupported destination URI scheme for {0}. "
                                          "Supported: {1}, given URI: {2}".format(self.__class__.__name__,
                                                                                  self.supported_dst_uri_schemes,
                                                                                  value))
        self._dst_uri = value

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
        exec_command(cmd)

    def fetch(self):
        getattr(self, "_copy_file_to_{}".format(self._dst_scheme))()


class RsyncDataFetcher(DataFetcher):
    def __init__(self, src_uri, dst_uri, params):
        super().__init__(src_uri, dst_uri, params)
        self.exclude_patterns = params.get("excludePatterns", [])
        self.delete_extraneous = params.get("deleteExtraneous", False)
        self.owner_uid = params.get("ownerUid")
        self.src_host = urllib.parse.urlparse(src_uri).netloc
        self.src_path = urllib.parse.urlparse(src_uri).path
        self.dst_path = urllib.parse.urlparse(dst_uri).path
        self.restic_repo = None
        if self.src_host.split(":")[0] in CONFIG.backup.server.names and \
                self.src_path.split('/')[1:2] == [CONFIG.backup.server.restic_location]:
            self.restic_repo = self.src_path.split("/ids/")[0].replace(
                    "/{}/".format(CONFIG.backup.server.restic_location), ""
            )

    def _mount_restic_repo(self):
        url = "http://{}/_mount/{}".format(self.src_host, self.restic_repo)
        LOGGER.info("Requesting restic repo mount: {}".format(url))
        res = requests.post(url, timeout=CONFIG.backup.server.mount_timeout + 3, params={"wait": True, "timeout": CONFIG.backup.server.mount_timeout})
        if not res.ok:
            raise DataFetchingError("Failed to mount Restic repo: {}".format(json.loads(res.text).get("error")))

    def _umount_restic_repo(self):
        url = "http://{}/_mount/{}".format(self.src_host, self.restic_repo)
        LOGGER.info("Requesting restic repo umount: {}".format(url))
        requests.delete(url)

    @property
    def supported_dst_uri_schemes(self):
        return ["file"]

    def fetch(self):
        if urllib.parse.urlparse(self.src_uri).netloc != CONFIG.localserver.name:
            if self.restic_repo:
                self._mount_restic_repo()
            LOGGER.info("Syncing files between {} and {}".format(self.src_uri, self.dst_path))
            args = "".join(map(lambda p: "--exclude {} ".format(p), self.exclude_patterns))
            if self.delete_extraneous:
                args += " --delete "
            cmd = "rsync {} -av {} {}".format(args, shlex.quote(self.src_uri), shlex.quote(self.dst_path))
            error = None
            try:
                exec_command(cmd)
            except Exception as e:
                error = e
            if self.restic_repo:
                self._umount_restic_repo()
            if error:
                raise error
            if self.owner_uid:
                if not self.src_uri.endswith("/"):
                    self.dst_path = os.path.join(self.dst_path,
                                                 os.path.split(urllib.parse.urlparse(self.src_uri).path)[1])
                exec_command("chown -R {0}:{0} {1}".format(self.owner_uid, self.dst_path))
                logs_path = self.dst_path + "/logs"
                if os.path.isdir(logs_path):
                    exec_command("chown -R {}:0 {}".format(self.owner_uid, logs_path))


class MysqlDataFetcher(DataFetcher):
    def __init__(self, src_uri, dst_uri, params):
        super().__init__(src_uri, dst_uri, params)
        src_uri_parsed = urllib.parse.urlparse(src_uri)
        dst_uri_parsed = urllib.parse.urlparse(dst_uri)
        self.src_uri_scheme = src_uri_parsed.scheme
        self.src_host = src_uri_parsed.netloc.split(":")[0]
        self.src_port = src_uri_parsed.netloc.split(":")[-1] if ":" in src_uri_parsed.netloc else CONFIG.mysql.port
        self.src_database = os.path.basename(src_uri_parsed.path)
        self.src_user = params.get("user") or CONFIG.mysql.user
        self.src_password = params.get("password") or CONFIG.mysql.password
        self.dst_host = dst_uri_parsed.netloc.split(":")[0]
        self.dst_port = dst_uri_parsed.netloc.split(":")[-1] if ":" in dst_uri_parsed.netloc else CONFIG.mysql.port
        self.dst_database = os.path.basename(dst_uri_parsed.path)

    @property
    def supported_dst_uri_schemes(self):
        return ["mysql", "file"]

    def _get_dump_streams(self):
        cmd = "mysqldump -h{0.src_host} -P{0.src_port} -u{0.src_user} -p{0.src_password} {0.src_database}".format(self)
        return exec_command(cmd, return_raw_streams=True)

    def fetch(self):
        if self.src_uri != self.dst_uri:
            data, error = self._get_dump_streams()
            if urllib.parse.urlparse(self.dst_uri).scheme == "mysql":
                cmd = "mysql -h{0.dst_host} -P{0.dst_port} -u{1.user} -p{1.password} " \
                      "{0.dst_database}".format(self, CONFIG.mysql)
                exec_command(cmd, pass_to_stdin=data)
            else:
                path = urllib.parse.urlparse(self.dst_uri).path
                with open(path, "w") as f:
                    f.write(data)
            error = error.read().decode("UTF-8")
            if error:
                raise DataFetchingError("Failed to dump MySQL database {}, error: {}".format(self.src_database, error))


class HttpDataFetcher(DataFetcher):
    def __init__(self, src_uri, dst_uri, params):
        super().__init__(src_uri, dst_uri, params)
        self._curl_cmd = "curl -s {}".format(src_uri)
        if urllib.parse.urlparse(src_uri).path.split(".")[-1] == "gz":
            self._curl_cmd += " | gunzip"
        self._dst_uri_parsed = urllib.parse.urlparse(self.dst_uri)

    @property
    def supported_dst_uri_schemes(self):
        return ["mysql", "file"]

    def _curl_to_mysql(self):
        host = self._dst_uri_parsed.netloc.split(":")[0]
        port = self._dst_uri_parsed.netloc.split(":")[-1] if ":" in self._dst_uri_parsed.netloc else CONFIG.mysql.port
        db = os.path.basename(self._dst_uri_parsed.path)
        cmd = "mysql -h{0} -P{1} -u{3.user} -p{3.password} {2}".format(host, port, db, CONFIG.mysql)
        data, error = exec_command(self._curl_cmd, return_raw_streams=True)
        exec_command(cmd, pass_to_stdin=data)
        error = error.read().decode("UTF-8")
        if error:
            raise DataFetchingError("Failed to fetch {}, error: {}".format(self.src_uri, error))

    def _curl_to_file(self):
        data, error = exec_command(self._curl_cmd, return_raw_streams=True)
        with open(self._dst_uri_parsed.path, "w") as f:
            f.write(data)
        error = error.read().decode("UTF-8")
        if error:
            raise DataFetchingError("Failed to fetch {}, error: {}".format(self.src_uri, error))

    def fetch(self):
        getattr(self, "_curl_to_{}".format(self._dst_uri_parsed.scheme))()


class GitDataFetcher(DataFetcher):
    def __init__(self, src_uri, dst_uri, params):
        super().__init__(src_uri, dst_uri, params)
        self.branch = params.get('branch', 'master')
        if 'key' in params and giturlparse.parse(src_uri).protocol == 'ssh':
            self.key = cnstr.get_conffile('basic', tempfile.mkstemp()[1], owner_uid=0, mode=0o400)
            self.key.body = params['key']
            self.key.save()
        self.password = params.get('password')
        self.owner_uid = params.get('ownerUid', 0)
        self.src_uri = self.normalize_git_url(src_uri, params.get('username'))
        self.dst_path = urllib.parse.urlparse(dst_uri).path

    @staticmethod
    def is_git_repo(path):
        r, _, _ = exec_command(f'git -C {path} rev-parse --git-dir', raise_exc=False)
        return r == 0

    @staticmethod
    def get_git_url(repo_path):
        url = exec_command(f'git -C {repo_path} ls-remote --get-url')
        if not urllib.parse.urlparse(url).scheme:
            url = 'ssh://' + url
        return url.strip()

    @staticmethod
    def get_git_user(repo_path):
        _, r, _ = exec_command(f'git -C {repo_path} config user.name', raise_exc=False)
        return r.strip() or None

    @staticmethod
    def normalize_git_url(url, user=None):
        parsed = giturlparse.parse(url)
        user = parsed.user or user
        netloc = parsed.resource
        if parsed.port: netloc += f':{parsed.port}'
        if user: netloc = f'{user}@' + netloc
        return urllib.parse.urlunparse((parsed.protocol, netloc, parsed.pathname, '', '', ''))

    @property
    def supported_dst_uri_schemes(self):
        return ['file']

    def fetch(self):
        env = {'GIT_ASKPASS': 'gitaskpass'}
        if self.password: env['GIT_PASSWORD'] = self.password
        if hasattr(self, 'key'):
            env['GIT_SSH_COMMAND'] = (f'ssh'
                                      f' -o StrictHostKeyChecking=no'
                                      f' -o UserKnownHostsFile=/dev/null'
                                      f' -o PubkeyAcceptedKeyTypes=+ssh-dss'
                                      f' -i {self.key.file_path}')
        try:
            if self.is_git_repo(self.dst_path):
                url = self.normalize_git_url(self.get_git_url(self.dst_path), self.get_git_user(self.dst_path))
                if url != self.src_uri:
                    exec_command(f'git -C {self.dst_path} remote set-url origin {self.src_uri}')
                exec_command(f'git -C {self.dst_path} checkout {self.branch}')
                exec_command(f'git -C {self.dst_path} pull', env=env)
            else:
                exec_command(f'git clone -b {self.branch} {self.src_uri} {self.dst_path}', env=env)
            exec_command(f'chown -R {self.owner_uid}:{self.owner_uid} {self.dst_path}')
        except Exception:
            if hasattr(self, 'key'): self.key.delete()
            raise
