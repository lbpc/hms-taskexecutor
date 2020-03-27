import os
import shutil

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.constructor import get_conffile
from taskexecutor.utils import exec_command, rgetattr, repquota

__all__ = ["LinuxUserManager", "MaildirManager"]


class MaildirManagerSecurityViolation(Exception):
    pass


class LinuxUserManager:
    def create_group(self, name, gid=None):
        etc_group_file = os.path.join(rgetattr(CONFIG, 'builtinservice.sysconf_dir', '/opt/etc/group'), 'group')
        etc_group = get_conffile('lines', etc_group_file, 0, 0o644)
        etc_group.add_line(':'.join((name, 'x', str(gid), '')))
        etc_group.add_line()
        etc_group.save()
        etc_gshadow_file = os.path.join(rgetattr(CONFIG, 'builtinservice.sysconf_dir', '/opt/etc/gshadow'), 'gshadow')
        etc_gshadow = get_conffile('lines', etc_gshadow_file, 0, 0o640)
        etc_gshadow.add_line(':'.join((name, '!', '', '')))
        etc_gshadow.add_line()
        etc_gshadow.save()

    def create_user(self, name, uid, home_dir, pass_hash, shell, gecos='', extra_groups=None):
        ...

    def delete_user(self, name):
        ...

    def set_quota(self, uid, quota_bytes):
        exec_command('setquota -g {0} 0 {1} 0 0 /home'.format(uid, int(quota_bytes / 1024) or 1))

    def get_quota(self):
        return {k: v['block_limit']['used'] * 1024 for k, v in repquota('vangp').items()}

    def get_cpuacct(self, user_name):
        try:
            with open(os.path.join('/sys/fs/cgroup/cpuacct/limitgroup', user_name, 'cpuacct.usage'), 'r') as f:
                return int(f.read())
        except FileNotFoundError:
            return 0

    def create_authorized_keys(self, pub_key_string, uid, home_dir):
        ssh_dir = os.path.join(home_dir, '.ssh')
        authorized_keys_path = os.path.join(ssh_dir, 'authorized_keys')
        if not os.path.exists(ssh_dir):
            os.makedirs(ssh_dir, mode=0o700, exist_ok=True)
            os.chown(ssh_dir, uid, uid)
        authorized_keys = get_conffile('basic', authorized_keys_path, owner_uid=uid, mode=0o400)
        authorized_keys.body = pub_key_string
        authorized_keys.save()

    def kill_user_processes(self, user_name):
        ...

    def set_shell(self, user_name, path):
        ...

    def set_comment(self, user_name, comment):
        ...

    def change_uid(self, user_name, uid):
        ...


class MaildirManager:
    def normalize_spool(self, spool):
        spool = str(spool)
        basedir, domain = os.path.split(spool)
        return os.path.normpath(os.path.join(basedir, domain.encode("idna").decode()))

    def get_maildir_path(self, spool, dir):
        spool = self.normalize_spool(spool)
        path = os.path.normpath(os.path.join(spool, str(dir)))
        if os.path.commonprefix([spool, path]) != spool:
            raise MaildirManagerSecurityViolation("{0} is outside of mailspool {1}".format(path, spool))
        return path

    def create_maildir(self, spool, dir, owner_uid):
        path = self.get_maildir_path(spool, dir)
        spool = self.normalize_spool(spool)
        if not os.path.isdir(path):
            LOGGER.debug("Creating directory {}".format(path))
            os.makedirs(path, mode=0o755, exist_ok=True)
        else:
            LOGGER.info("Maildir {} already exists".format(path))
        LOGGER.debug("Setting owner {0} for {1}".format(owner_uid, path))
        os.chown(spool, owner_uid, owner_uid)
        os.chown(path, owner_uid, owner_uid)

    def delete_maildir(self, spool, dir):
        path = self.get_maildir_path(spool, dir)
        if os.path.exists(path):
            LOGGER.debug("Removing {} recursively".format(path))
            shutil.rmtree(path)
        else:
            LOGGER.warning("{} does not exist".format(path))

    def create_maildirsize_file(self, spool, dir, size, owner_uid):
        maildir_path = self.get_maildir_path(spool, dir)
        if not os.path.exists(maildir_path):
            LOGGER.warning("{} does not exist, creating".format(maildir_path))
            self.create_maildir(spool, dir, owner_uid)
        path = os.path.join(maildir_path, "maildirsize")
        if os.path.exists(path):
            LOGGER.info("Removing old {}".format(path))
            os.unlink(path)
        LOGGER.info("Creating new {}".format(path))
        with open(path, "w") as f:
            f.write("0S,0C\n")
            f.write("{} 1\n".format(size))
        os.chown(path, owner_uid, owner_uid)

    def get_maildir_size(self, spool, dir):
        path = self.get_maildir_path(spool, dir)
        maildirsize_file = os.path.join(path, "maildirsize")
        if os.path.exists(maildirsize_file):
            with open(maildirsize_file, "r") as f:
                f.readline()
                return sum([int(next(iter(l.split()), 0)) for l in f.readlines() if l])
        return 0

    def get_real_maildir_size(self, spool, dir):
        path = self.get_maildir_path(spool, dir)
        LOGGER.info("Calculating real {} size".format(path))
        return sum([sum(map(lambda f: os.path.getsize(os.path.join(d, f)), files)) for d, _, files in os.walk(path)])
