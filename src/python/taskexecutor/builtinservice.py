import functools
import os
import shutil

from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.utils

__all__ = ["LinuxUserManager", "MaildirManager"]


class MaildirManagerSecurityViolation(Exception):
    pass


def with_sync_passwd(f):
    @functools.wraps(f)
    def wrapper(self, *args, **kwargs):
        f(self, *args, **kwargs)
        os.makedirs("/opt/etc", exist_ok=True)
        LOGGER.info("Copying /etc/{passwd,group,shadow,gshadow} to /opt/etc")
        for each in ("passwd", "group", "shadow", "gshadow"):
            shutil.copy2("/etc/{}".format(each), "/opt/etc")

    return wrapper


class LinuxUserManager:
    @property
    def default_shell(self):
        return "/bin/bash"

    @property
    def disabled_shell(self):
        return "/usr/sbin/nologin"

    @with_sync_passwd
    def create_group(self, name, gid=None, delete_first=False):
        if delete_first:
            taskexecutor.utils.exec_command("groupdel {} || true".format(name))
        setgid = "--gid {}".format(gid) if gid else ""
        taskexecutor.utils.exec_command("groupadd --force {0} {1}".format(setgid, name))

    @with_sync_passwd
    def create_user(self, name, uid, home_dir, pass_hash, shell, gecos="", extra_groups=[]):
        if os.path.exists(home_dir):
            os.chown(home_dir, uid, uid)
        self.create_group(name, gid=uid, delete_first=True)
        extra_groups = [g for g in extra_groups if g]
        for group in extra_groups:
            self.create_group(group)
        groups = ",".join(extra_groups) if extra_groups else '""'
        taskexecutor.utils.exec_command("useradd "
                                        "--comment '{0}' "
                                        "--uid {1} "
                                        "--gid {1} "
                                        "--home {2} "
                                        "--password '{3}' "
                                        "--create-home "
                                        "--shell {4} "
                                        "--groups {5} "
                                        "{6}".format(gecos, uid, home_dir, pass_hash, shell, groups, name))
        os.chmod(home_dir, 0o0700)

    @with_sync_passwd
    def delete_user(self, name):
        taskexecutor.utils.exec_command("userdel --force --remove {}".format(name))

    def set_quota(self, uid, quota_bytes):
        taskexecutor.utils.exec_command("setquota "
                                        "-g {0} 0 {1} "
                                        "0 0 /home".format(uid, int(quota_bytes / 1024) or 1))

    @taskexecutor.utils.synchronized
    def get_quota(self):
        return {k: v["block_limit"]["used"] * 1024
                for k, v in taskexecutor.utils.repquota("vangp").items()}

    def get_cpuacct(self, user_name):
        try:
            with open(os.path.join("/sys/fs/cgroup/cpuacct/limitgroup", user_name, "cpuacct.usage"), "r") as f:
                return int(f.read())
        except FileNotFoundError:
            return 0

    def create_authorized_keys(self, pub_key_string, uid, home_dir):
        ssh_dir = os.path.join(home_dir, ".ssh")
        authorized_keys_path = os.path.join(ssh_dir, "authorized_keys")
        if not os.path.exists(ssh_dir):
            os.makedirs(ssh_dir, mode=0o700, exist_ok=True)
            os.chown(ssh_dir, uid, uid)
        authorized_keys = taskexecutor.constructor.get_conffile("basic",
                                                                authorized_keys_path, owner_uid=uid, mode=0o400)
        authorized_keys.body = pub_key_string
        authorized_keys.save()

    def create_crontab(self, user_name, cron_tasks_list):
        crontab_string = str()
        for task in cron_tasks_list:
            crontab_string = ("{0}"
                              "#{1.execTimeDescription}\n"
                              "{1.execTime} {1.command}\n").format(crontab_string, task)
        LOGGER.debug("Installing '{0}' crontab for {1}".format(crontab_string, user_name))
        taskexecutor.utils.exec_command("crontab -u {} -".format(user_name), pass_to_stdin=crontab_string)

    def get_crontab(self, user_name):
        return taskexecutor.utils.exec_command("crontab -l -u {} | awk '$1!~/^#/ {{print}}'".format(user_name))

    def delete_crontab(self, user_name):
        if os.path.exists(os.path.join("/var/spool/cron/crontabs", user_name)):
            taskexecutor.utils.exec_command("crontab -u {} -r".format(user_name))

    def kill_user_processes(self, user_name):
        taskexecutor.utils.exec_command("killall -9 -u {} || true".format(user_name))

    def enable_sendmail(self, uid):
        taskexecutor.utils.exec_command("(/usr/bin/postfix_dbs_ctrl --db map --uid {0} --get && "
                                        "/usr/bin/postfix_dbs_ctrl --db map --uid {0} --del) || true".format(uid))

    def disable_sendmail(self, uid):
        taskexecutor.utils.exec_command("/usr/bin/postfix_dbs_ctrl --db map --uid {0} --get || "
                                        "/usr/bin/postfix_dbs_ctrl --db map --uid {0} --add".format(uid))

    @with_sync_passwd
    def set_shell(self, user_name, path):
        path = path or "/usr/sbin/nologin"
        taskexecutor.utils.exec_command("usermod --shell {0} {1}".format(path, user_name))

    @with_sync_passwd
    def set_comment(self, user_name, comment):
        taskexecutor.utils.exec_command("usermod --comment '{0}' {1}".format(comment, user_name))

    @with_sync_passwd
    def change_uid(self, user_name, uid):
        taskexecutor.utils.exec_command("groupmod --gid {0} {1}".format(uid, user_name))
        taskexecutor.utils.exec_command("usermod --uid {0} --gid {0} {1}".format(uid, user_name))


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
