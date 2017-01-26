import abc
import os
import shutil

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class UnixAccountManager(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def create_user(self, name, uid, home_dir, pass_hash, gecos=""):
        pass

    @abc.abstractmethod
    def delete_user(self, name):
        pass

    @abc.abstractmethod
    def set_quota(self, uid, quota_bytes):
        pass

    @abc.abstractmethod
    def get_quota_used(self, uid):
        pass

    @abc.abstractmethod
    def get_all_quota_used(self):
        pass

    @abc.abstractmethod
    def create_authorized_keys(self, pub_key_string, uid, home_dir):
        pass

    @abc.abstractmethod
    def create_crontab(self, user_name, cron_tasks_list):
        pass

    @abc.abstractmethod
    def delete_crontab(self, user_name):
        pass

    @abc.abstractmethod
    def kill_user_processes(self, user_name):
        pass


class LinuxUserManager(UnixAccountManager):
    def create_user(self, name, uid, home_dir, pass_hash, gecos=""):
        taskexecutor.utils.exec_command("useradd "
                                        "--comment '{0}' "
                                        "--uid {1} "
                                        "--home {2} "
                                        "--password '{3}' "
                                        "--create-home "
                                        "--shell /bin/bash "
                                        "{4}".format(gecos, uid, home_dir, pass_hash, name))

    def delete_user(self, name):
        taskexecutor.utils.exec_command("userdel --force --remove {}".format(name))

    def set_quota(self, uid, quota_bytes):
        taskexecutor.utils.exec_command("setquota "
                                        "-g {0} 0 {1} "
                                        "0 0 /home".format(uid, int(quota_bytes / 1024)))

    def get_quota_used(self, uid):
        return self.get_all_quota_used()[uid]

    def get_all_quota_used(self):
        return taskexecutor.utils.repquota("vangp")["block_limit"]["used"]

    def create_authorized_keys(self, pub_key_string, uid, home_dir):
        ssh_dir = os.path.join(home_dir, ".ssh")
        authorized_keys_path = os.path.join(ssh_dir, "authorized_keys")
        if not os.path.exists(ssh_dir):
            os.mkdir(ssh_dir, mode=0o700)
        constructor = taskexecutor.constructor.Constructor()
        authorized_keys = constructor.get_conffile("basic",
                                                   authorized_keys_path,
                                                   owner_uid=uid,
                                                   mode=0o400)
        authorized_keys.body = pub_key_string
        authorized_keys.save()

    def create_crontab(self, user_name, cron_tasks_list):
        crontab_string = str()
        for task in cron_tasks_list:
            crontab_string = ("{0}"
                              "#{1.execTimeDescription}\n"
                              "{1.execTime} {1.command}\n").format(crontab_string, task)
        LOGGER.info("Installing '{0}' crontab for {1}".format(crontab_string, user_name))
        taskexecutor.utils.exec_command("crontab -u {} -".format(user_name), pass_to_stdin=crontab_string)

    def delete_crontab(self, user_name):
        if os.path.exists(os.path.join("/var/spool/cron/crontabs", user_name)):
            taskexecutor.utils.exec_command("crontab -u {} -r".format(user_name))

    def kill_user_processes(self, user_name):
        taskexecutor.utils.exec_command("killall -9 -u {} || true".format(user_name))


class FreebsdUserManager(UnixAccountManager):
    def _update_jailed_ssh(self, action, user_name):
        jailed_ssh_config = taskexecutor.constructor.Constructor().get_conffile(
                "lines", "/usr/jail/usr/local/etc/ssh/sshd_clients_config"
        )
        allow_users = jailed_ssh_config.get_lines("^AllowUsers", count=1).split(' ')
        getattr(allow_users, action)(user_name)
        jailed_ssh_config.replace_line("^AllowUsers", " ".join(allow_users))
        jailed_ssh_config.save()
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{}-a/ {print $12}') "
                                        "pgrep sshd | xargs kill -HUP".format(CONFIG.hostname),
                                        shell="/usr/local/bin/bash")

    def create_user(self, name, uid, home_dir, pass_hash, gecos=""):
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{0}-a/ {print $12}') "
                                        "pw useradd {1} "
                                        "-u {2} "
                                        "-d {3} "
                                        "-h - "
                                        "-s /usr/local/bin/bash "
                                        "-c '{4}'".format(CONFIG.hostname, name, uid, home_dir, gecos),
                                        shell="/usr/local/bin/bash")
        self._update_jailed_ssh("append", name)

    def delete_user(self, name):
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{0}-a/ {print $12}') "
                                        "pw userdel {1} -r".format(CONFIG.hostname, name),
                                        shell="/usr/local/bin/bash")
        self._update_jailed_ssh("remove", name)

    def set_quota(self, uid, quota_bytes):
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{0}-a/ {print $12}') "
                                        "edquota "
                                        "-g "
                                        "-e /home:0:{1} "
                                        "{0}".format(CONFIG.hostname, int(quota_bytes / 1024), uid),
                                        shell="/usr/local/bin/bash")

    def get_quota_used(self, uid):
        return self.get_all_quota_used()[uid]

    def get_all_quota_used(self):
        return taskexecutor.utils.repquota("vang", shell="/usr/local/bin/bash")

    def create_authorized_keys(self, pub_key_string, uid, home_dir):
        ssh_dir = os.path.join(home_dir, ".ssh")
        authorized_keys_path = os.path.join(ssh_dir, "authorized_keys")
        if not os.path.exists(ssh_dir):
            os.mkdir(ssh_dir, mode=0o700)
        constructor = taskexecutor.constructor.Constructor()
        authorized_keys = constructor.get_conffile("basic",
                                                   authorized_keys_path,
                                                   owner_uid=uid,
                                                   mode=0o400)
        authorized_keys.body = pub_key_string
        authorized_keys.save()

    def create_crontab(self, user_name, cron_tasks_list):
        crontab_string = str()
        for task in cron_tasks_list:
            crontab_string = ("{0}"
                              "{1.execTime} {1.command}\n").format(crontab_string, task)
        LOGGER.info("Installing '{0}' crontab for {1}".format(crontab_string, user_name))
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | awk -F'[ =]' '/{0}-a/ {print $12}') "
                                        "crontab -u {1} -".format(CONFIG.hostname, user_name),
                                        shell="/usr/local/bin/bash",
                                        pass_to_stdin=crontab_string)

    def delete_crontab(self, user_name):
        if os.path.exists(os.path.join("/usr/jail/var/cron/tabs", user_name)):
            taskexecutor.utils.exec_command("jexec "
                                            "$(jls -ns | "
                                            "awk -F'[ =]' '/{0}-a/ {print $12}') "
                                            "crontab -u {1} -r".format(CONFIG.hostname, user_name),
                                            shell="/usr/local/bin/bash")

    def kill_user_processes(self, user_name):
        taskexecutor.utils.exec_command("jexec "
                                        "$(jls -ns | "
                                        "awk -F'[ =]' '/{0}-a/ {print $12}') "
                                        "killall -9 -u {1} || true".format(CONFIG.hostname, user_name),
                                        shell="/usr/local/bin/bash")


class MaildirManager:
    def create_maildir(self, spool, dir, owner_uid):
        path = os.path.join(spool, dir)
        if not os.path.isdir(path):
            LOGGER.info("Creating directory {}".format(path))
            os.makedirs(path, mode=0o755, exist_ok=True)
        else:
            LOGGER.info("Maildir {} already exists".format(path))
        LOGGER.info("Setting owner {0} for {1}".format(owner_uid, path))
        os.chown(spool, owner_uid, owner_uid)
        os.chown(path, owner_uid, owner_uid)

    def delete_maildir(self, spool, dir):
        path = os.path.join(spool, dir)
        LOGGER.info("Removing {} recursively".format(path))
        shutil.rmtree(path)

    def get_quota_used(self, spool, dir):
        maildirsize_file = os.path.join(spool, dir, "maildirsize")
        size = 0
        if os.path.exists(maildirsize_file):
            with open(maildirsize_file, "r") as f:
                f.readline()
                line = f.readline()
                while line:
                    size += int(line.split()[0])
                    line = f.readline()
        else:
            size = sum([sum(map(lambda f: os.path.getsize(os.path.join(dir, f)), files))
                        for dir, _, files in os.walk(os.path.join(spool, dir))])
        return size


class Builder:
    def __new__(cls, service_type):
        if service_type == "LINUX_USER_MANAGER":
            return LinuxUserManager
        elif service_type == "FREEBSD9_USER_MANAGER":
            return FreebsdUserManager
        elif service_type.split("_")[1] == "MAILDIR":
            return MaildirManager
        else:
            raise BuilderTypeError("Unknown SysService type: {}".format(service_type))
