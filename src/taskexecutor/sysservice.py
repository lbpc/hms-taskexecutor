import abc
import os
import shutil

from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
import taskexecutor.constructor
import taskexecutor.baseservice
import taskexecutor.utils

__all__ = ["Builder"]


class BuilderTypeError(Exception):
    pass


class UnixAccountManager(metaclass=abc.ABCMeta):
    @property
    @abc.abstractmethod
    def default_shell(self):
        return

    @property
    @abc.abstractmethod
    def disabled_shell(self):
        return

    @abc.abstractmethod
    def create_user(self, name, uid, home_dir, pass_hash, shell, gecos=""):
        pass

    @abc.abstractmethod
    def delete_user(self, name):
        pass

    @abc.abstractmethod
    def set_quota(self, uid, quota_bytes):
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

    @abc.abstractmethod
    def enable_sendmail(self, uid):
        pass

    @abc.abstractmethod
    def disable_sendmail(self, uid):
        pass

    @abc.abstractmethod
    def set_shell(self, user_name, path):
        pass


class LinuxUserManager(UnixAccountManager):
    @property
    def default_shell(self):
        return "/bin/bash"

    @property
    def disabled_shell(self):
        return "/usr/sbin/nologin"

    def create_user(self, name, uid, home_dir, pass_hash, shell, gecos=""):
        if os.path.exists(home_dir):
            os.chown(home_dir, uid, uid)
        taskexecutor.utils.exec_command("useradd "
                                        "--comment '{0}' "
                                        "--uid {1} "
                                        "--home {2} "
                                        "--password '{3}' "
                                        "--create-home "
                                        "--shell {4} "
                                        "{5}".format(gecos, uid, home_dir, pass_hash, shell, name))

    def delete_user(self, name):
        taskexecutor.utils.exec_command("userdel --force --remove {}".format(name))

    def set_quota(self, uid, quota_bytes):
        taskexecutor.utils.exec_command("setquota "
                                        "-g {0} 0 {1} "
                                        "0 0 /home".format(uid, int(quota_bytes / 1024)))

    @taskexecutor.utils.synchronized
    def get_quota(self):
        return {k: v["block_limit"]["used"] * 1024
                for k, v in taskexecutor.utils.repquota("vangp").items()}

    def create_authorized_keys(self, pub_key_string, uid, home_dir):
        ssh_dir = os.path.join(home_dir, ".ssh")
        authorized_keys_path = os.path.join(ssh_dir, "authorized_keys")
        if not os.path.exists(ssh_dir):
            os.mkdir(ssh_dir, mode=0o700)
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

    def set_shell(self, user_name, path):
        path = path or "/usr/sbin/nologin"
        taskexecutor.utils.exec_command("usermod --shell {0} {1}".format(path, user_name))


class FreebsdUserManager(UnixAccountManager):
    def __init__(self):
        self._jail_id = int()

    @property
    def default_shell(self):
        return "/usr/local/bin/bash"

    @property
    def disabled_shell(self):
        return "/usr/sbin/nologin"

    @property
    def jail_id(self):
        if not self._jail_id:
            with open("/var/run/jail_j.id", "r") as f:
                self._jail_id = int(f.read())
        return self._jail_id

    def _uid_to_mail_sender(self, uid):
        username = taskexecutor.utils.exec_command("jexec {0} pw usershow {1}".format(self.jail_id, uid))[0]
        return "{0}@{1}-a.majordomo.ru".format(username, CONFIG.hostname)

    def _update_jailed_ssh(self, action, user_name):
        jailed_ssh_config = taskexecutor.constructor.get_conffile("lines",
                                                                  "/usr/jail/usr/local/etc/ssh/sshd_clients_config")
        allow_users = jailed_ssh_config.get_lines("^AllowUsers", count=1).split(' ')
        getattr(allow_users, action)(user_name)
        jailed_ssh_config.replace_line("^AllowUsers", " ".join(allow_users))
        jailed_ssh_config.save()
        taskexecutor.utils.exec_command("jexec {} "
                                        "pgrep sshd | xargs kill -HUP".format(self.jail_id),
                                        shell=self.default_shell)

    def create_user(self, name, uid, home_dir, pass_hash, shell, gecos=""):
        taskexecutor.utils.exec_command("jexec {0} "
                                        "pw useradd {1} "
                                        "-u {2} "
                                        "-d {3} "
                                        "-h - "
                                        "-s {4} "
                                        "-c '{5}'".format(self.jail_id, name, uid, home_dir, shell, gecos),
                                        shell=self.default_shell)
        self._update_jailed_ssh("append", name)

    def delete_user(self, name):
        taskexecutor.utils.exec_command("jexec {0} "
                                        "pw userdel {1} -r".format(self.jail_id, name),
                                        shell=self.default_shell)
        self._update_jailed_ssh("remove", name)

    def set_quota(self, uid, quota_bytes):
        taskexecutor.utils.exec_command("jexec {0} "
                                        "edquota "
                                        "-g "
                                        "-e /home:0:{1} "
                                        "{0}".format(self.jail_id, int(quota_bytes) / 1024, uid),
                                        shell=self.default_shell)

    @taskexecutor.utils.synchronized
    def get_quota(self):
        return {k: v["block_limit"]["used"]
                for k, v in taskexecutor.utils.repquota("vang", shell=self.default_shell).items()}

    def create_authorized_keys(self, pub_key_string, uid, home_dir):
        ssh_dir = os.path.join(home_dir, ".ssh")
        authorized_keys_path = os.path.join(ssh_dir, "authorized_keys")
        if not os.path.exists(ssh_dir):
            os.mkdir(ssh_dir, mode=0o700)
        authorized_keys = taskexecutor.constructor.get_conffile("basic",
                                                                authorized_keys_path, owner_uid=uid, mode=0o400)
        authorized_keys.body = pub_key_string
        authorized_keys.save()

    def create_crontab(self, user_name, cron_tasks_list):
        crontab_string = str()
        for task in cron_tasks_list:
            crontab_string = ("{0}"
                              "{1.execTime} {1.command}\n").format(crontab_string, task)
        LOGGER.debug("Installing '{0}' crontab for {1}".format(crontab_string, user_name))
        taskexecutor.utils.exec_command("jexec {0} "
                                        "crontab -u {1} -".format(self.jail_id, user_name),
                                        shell=self.default_shell,
                                        pass_to_stdin=crontab_string)

    def delete_crontab(self, user_name):
        if os.path.exists(os.path.join("/usr/jail/var/cron/tabs", user_name)):
            taskexecutor.utils.exec_command("jexec {0} "
                                            "crontab -u {1} -r".format(self.jail_id, user_name),
                                            shell=self.default_shell)

    def kill_user_processes(self, user_name):
        taskexecutor.utils.exec_command("jexec {0} "
                                        "killall -9 -u {1} || true".format(self.jail_id, user_name),
                                        shell=self.default_shell)

    def enable_sendmail(self, uid):
        local_sender = self._uid_to_mail_sender(uid)
        deny_local_senders_file = taskexecutor.constructor.get_conffile(
                "lines", "/usr/jail/usr/local/etc/exim/deny_local_senders"
        )
        if deny_local_senders_file.has_line(local_sender):
            deny_local_senders_file.remove_line(local_sender)
            deny_local_senders_file.save()

    def disable_sendmail(self, uid):
        local_sender = self._uid_to_mail_sender(uid)
        deny_local_senders_file = taskexecutor.constructor.get_conffile(
                "lines", "/usr/jail/usr/local/etc/exim/deny_local_senders"
        )
        if not deny_local_senders_file.has_line(local_sender):
            deny_local_senders_file.add_line(local_sender)
            deny_local_senders_file.save()

    def set_shell(self, user_name, path):
        path = path or "/usr/sbin/nologin"
        taskexecutor.utils.exec_command("jexec {0} "
                                        "pw usermod -s {1} -n {2}".format(self.jail_id, path, user_name),
                                        shell=self.default_shell)


class MaildirManager:
    def create_maildir(self, spool, dir, owner_uid):
        path = os.path.join(spool, dir)
        if not os.path.isdir(path):
            LOGGER.debug("Creating directory {}".format(path))
            os.makedirs(path, mode=0o755, exist_ok=True)
        else:
            LOGGER.info("Maildir {} already exists".format(path))
        LOGGER.debug("Setting owner {0} for {1}".format(owner_uid, path))
        os.chown(spool, owner_uid, owner_uid)
        os.chown(path, owner_uid, owner_uid)

    def delete_maildir(self, spool, dir):
        path = os.path.join(str(spool), str(dir))
        if os.path.exists(path):
            LOGGER.debug("Removing {} recursively".format(path))
            shutil.rmtree(path)
        else:
            LOGGER.warning("{} does not exist".format(path))

    def get_maildir_size(self, path):
        maildirsize_file = os.path.join(path, "maildirsize")
        if os.path.exists(maildirsize_file):
            with open(maildirsize_file, "r") as f:
                f.readline()
                size = sum([int(l.split()[0]) for l in f.readlines()])
        else:
            size = sum([sum(map(lambda f: os.path.getsize(os.path.join(dir, f)), files))
                        for dir, _, files in os.walk(path)])
        return size


class Builder:
    def __new__(cls, service_type):
        SysServiceClass = {service_type == "LINUX_USER_MANAGER": LinuxUserManager,
                           service_type == "FREEBSD9_USER_MANAGER": FreebsdUserManager,
                           service_type.split("_")[1] == "MAILDIR": MaildirManager}.get(True)
        if not SysServiceClass:
            raise BuilderTypeError("Unknown SysService type: {}".format(service_type))
        return SysServiceClass
