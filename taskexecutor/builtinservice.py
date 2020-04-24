import os
import shutil
import time
from typing import Set

import attr

import taskexecutor.constructor as cnstr
from taskexecutor.config import CONFIG
from taskexecutor.logger import LOGGER
from taskexecutor.utils import exec_command, rgetattr, repquota, CommandExecutionError

__all__ = ['LinuxUserManager', 'MaildirManager']


class MaildirManagerSecurityViolation(Exception): ...


class InconsistentData(Exception): ...


class InconsistentUserData(InconsistentData): ...


class InconsistentGroupData(InconsistentData): ...


class IdConflict(Exception): ...


class MalformedLine(Exception): ...


@attr.s(slots=True, auto_attribs=True, eq=True, frozen=True)
class User:
    name: str
    uid: int = attr.ib(converter=int)
    gid: int = attr.ib(converter=int)
    password_hash: str
    gecos: str
    home: str
    shell: str


@attr.s(slots=True, auto_attribs=True, eq=True, frozen=True)
class Group:
    name: str
    gid: int = attr.ib(converter=int)
    users: Set[User]


class LinuxUserManager:
    def __init__(self, sysconf_dir=None):
        sysconf_dir = sysconf_dir or rgetattr(CONFIG, 'builtinservice.sysconf_dir', '/opt/etc')
        self._etc_passwd = cnstr.get_conffile('lines', os.path.join(sysconf_dir, 'passwd'), 0, 0o644)
        self._etc_shadow = cnstr.get_conffile('lines', os.path.join(sysconf_dir, 'shadow'), 0, 0o640)
        self._etc_group = cnstr.get_conffile('lines', os.path.join(sysconf_dir, 'group'), 0, 0o644)
        self._etc_gshadow = cnstr.get_conffile('lines', os.path.join(sysconf_dir, 'gshadow'), 0, 0o640)

    @property
    def default_shell(self):
        return rgetattr(CONFIG, 'builtinservice.linux_user_manager.default_shell', '/bin/bash')

    @property
    def disabled_shell(self):
        return rgetattr(CONFIG, 'builtinservice.linux_user_manager.disabled_shell', '/bin/false')

    @staticmethod
    def _id_from_config(config, name):
        id = next(
            (next(
                (int(i) for i in l.split(':')[-2:-1]),
                None) for l in config.get_lines(f'^{name}:x:.+')),
            None)
        if not id:
            try:
                id = max(g for g in
                         (int(l.split(':')[2]) for l in config.get_lines('.*') if l)
                         if g < rgetattr(CONFIG, 'builtinservice.linux_user_manager.min_uid', 2000)) + 1
            except ValueError:
                id = 1000
        return id

    def get_user(self, name):
        passwd_matched = self._etc_passwd.get_lines(f'^{name}:.+')
        if len(passwd_matched) > 1:
            raise InconsistentUserData('More than one user has name {}:\n{}'.format(name, '\n'.join(passwd_matched)))
        if passwd_matched:
            passwd = passwd_matched[0].split(':')
            if len(passwd) != 7: raise MalformedLine('Bad passwd line:\n{}'.format(':'.join(passwd)))
            _, _, uid, gid, gecos, home, shell = passwd
            shadow_matched = self._etc_shadow.get_lines(f'^{name}:.+')
            if not shadow_matched: raise InconsistentUserData(f'User {name} has no shadow information')
            if len(shadow_matched) > 1:
                raise InconsistentUserData('User {} has more than one shadow entry:\n'
                                           '{}'.format(name, '\n'.join(passwd_matched)))
            shadow = shadow_matched[0].split(':')
            if len(shadow) != 9: raise MalformedLine('Bad shadow line:\n{}'.format(':'.join(shadow)))
            hash = shadow[1] if shadow[1] not in ('!', '*') else ''
            return User(name, uid, gid, hash, gecos, home, shell)

    def get_user_by_uid(self, uid):
        matched = self._etc_passwd.get_lines(f'^.+:x:{uid}:')
        if len(matched) > 1: raise IdConflict(f'Users with conflicting UID found: {matched}')
        if matched: return self.get_user(matched[0].split(':')[0])

    def get_group(self, name):
        group_matched = self._etc_group.get_lines(f'^{name}:.+')
        if len(group_matched) > 1:
            raise InconsistentGroupData('More than one group has name {}:\n{}'.format(name, '\n'.join(group_matched)))
        if group_matched:
            group = group_matched[0].split(':')
            if len(group) != 4: raise MalformedLine('Bad group line:\n{}'.format(':'.join(group)))
            users = set()
            same_name_user = self.get_user(name)
            if same_name_user: users.add(same_name_user)
            for each in group[3].split(','):
                if each:
                    user = self.get_user(each)
                    if not user:
                        raise InconsistentUserData(f'User {each} should be member of group {name} '
                                                   f'but it does not exist')
                    users.add(user)
            return Group(name, group[2], users)

    def get_group_by_gid(self, gid):
        matched = self._etc_group.get_lines(f'^.+:x:{gid}:')
        if len(matched) > 1: raise IdConflict(f'Groups with conflicting GID found: {matched}')
        if matched: return self.get_group(matched[0].split(':')[0])

    def create_group(self, name, gid=None):
        if not name: raise InconsistentGroupData('Cannot create group without name')
        gid = gid or self._id_from_config(self._etc_group, name)
        try:
            same_gid = self.get_group_by_gid(gid)
            if same_gid and same_gid.name != name: raise IdConflict(f'{same_gid} group has conflicting GID {gid}')
        except (InconsistentGroupData, MalformedLine):
            pass
        group_line = f'{name}:x:{gid}:'
        gshadow_line = f'{name}:!::'
        try:
            if not self.get_group(name):
                self._etc_group.add_line(group_line)
                self._etc_group.add_line()
                self._etc_gshadow.add_line(gshadow_line)
                self._etc_gshadow.add_line()
                self._etc_group.save()
                self._etc_gshadow.save()
        except (InconsistentGroupData, MalformedLine) as e:
            LOGGER.warning(f'{e}, removing all entries starting from {name}')
            for each in self._etc_group.get_lines(f'^{name}:'): self._etc_group.remove_line(each)
            for each in self._etc_gshadow.get_lines(f'^{name}:'): self._etc_gshadow.remove_line(each)
            self.create_group(name, gid)

    def add_user_to_group(self, user_name, group_name):
        group = self.get_group(group_name)
        user = self.get_user(user_name)
        if not group: raise InconsistentGroupData(f'No such group: {group_name}')
        if not user: raise InconsistentUserData(f'No such user: {user_name}')
        if user not in group.users:
            group.users.add(user)
            group_line = '{0.name}:x:{0.gid}:{1}'.format(group, ','.join(sorted((u.name for u in group.users))))
            gshadow_line = '{0.name}:!::{1}'.format(group, ','.join(sorted((u.name for u in group.users))))
            self._etc_group.replace_line(f'^{group.name}:.+', group_line)
            self._etc_gshadow.replace_line(f'^{group.name}:.+', gshadow_line)
            self._etc_group.save()
            self._etc_gshadow.save()

    def remove_user_from_group(self, user_name, group_name):
        group = self.get_group(group_name)
        if not group: raise InconsistentGroupData(f'No such group: {group_name}')
        user = next((u for u in group.users if u.name == user_name), None)
        if user in group.users:
            group.users.remove(user)
            group_line = '{0.name}:x:{0.gid}:{1}'.format(group, ','.join(sorted((u.name for u in group.users))))
            gshadow_line = '{0.name}:!::{1}'.format(group, ','.join(sorted((u.name for u in group.users))))
            self._etc_group.replace_line(f'^{group.name}:.+', group_line)
            self._etc_gshadow.replace_line(f'^{group.name}:.+', gshadow_line)
            self._etc_group.save()
            self._etc_gshadow.save()

    def create_user(self, name, uid, home_dir, pass_hash, shell, gecos='', extra_groups=None):
        if not name: raise InconsistentUserData('Cannot create user without name')
        try:
            user = self.get_user(name)
            if not user:
                days = int(time.time() / 3600 / 24)
                passwd_line = f'{name}:x:{uid}:{uid}:{gecos}:{home_dir}:{shell}'
                shadow_line = f'{name}:{pass_hash}:{days}:0:99999:7:::'
                self._etc_passwd.add_line(passwd_line)
                self._etc_passwd.add_line()
                self._etc_shadow.add_line(shadow_line)
                self._etc_shadow.add_line()
                self._etc_passwd.save()
                self._etc_shadow.save()
            elif user != User(name, uid, uid, pass_hash, gecos, home_dir, shell):
                raise InconsistentUserData(f'User {name} already exists: {user}, requested params: '
                                           f'UID={uid}, home={home_dir}, hash={pass_hash}, shell={shell}, GECOS={gecos}')
            self.create_group(name, uid)
            for each in extra_groups or []:
                self.create_group(each)
                self.add_user_to_group(name, each)
            os.makedirs(home_dir, 0o700, exist_ok=True)
            os.chown(home_dir, uid, uid)
            os.chmod(home_dir, 0o700)
        except InconsistentUserData as e:
            LOGGER.warning(f'{e} removing all entries starting with {name}')
            for each in self._etc_passwd.get_lines(f'^{name}:.+'): self._etc_passwd.remove_line(each)
            for each in self._etc_shadow.get_lines(f'^{name}:.+'): self._etc_shadow.remove_line(each)
            self.create_user(name, uid, home_dir, pass_hash, shell, gecos, extra_groups)

    def delete_user(self, name):
        home = f'/home/{name}'
        try:
            for each in self._etc_group.get_lines(f'.+(:|,){name},?'):
                group_name = next(iter(each.split(':')[0:1]), None)
                if group_name: self.remove_user_from_group(name, group_name)
        except InconsistentData as e:
            LOGGER.warning(e)
        try:
            home = self.get_user(name).home
        except (InconsistentUserData, MalformedLine) as e:
            LOGGER.warning(f'{e}, home directory would be {home}')
        for each in self._etc_group.get_lines(f'^{name}:.+'): self._etc_group.remove_line(each)
        for each in self._etc_gshadow.get_lines(f'^{name}:.+'): self._etc_gshadow.remove_line(each)
        for each in self._etc_passwd.get_lines(f'^{name}:.+'): self._etc_passwd.remove_line(each)
        for each in self._etc_shadow.get_lines(f'^{name}:.+'): self._etc_shadow.remove_line(each)
        self._etc_gshadow.save()
        self._etc_group.save()
        self._etc_passwd.save()
        self._etc_shadow.save()
        if os.path.exists(home): shutil.rmtree(home)

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
        authorized_keys = cnstr.get_conffile('basic', authorized_keys_path, owner_uid=uid, mode=0o400)
        authorized_keys.body = pub_key_string
        authorized_keys.save()

    def kill_user_processes(self, user_name):
        try:
            exec_command(f'killall -9 -u {user_name}')
        except CommandExecutionError as e:
            LOGGER.warning(f'Failed to kill {user_name} processes with killall: {e}')

    def set_shell(self, user_name, path):
        user = self.get_user(user_name)
        line = '{0.name}:x:{0.uid}:{0.uid}:{0.gecos}:{0.home}:{1}'.format(user, path)
        self._etc_passwd.replace_line(f'^{user.name}:.+', line)
        self._etc_passwd.save()

    def set_comment(self, user_name, comment):
        user = self.get_user(user_name)
        line = '{0.name}:x:{0.uid}:{0.uid}:{1}:{0.home}:{0.shell}'.format(user, comment)
        self._etc_passwd.replace_line(f'^{user.name}:.+', line)
        self._etc_passwd.save()

    def change_uid(self, user_name, uid):
        same_uid = self.get_user_by_uid(uid)
        if same_uid and same_uid.name != user_name:
            raise IdConflict(f'User with UID {uid} already exists: {same_uid.name}')
        elif same_uid and same_uid.name == user_name:
            LOGGER.debug(f'User {user_name} already has UID {uid}, nothing to do')
        else:
            if self.get_group(user_name):
                self._etc_group.replace_line(f'^{user_name}:.+', f'{user_name}:x:{uid}:')
                self._etc_group.save()
            else:
                self.create_group(user_name, uid)
            user = self.get_user(user_name)
            line = '{0.name}:x:{1}:{1}:{0.gecos}:{0.home}:{0.shell}'.format(user, uid)
            self._etc_passwd.replace_line(f'^{user_name}:.+', line)
            self._etc_passwd.save()
            exec_command(f'chown -R {uid}:{uid} {user.home}')


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
