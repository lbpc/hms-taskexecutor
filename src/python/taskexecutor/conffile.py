import re
import os
import shutil
import jinja2
from taskexecutor.logger import LOGGER

__all__ = ["Builder"]


class PropertyValidationError(Exception):
    pass


class BuilderTypeError(Exception):
    pass


class ConfigFile:
    def __init__(self, file_path, owner_uid, mode):
        self._body = ""
        self._owner_uid = owner_uid
        self._mode = mode
        self.file_path = file_path

    @property
    def body(self):
        if not self._body and self.exists:
            self._read_file()
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    @body.deleter
    def body(self):
        self._body = ""

    @property
    def exists(self):
        return os.path.exists(self.file_path)

    @property
    def _backup_file_path(self):
        backup_path = os.path.normpath("{0}/{1}".format("/var/tmp", self.file_path))
        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
        return backup_path

    def _read_file(self):
        with open(self.file_path, "r") as f:
            self._body = f.read()

    def write(self):
        dir_path = os.path.dirname(self.file_path)
        if not os.path.exists(dir_path):
            LOGGER.warning("There is no {} found, creating".format(dir_path))
            os.makedirs(dir_path)
        if os.path.exists(self.file_path):
            LOGGER.debug("Backing up {0} file as {1}".format(self.file_path, self._backup_file_path))
            shutil.move(self.file_path, self._backup_file_path)
        LOGGER.debug("Saving {} file".format(self.file_path))
        with open(self.file_path, "w") as f:
            f.write(self.body)
        if self._mode:
            os.chmod(self.file_path, self._mode)
        if self._owner_uid:
            os.chown(self.file_path, self._owner_uid, self._owner_uid)

    def revert(self):
        if os.path.exists(self._backup_file_path):
            LOGGER.warning(
                    "Reverting {0} from {1}, {0} will be saved as "
                    "/var/tmp/te_{2}".format(self.file_path,
                                             self._backup_file_path,
                                             self.file_path.replace("/", "_"))
            )
            shutil.move(self.file_path,
                        "/var/tmp/te_{}".format(self.file_path.replace("/", "_")))
            shutil.move(self._backup_file_path, self.file_path)

    def confirm(self):
        if os.path.exists(self._backup_file_path):
            LOGGER.debug("Removing {}".format(self._backup_file_path))
            try:
                os.unlink(self._backup_file_path)
            except FileNotFoundError as e:
                LOGGER.warning("Could not delete file cuz not exist, ERROR: {}".format(e))

    def save(self):
        self.write()
        self.confirm()

    def delete(self):
        LOGGER.debug("Deleting {} file".format(self.file_path))
        if os.path.exists(self.file_path):
            os.unlink(self.file_path)
        else:
            LOGGER.warn("{} doesn't exists")
        del self.body


class TemplatedConfigFile(ConfigFile):
    def __init__(self, file_path, owner_uid, mode):
        super().__init__(file_path, owner_uid, mode)
        self.template = None

    @staticmethod
    def _setup_jinja2_env():
        jinja2_env = jinja2.Environment()
        jinja2_env.filters["path_join"] = lambda paths: os.path.join(*paths)
        jinja2_env.filters["punycode"] = lambda domain: domain.encode("idna").decode()
        jinja2_env.filters["normpath"] = lambda path: os.path.normpath(path)
        jinja2_env.filters["dirname"] = lambda path: os.path.dirname(path)
        return jinja2_env

    def render_template(self, **kwargs):
        if not self.template:
            raise PropertyValidationError("Template is not set")
        jinja2_env = self._setup_jinja2_env()
        self.body = jinja2_env.from_string(self.template).render(**kwargs)


class LineBasedConfigFile(ConfigFile):
    def __init__(self, file_path, owner_uid, mode):
        super().__init__(file_path, owner_uid, mode)

    def _body_as_list(self):
        return str(self.body).split("\n") if self.body else []

    def has_line(self, line):
        return line in self._body_as_list()

    def get_lines(self, regex, count=-1):
        lines_list = self._body_as_list()
        ret_list = list()
        for line in lines_list:
            if count != 0 and re.match(regex, line):
                ret_list.append(line)
                count -= 1
        return ret_list

    def add_line(self, line):
        LOGGER.debug("Adding '{0}' to {1}".format(line, self.file_path))
        list = self._body_as_list()
        list.append(line)
        self.body = "\n".join(list)

    def remove_line(self, line):
        LOGGER.debug("Removing '{0}' from {1}".format(line, self.file_path))
        list = self._body_as_list()
        list.remove(line)
        self.body = "\n".join(list)

    def replace_line(self, regex, new_line, count=1):
        list = self._body_as_list()
        for idx, line in enumerate(list):
            if count != 0 and re.match(regex, line):
                LOGGER.debug("Replacing '{0}' by '{1}' in {2}".format(line, new_line, self.file_path))
                del list[idx]
                list.insert(idx, new_line)
                count -= 1
        self.body = "\n".join(list)


class Builder:
    def __new__(cls, config_type):
        ConfigFileClass = {"templated": TemplatedConfigFile,
                           "lines": LineBasedConfigFile,
                           "basic": ConfigFile}.get(config_type)
        if not ConfigFileClass:
            raise BuilderTypeError("Unknown config type: {}".format(config_type))
        return ConfigFileClass
