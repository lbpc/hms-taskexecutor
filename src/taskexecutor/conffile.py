import re
import os
from jinja2 import Environment
from taskexecutor.logger import LOGGER

__all__ = ["Builder"]


class PropertyValidationError(Exception):
    pass


class BuilderTypeError(Exception):
    pass


class ConfigFile:
    def __init__(self, abs_path, owner_uid, mode):
        self._file_path = None
        self._body = None
        self._owner_uid = owner_uid
        self._mode = mode
        self.file_path = abs_path

    @property
    def file_path(self):
        return self._file_path

    @file_path.setter
    def file_path(self, value):
        self._file_path = value

    @file_path.deleter
    def file_path(self):
        del self._file_path

    @property
    def body(self):
        if not self._body:
            self._read_file()
        return self._body

    @body.setter
    def body(self, value):
        self._body = value

    @body.deleter
    def body(self):
        del self._body

    def _read_file(self):
        with open(self._file_path, "r") as f:
            self._body = f.read()

    def write(self):
        dir_path = os.path.dirname(self.file_path)
        if not os.path.exists(dir_path):
            LOGGER.warning("There is no {} found, creating".format(dir_path))
            os.makedirs(dir_path)
        if os.path.exists(self.file_path):
            LOGGER.info("Backing up {0} file as {0}.old".format(self.file_path))
            os.rename(self.file_path, "{}.old".format(self.file_path))
        LOGGER.info("Saving {} file".format(self.file_path))
        with open(self.file_path, "w") as f:
            f.write(self.body)
        if self._mode:
            os.chmod(self.file_path, self._mode)
        if self._owner_uid:
            os.chown(self.file_path, self._owner_uid, self._owner_uid)

    def revert(self):
        if os.path.exists("{}.old".format(self.file_path)):
            LOGGER.warning(
                    "Reverting {0} from {0}.old, {0} will be saved as "
                    "/tmp/te_{1}".format(self.file_path,
                                         self.file_path.replace("/", "_"))
            )
            os.rename(self.file_path,
                      "/tmp/te_{}".format(self.file_path.replace("/", "_")))
            os.rename("{}.old".format(self.file_path), self.file_path)

    def confirm(self):
        if os.path.exists("{}.old".format(self.file_path)):
            LOGGER.info("Removing {}.old".format(self.file_path))
            os.unlink("{}.old".format(self.file_path))

    def save(self):
        self.write()
        self.confirm()

    def delete(self):
        LOGGER.info("Deleting {} file".format(self.file_path))
        os.unlink(self.file_path)
        del self.body


class SwitchableConfigFile(ConfigFile):
    def __init__(self, abs_path, owner_uid, mode):
        super().__init__(abs_path, owner_uid, mode)
        self._enabled_path = None

    @property
    def enabled_path(self):
        if not self._enabled_path and "sites-available" in self.file_path:
            return self.file_path.replace("available", "enabled")
        else:
            return self._enabled_path

    @enabled_path.setter
    def enabled_path(self, value):
        self._enabled_path = value

    @enabled_path.deleter
    def enabled_path(self):
        del self._enabled_path

    @property
    def is_enabled(self):
        if not self.enabled_path:
            raise PropertyValidationError("enabled_path property is not set ")
        return os.path.exists(self.enabled_path) and os.path.islink(
                self.enabled_path) and os.readlink(
                self.enabled_path) == self.file_path

    def enable(self):
        if not self.enabled_path:
            raise PropertyValidationError("enabled_path property is not set ")
        LOGGER.info("Linking {0} to {1}".format(self.file_path,
                                                self.enabled_path))
        os.symlink(self.file_path, self.enabled_path)

    def disable(self):
        if not self.enabled_path:
            raise PropertyValidationError("enabled_path property is not set ")
        LOGGER.info("Unlinking {}".format(self.enabled_path))
        os.unlink(self.enabled_path)


class TemplatedConfigFile(ConfigFile):
    def __init__(self, abs_path, owner_uid, mode):
        super().__init__(abs_path, owner_uid, mode)
        self._template = None

    @property
    def template(self):
        return self._template

    @template.setter
    def template(self, value):
        self._template = value

    @template.deleter
    def template(self):
        del self._template

    @staticmethod
    def _setup_jinja2_env():
        jinja2_env = Environment()
        jinja2_env.filters["path_join"] = lambda paths: os.path.join(*paths)
        return jinja2_env

    def render_template(self, **kwargs):
        if not self.template:
            raise PropertyValidationError("Template is not set")
        jinja2_env = self._setup_jinja2_env()
        self.body = jinja2_env.from_string(self.template).render(**kwargs)


class LineBasedConfigFile(ConfigFile):
    def __init__(self, abs_path, owner_uid, mode):
        super().__init__(abs_path, owner_uid, mode)

    def _body_as_list(self):
        return self.body.split("\n")

    def has_line(self, line):
        return line in self._body_as_list()

    def get_lines(self, regex, count=-1):
        list = self._body_as_list()
        ret_list = list()
        for line in list:
            if count != 0 and re.match(regex, line):
                ret_list.append(line)
                count -= 1
        return ret_list

    def add_line(self, line):
        LOGGER.info("Adding '{0}' to {1}".format(line, self.file_path))
        list = self._body_as_list().append(line)
        self.body = "\n".join(list)

    def remove_line(self, line):
        LOGGER.info("Removing '{0}' from {1}".format(line, self.file_path))
        list = self._body_as_list().remove(line)
        self.body = "\n".join(list)

    def replace_line(self, regex, new_line, count=1):
        list = self._body_as_list()
        for idx, line in enumerate(list):
            if count != 0 and re.match(regex, line):
                LOGGER.info("Replacing '{0}' by '{1}' in {2}".format(line, new_line, self.file_path))
                del list[idx]
                list.insert(idx, new_line)
                count -= 1
        self.body = "\n".join(list)


class WebSiteConfigFile(TemplatedConfigFile, SwitchableConfigFile):
    pass


class Builder:
    def __new__(cls, config_type):
        if config_type == "website":
            return WebSiteConfigFile
        elif config_type == "templated":
            return TemplatedConfigFile
        elif config_type == "lines":
            return LineBasedConfigFile
        elif config_type == "basic":
            return ConfigFile
        else:
            raise BuilderTypeError("Unknown config type: {}".format(config_type))
