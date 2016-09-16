from abc import ABCMeta, abstractmethod
from taskexecutor.utils import exec_command, set_apparmor_mode
from taskexecutor.logger import LOGGER


class OpService(metaclass=ABCMeta):
    def __init__(self, name=None):
        self._name = str()
        self._cfg_base = str()
        self._instance_id = None
        if name:
            self.name = name

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @name.deleter
    def name(self):
        del self._name

    @property
    def cfg_base(self):
        return self._cfg_base

    @cfg_base.setter
    def cfg_base(self, value):
        self._cfg_base = value

    @cfg_base.deleter
    def cfg_base(self):
        del self._cfg_base

    @property
    def instance_id(self):
        return self._instance_id

    @instance_id.setter
    def instance_id(self, value):
        self._instance_id = value

    @instance_id.deleter
    def instance_id(self):
        del self._instance_id

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def restart(self):
        pass

    @abstractmethod
    def reload(self):
        pass


class UpstartService(OpService):
    def start(self):
        LOGGER.info("starting {} service via Upstart".format(self.name))
        exec_command("start {}".format(self.name))

    def stop(self):
        LOGGER.info("stopping {} service via Upstart".format(self.name))
        exec_command("stop {}".format(self.name))

    def restart(self):
        LOGGER.info("restarting {} service via Upstart".format(self.name))
        exec_command("restart {}".format(self.name))

    def reload(self):
        LOGGER.info("reloading {} service via Upstart".format(self.name))
        exec_command("reload {}".format(self.name))


class SysVService(OpService):
    def start(self):
        LOGGER.info("starting {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} start".format(self.name))

    def stop(self):
        LOGGER.info("stopping {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} stop".format(self.name))

    def restart(self):
        LOGGER.info("restarting {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} restart".format(self.name))

    def reload(self):
        LOGGER.info("reloading {} service via init script".format(self.name))
        exec_command("invoke-rc.d {} reload".format(self.name))


class Nginx(SysVService):
    def __init__(self):
        super().__init__()
        self.name = "nginx"
        self.cfg_base = "/etc/nginx"
        self.config_body = str()
        self.available_cfg_path = str()
        self.enabled_cfg_path = str()

    def reload(self):
        LOGGER.info("Testing nginx config")
        exec_command("nginx -t")
        super().reload()
        set_apparmor_mode("enforce", "/usr/sbin/nginx")


class Apache(UpstartService):
    def __init__(self, name):
        super().__init__(name)
        if not self.name:
            raise Exception("Apache instance requires name keyword")
        self.cfg_base = "/etc/{}".format(self.name)
        self.config_body = str()
        self.available_cfg_path = str()
        self.enabled_cfg_path = str()

    def reload(self):
        LOGGER.info("Testing apache2 config in {}".format(self.cfg_base))
        exec_command("apache2ctl -d {} -t".format(self.cfg_base))
        super().reload()
