from abc import ABCMeta, abstractmethod
from taskexecutor.utils import ConfigFile, exec_command, set_apparmor_mode
from taskexecutor.logger import LOGGER


class OpService(metaclass=ABCMeta):
    def __init__(self, name=None):
        self._name = str()
        self._cfg_base = str()
        self._config = None
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
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        if not isinstance(value, ConfigFile):
            raise ValueError("config must be instance of ConfigFile class")
        self._config = value

    @config.deleter
    def config(self):
        del self._config

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

    def reload(self):
        LOGGER.info("Testing apache2 config in {}".format(self.cfg_base))
        exec_command("apache2ctl -d {} -t".format(self.cfg_base))
        super().reload()


# HACK: the two 'Unmanaged' classes below are responsible for
# reloading services at baton.intr only
# would be removed when this server is gone
class UnmanagedNginx(OpService):
    def __init__(self):
        super().__init__()
        self.name = "nginx"
        self.cfg_base = "/usr/local/nginx/conf"

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def restart(self):
        raise NotImplementedError

    def reload(self):
        LOGGER.info("Testing nginx config")
        exec_command("/usr/local/nginx/sbin/nginx -t")
        LOGGER.info("Reloading nginx")
        exec_command("/usr/local/nginx/sbin/nginx -s reload")


class UnmanagedApache(OpService):
    def __init__(self, name):
        super().__init__(name)
        if not self.name:
            raise Exception("Apache instance requires name keyword")
        self.cfg_base = "/usr/local/{}/conf".format(self.name)

    def start(self):
        raise NotImplementedError

    def stop(self):
        raise NotImplementedError

    def restart(self):
        raise NotImplementedError

    def reload(self):
        LOGGER.info("Testing apache config: "
                    "{}/conf/httpd.conf".format(self.cfg_base))
        exec_command(
                "/usr/sbin/jail "
                "/usr/jail t 127.0.0.1 "
                "{0}/bin/httpd -T -f {0}/conf/httpd.conf".format(self.cfg_base)
        )
        LOGGER.info("Reloading apache")
        exec_command("{}/bin/apachectl2 graceful".format(self.cfg_base))
