import logging
import os
import socket
import time

from taskexecutor.httpsclient import ConfigServerClient, ApiClient
from taskexecutor.logger import LOGGER
from taskexecutor.utils import asdict


class PropertyValidationError(Exception):
    pass


_REMOTE_CONFIG_TIMESTAMP = 0
_REMOTE_CONFIG_STALE = False
_REMOTE_CONFIG_TTL = os.environ.get('REMOTE_CONFIG_TTL') or 60


class __Config:
    @classmethod
    def __init__(cls):
        log_level = os.environ.get('LOG_LEVEL') or 'INFO'
        log_level = getattr(logging, log_level.upper())
        LOGGER.setLevel(log_level)
        LOGGER.debug('Initializing config')
        cls.hostname = socket.gethostname().split('.')[0]
        cls.profile = os.environ.get('TE_CONFIG_PROFILE', 'dev')
        cls.apigw = dict(host=os.environ.get('APIGW_HOST', 'api.intr'),
                         port=int(os.environ.get('APIGW_PORT', 443)),
                         user=os.environ.get('APIGW_USER', 'service'),
                         password=os.environ.get('APIGW_PASSWORD'))
        LOGGER.debug('Effective configuration:{}'.format(cls))

    @classmethod
    def _fetch_remote_properties(cls):
        LOGGER.info('Fetching properties from config server')
        with ConfigServerClient(**cls.apigw) as cfg_srv:
            extra_attrs = ['amqp.host=rabbit.intr',
                           'amqp.port=5672',
                           f'amqp.consumer_routing_key=te.{cls.hostname}']
            for k, v in os.environ.items():
                if k.startswith('TE_'):
                    k = k[3:].lower().replace('_', '.').replace('-', '_')
                    extra_attrs.append(f'{k}={v}')
            cfg_srv.extra_attrs = extra_attrs
            props = cfg_srv.te(cls.profile).get().propertySources[0].source
            for attr, value in asdict(props).items():
                if not attr.startswith('_'):
                    setattr(cls, attr, value)
        with ApiClient(**cls.apigw) as api:
            result = api.Server(query={'name': cls.hostname}).get()
            if len(result) > 1:
                raise PropertyValidationError(f'There is more than one server with name {cls.hostname}: {result}')
            elif len(result) == 0:
                raise PropertyValidationError(f'No {cls.hostname} server found')
            cls.localserver = result[0]
        global _REMOTE_CONFIG_TIMESTAMP
        _REMOTE_CONFIG_TIMESTAMP = time.time()
        global _REMOTE_CONFIG_STALE
        _REMOTE_CONFIG_STALE = False
        if not hasattr(cls, 'role'): raise PropertyValidationError('No role descriptions found')
        enabled_resources = list()
        for server_role in cls.localserver.serverRoles:
            server_role_attr = server_role.name.replace('-', '_')
            if hasattr(cls.role, server_role_attr):
                config_role = getattr(cls.role, server_role_attr)
                if isinstance(config_role.resources, list):
                    enabled_resources.extend(config_role.resources)
                else:
                    enabled_resources.append(config_role.resources)
        cls.enabled_resources = set(enabled_resources)
        LOGGER.info('Server roles: {}, manageable '
                    'resources: {}'.format([r.name for r in cls.localserver.serverRoles], enabled_resources))

    @classmethod
    def __getattr__(cls, item):
        LOGGER.warn(item)
        value = getattr(cls, item, None)
        global _REMOTE_CONFIG_STALE
        if not value or _REMOTE_CONFIG_STALE:
            if not _REMOTE_CONFIG_STALE: LOGGER.warning(f'{item} not found in config')
            cls._fetch_remote_properties()
            LOGGER.debug(f'Effective configuration:{cls}')
            value = getattr(cls, item)
        return value

    @classmethod
    def __setattr__(cls, name, value):
        if hasattr(cls, name) and not name.startswith('_'): raise AttributeError(f'{name} is a read-only attribute')
        setattr(cls, name, value)

    def __getattribute__(self, item):
        global _REMOTE_CONFIG_STALE
        global _REMOTE_CONFIG_TIMESTAMP
        global _REMOTE_CONFIG_TTL
        if not item.startswith('_') and time.time() - _REMOTE_CONFIG_TIMESTAMP > _REMOTE_CONFIG_TTL:
            _REMOTE_CONFIG_STALE = True
            raise AttributeError
        return super().__getattribute__(item)

    def __str__(self):
        attr_list = list()
        for attr, value in vars(self).items():
            if not attr.startswith('_') and not callable(getattr(self, attr)):
                attr_list.append(f'{attr}={value}')
        return 'CONFIG({})'.format(', '.join(attr_list))


CONFIG = __Config()
