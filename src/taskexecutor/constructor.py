import urllib.parse
from types import SimpleNamespace

from taskexecutor.backup import ResticBackup
from taskexecutor.builtinservice import *
from taskexecutor.conffile import ConfigFile, TemplatedConfigFile, LineBasedConfigFile
from taskexecutor.config import CONFIG
from taskexecutor.executor import Executor
from taskexecutor.httpsclient import ApiClient
from taskexecutor.listener import *
from taskexecutor.logger import LOGGER
from taskexecutor.opservice import *
from taskexecutor.opservice import DockerService, NetworkingService, ConfigurableService
from taskexecutor.reporter import *
from taskexecutor.rescollector import *
from taskexecutor.resdatafetcher import *
from taskexecutor.resdataprocessor import *
from taskexecutor.resprocessor import *


class ClassSelectionError(Exception):
    pass


class OpServiceNotFound(Exception):
    pass


SERVICE_ID_TO_OPSERVICE_MAPPING = dict()


def get_conffile(config_type, abs_path, owner_uid=None, mode=None):
    Conffile = {'templated': TemplatedConfigFile,
                'lines': LineBasedConfigFile,
                'basic': ConfigFile}.get(config_type)
    if not Conffile: raise ClassSelectionError(f'Unknown config type: {config_type}')
    return Conffile(abs_path, owner_uid, mode)


def get_services_of_type(type_name):
    with ApiClient(**CONFIG.apigw) as api:
        return (get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
                if s.template.__class__.__name__ == type_name)


def get_http_proxy_service():
    return next(get_services_of_type('HttpServer'), None)


def get_application_servers():
    return get_services_of_type('ApplicationServer')


def get_database_server():
    return next(get_services_of_type('DatabaseServer'), None)


def get_mta_service():
    return next(get_services_of_type('Postfix'), None)


def get_cron_service():
    return next(get_services_of_type('CronD'), None)


def get_ssh_service():
    return next(get_services_of_type('SshD'), None)


def get_ftp_service():
    return next(get_services_of_type('FtpD'), None)


def get_opservice(service):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    opservice = SERVICE_ID_TO_OPSERVICE_MAPPING.get(service.id)
    if not opservice:
        LOGGER.debug(f"service template name is '{service.template.name}'")
        t_name = service.template.__class__.__name__
        superv = service.template.supervisionType
        private = service.template.availableToAccounts
        t_mod = getattr(service.template, 'type', None)
        OpService = {
            superv == 'docker': SomethingInDocker,
            t_name == 'CronD': Cron,
            t_name == 'Postfix': Postfix,
            t_name == 'HttpServer': HttpServer,
            t_name == 'ApplicationServer': Apache,
            t_name == 'ApplicationServer' and superv == 'docker': SharedAppServer,
            t_name == 'ApplicationServer' and superv == 'docker' and private: PersonalAppServer,
            t_name == 'DatabaseServer' and t_mod == 'MYSQL': MySQL,
            t_name == 'DatabaseServer' and t_mod == 'POSTGRESQL': PostgreSQL
        }.get(True)
        if not OpService: raise ClassSelectionError(f"Unknown OpService type: {t_name} "
                                                    f"and catch-all 'SomethingInDocker' did not match "
                                                    f"due to '{superv}' supervision")
        service_name = service.name.lower().split('@')[0]
        if hasattr(service, 'accountId') and service.accountId:
            service_name += '-' + service.id
        LOGGER.debug(f"service name will be '{service_name}'")
        opservice = OpService(service_name, service)
        if isinstance(opservice, DockerService):
            LOGGER.debug(f'{service_name} is dockerized service')
        if isinstance(opservice, PersonalAppServer):
            LOGGER.debug(f'{service_name} is personal application server')
        if isinstance(opservice, NetworkingService):
            LOGGER.debug(f'{service_name} is networking service')
            for socket in service.sockets:
                opservice.set_socket(socket.protocol or 'default', socket)
        if isinstance(opservice, ConfigurableService):
            LOGGER.debug(f'{service_name} is configurable service')
            for each in service.template.configTemplates:
                opservice.set_config(each.pathTemplate or each.name, each.fileLink, each.context)
        SERVICE_ID_TO_OPSERVICE_MAPPING[service.id] = opservice
    return opservice


def get_opservice_by_resource(resource, resource_type):
    global SERVICE_ID_TO_OPSERVICE_MAPPING
    if hasattr(resource, 'serverId') and resource_type != 'service':
        BuiltinService = {'unix-account': LinuxUserManager, 'mailbox': MaildirManager}.get(resource_type)
        if not BuiltinService: raise ClassSelectionError(f"Resource has 'serverId' property, "
                                                         f"but no built-in service exist for {resource_type}")
        service = BuiltinService()
    elif hasattr(resource, 'serviceId'):
        service = SERVICE_ID_TO_OPSERVICE_MAPPING.get(resource.serviceId)
        if not service:
            with ApiClient(**CONFIG.apigw) as api:
                service = get_opservice(api.Service(resource.serviceId).get())
    elif hasattr(resource, 'template'):
        service = get_opservice(resource)
    elif resource_type == 'ssl-certificate':
        service = get_http_proxy_service()
    else:
        raise OpServiceNotFound(f"Cannot find operational service for given '{resource_type}' resource: {resource}")
    return service


def get_all_opservices_by_res_type(resource_type):
    with ApiClient(**CONFIG.apigw) as api:
        next((get_opservice(s) for s in api.server(CONFIG.localserver.id).get().services
              if s.template.resourceType == resource_type.upper()), None)


def get_extra_services(worker):
    if isinstance(worker, WebSiteProcessor):
        return SimpleNamespace(http_proxy=get_http_proxy_service(),
                               old_app_server=get_opservice_by_resource(worker.op_resource, 'website')
                               if worker.op_resource else None)
    elif isinstance(worker, (UnixAccountProcessor, UnixAccountCollector)):
        return SimpleNamespace(mta=get_mta_service(), cron=get_cron_service())


def get_resprocessor(resource_type, resource, params=None):
    ResProcessor = {'service': ServiceProcessor,
                    'unix-account': UnixAccountProcessor,
                    'database-user': DatabaseUserProcessor,
                    'database': DatabaseProcessor,
                    'website': WebSiteProcessor,
                    'ssl-certificate': SslCertificateProcessor,
                    'mailbox': MailboxProcessor,
                    'resource-archive': ResourceArchiveProcessor,
                    'redirect': RedirectProcessor}.get(resource_type)
    if not ResProcessor: raise ClassSelectionError(f'Unknown resource type: {resource_type}')
    op_service = get_opservice_by_resource(resource, resource_type)
    processor = ResProcessor(resource, op_service, params=params or {})
    collector = get_rescollector(resource_type, resource)
    collector.ignore_property('quotaUsed')
    processor.op_resource = collector.get()
    processor.extra_services = get_extra_services(processor)
    return processor


def get_rescollector(resource_type, resource):
    ResCollector = {'unix-account': UnixAccountCollector,
                    'database-user': DatabaseUserCollector,
                    'database': DatabaseCollector,
                    'mailbox': MailboxCollector,
                    'website': WebsiteCollector,
                    'ssl-certificate': SslCertificateCollector,
                    'service': ServiceCollector,
                    'resource-archive': ResourceArchiveCollector,
                    'redirect': RedirectCollector}.get(resource_type)
    if not ResCollector: raise ClassSelectionError(f'Unknown resource type: {resource_type}')
    op_service = get_opservice_by_resource(resource, resource_type)
    collector = ResCollector(resource, op_service)
    collector.extra_services = get_extra_services(collector)
    return collector


def get_datafetcher(src_uri, dst_uri, params=None):
    scheme = urllib.parse.urlparse(src_uri).scheme
    DataFetcher = {'file': FileDataFetcher,
                   'rsync': RsyncDataFetcher,
                   'mysql': MysqlDataFetcher,
                   'http': HttpDataFetcher,
                   'git+ssh': GitDataFetcher,
                   'git+http': GitDataFetcher,
                   'git+https': GitDataFetcher}.get(urllib.parse.urlparse(src_uri).scheme)
    if not DataFetcher: raise ClassSelectionError(f'Unknown data source URI scheme: {scheme}')
    return DataFetcher(src_uri, dst_uri, params=params or {})


def get_datapostprocessor(postproc_type, args):
    DataPostprocessor = {'docker': DockerDataPostprocessor,
                         'string-replace': StringReplaceDataProcessor,
                         'eraser': DataEraser}.get(postproc_type)
    if not DataPostprocessor: raise ClassSelectionError(f'Unknown data postprocessor type: {postproc_type}')
    return DataPostprocessor(**args)


def get_listener(listener_type):
    Listener = {'amqp': AMQPListener,
                'time': TimeListener}.get(listener_type)
    if not Listener: raise ClassSelectionError(f'Unknown Listener type: {listener_type}')
    out_queue = Executor.get_new_task_queue()
    return Listener(out_queue)


def get_reporter(reporter_type):
    Reporter = {'amqp': AMQPReporter,
                'https': HttpsReporter,
                'alerta': AlertaReporter,
                'null': NullReporter}.get(reporter_type)
    if not Reporter: raise ClassSelectionError(f'Unknown Reporter type: {reporter_type}')
    return Reporter()


def get_backuper(res_type, resource):
    Backuper = {'unix-account': ResticBackup,
                'website': ResticBackup}.get(res_type)
    if not Backuper: raise ClassSelectionError(f'Unknown resource type: {res_type}')
    return Backuper(resource)
