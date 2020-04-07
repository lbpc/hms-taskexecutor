import logging
import signal
import sys
import time
from threading import Thread

from taskexecutor import constructor
from taskexecutor.config import CONFIG
from taskexecutor.executor import Executor
from taskexecutor.httpsclient import ApiClient
from taskexecutor.logger import LOGGER, StreamToLogger
from taskexecutor.task import Task
from taskexecutor.watchdog import ProcessWatchdog

sys.stderr = StreamToLogger(LOGGER, logging.ERROR)
STOP = False


def receive_signal(signum, unused_stack):
    if signum in (signal.SIGINT, signal.SIGTERM):
        LOGGER.info(f'{signum} signal recieved')
        global STOP
        STOP = True
    elif signum == signal.SIGUSR1:
        LOGGER.info('SIGUSR1 recieved')
        new_task_queue = Executor().get_new_task_queue()
        update_all_services(new_task_queue)


def update_all_services(new_task_queue, isolated=False):
    LOGGER.info('Performing Service updates')
    services = [constructor.get_http_proxy_service(),
                constructor.get_database_server(),
                constructor.get_cron_service(),
                constructor.get_mta_service(),
                constructor.get_ssh_service(),
                constructor.get_ftp_service()]
    if isolated: services.append(constructor.get_application_servers())
    for each in services:
        task = Task(None, type(None), 'LOCAL', f'{each.name}.update', 'service', 'update',
                    params={'resource': each, 'isolated': isolated})
        new_task_queue.put(task)


signal.signal(signal.SIGINT, receive_signal)
signal.signal(signal.SIGTERM, receive_signal)
signal.signal(signal.SIGUSR1, receive_signal)

executor = Executor()
executor_thread = Thread(target=executor.run, daemon=True)
executor_thread.start()
LOGGER.info('Executor thread started')

update_all_services(Executor().get_new_task_queue(), isolated=True)

amqp_listener = constructor.get_listener('amqp')
amqp_listener_thread = Thread(target=amqp_listener.listen, daemon=True)
amqp_listener_thread.start()
LOGGER.info('AMQP listener thread started')

time_listener = constructor.get_listener('time')
time_listener_thread = Thread(target=time_listener.listen, daemon=True)
time_listener_thread.start()
LOGGER.info('Time listener thread started')

process_watchdog = ProcessWatchdog(**CONFIG.process_watchdog._asdict())
process_watchdog_thread = Thread(target=process_watchdog.run, daemon=True)
process_watchdog_thread.start()
uids_queue = process_watchdog.get_uids_queue()
with ApiClient(**CONFIG.apigw) as api:
    for each in (u.uid for u in api.UnixAccount().filter(serverId=CONFIG.localserver.id).get() if u.infected):
        LOGGER.info(f'UID {each} is restricted')
        uids_queue.put(each)

while True:
    if not amqp_listener_thread.is_alive():
        LOGGER.error('AMQP Listener is dead, exiting now')
        executor.stop()
        time_listener.stop()
        process_watchdog.stop()
        sys.exit(1)
    if STOP:
        LOGGER.info('Stopping AMQP listener')
        amqp_listener.stop()
        amqp_listener_thread.join()
        LOGGER.info('AMQP listener stopped')
        LOGGER.info('Stopping scheduler')
        time_listener.stop()
        time_listener_thread.join()
        LOGGER.info('Scheduler stopped')
        executor.stop(wait=True)
        executor_thread.join()
        LOGGER.info('Executor stopped')
        LOGGER.info('Stopping process watchdog')
        process_watchdog.stop()
        process_watchdog_thread.join()
        LOGGER.info('Process watchdog stopped')
        break
    time.sleep(1)
