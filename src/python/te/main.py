import sys
import time
import logging
import signal
import threading

from taskexecutor.config import CONFIG
from taskexecutor.httpsclient import ApiClient
from taskexecutor import watchdog
from taskexecutor import constructor
from taskexecutor.executor import Executor
from taskexecutor.task import Task
from taskexecutor import logger

sys.stderr = logger.StreamToLogger(logger.LOGGER, logging.ERROR)
STOP = False

"""Taskexecutor MJ inc"""
def receive_signal(signum, unused_stack):
    if signum in (signal.SIGINT, signal.SIGTERM):
        logger.LOGGER.info("{} signal recieved".format(signum))
        global STOP
        STOP = True
    elif signum == signal.SIGUSR1:
        logger.LOGGER.info("SIGUSR1 recieved")
        new_task_queue = Executor().get_new_task_queue()
        update_all_services(new_task_queue)


def update_all_services(new_task_queue, isolated=False):
    logger.LOGGER.info("Perfoming Service updates")
    type_name_conditions = [(str.startswith, "STAFF_"), (str.startswith, "DATABASE_"),
                            (str.startswith, "ACCESS_")]
    if isolated:
        type_name_conditions.append((str.startswith, "WEBSITE_"))
    for service in (s for s in CONFIG.localserver.services
                    if any((c[0](s.serviceTemplate.serviceType.name, c[1]) for c in type_name_conditions))):
        task = Task(None,
                    type(None),
                    "LOCAL",
                    "{}.update".format(service.name),
                    "service",
                    "update",
                    params={"resource": service, "isolated": isolated})
        new_task_queue.put(task)


def populate_restricted_uids_set(get_uids_queue):
    with ApiClient(**CONFIG.apigw) as api:
        uids = [u.uid for u in api.UnixAccount().filter(serverId=CONFIG.localserver.id).get() if u.infected]
    for uid in uids:
        get_uids_queue.put(uid)
    logger.LOGGER.info("Restricted UIDs: {}".format(uids))


def main():
    signal.signal(signal.SIGINT, receive_signal)
    signal.signal(signal.SIGTERM, receive_signal)
    signal.signal(signal.SIGUSR1, receive_signal)
    executor = Executor()
    executor_thread = threading.Thread(target=executor.run, daemon=True)
    executor_thread.start()
    logger.LOGGER.info("Executor thread started")
    update_all_services(Executor().get_new_task_queue(), isolated=True)
    amqp_listener = constructor.get_listener("amqp")
    amqp_listener_thread = threading.Thread(target=amqp_listener.listen, daemon=True)
    amqp_listener_thread.start()
    logger.LOGGER.info("AMQP listener thread started")
    time_listener = constructor.get_listener("time")
    time_listener_thread = threading.Thread(target=time_listener.listen, daemon=True)
    time_listener_thread.start()
    logger.LOGGER.info("Time listener thread started")
    process_watchdog = watchdog.ProcessWatchdog(**CONFIG.process_watchdog._asdict())
    process_watchdog_thread = threading.Thread(target=process_watchdog.run, daemon=True)
    process_watchdog_thread.start()
    populate_restricted_uids_set(process_watchdog.get_uids_queue())
    logger.LOGGER.info("Process watchdog thread started")
    while True:
        if not amqp_listener_thread.is_alive():
            logger.LOGGER.error("AMQP Listener is dead, exiting now")
            executor.stop()
            time_listener.stop()
            process_watchdog.stop()
            sys.exit(1)
        if STOP:
            logger.LOGGER.info("Stopping AMQP listener")
            amqp_listener.stop()
            amqp_listener_thread.join()
            logger.LOGGER.info("AMQP listener stopped")
            logger.LOGGER.info("Stopping scheduler")
            time_listener.stop()
            time_listener_thread.join()
            logger.LOGGER.info("Scheduler stopped")
            executor.stop(wait=True)
            executor_thread.join()
            logger.LOGGER.info("Executor stopped")
            logger.LOGGER.info("Stopping process watchdog")
            process_watchdog.stop()
            process_watchdog_thread.join()
            logger.LOGGER.info("Process watchdog stopped")
            break
        time.sleep(1)

if __name__ == "__main__":
    main()
