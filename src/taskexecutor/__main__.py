import sys
import time
import logging
import signal
import threading

from taskexecutor.config import CONFIG
from taskexecutor.httpsclient import ApiClient
import taskexecutor.watchdog
import taskexecutor.constructor
import taskexecutor.executor
import taskexecutor.task
import taskexecutor.logger

sys.stderr = taskexecutor.logger.StreamToLogger(taskexecutor.logger.LOGGER, logging.ERROR)
STOP = False


def receive_signal(signum, unused_stack):
    if signum == signal.SIGINT:
        taskexecutor.logger.LOGGER.info("SIGINT recieved")
        global STOP
        STOP = True
    elif signum == signal.SIGUSR1:
        taskexecutor.logger.LOGGER.info("SIGUSR1 recieved")
        new_task_queue = taskexecutor.executor.Executor().get_new_task_queue()
        update_all_services(new_task_queue)


def update_all_services(new_task_queue):
    taskexecutor.logger.LOGGER.info("Perfoming Service updates")
    for service in [s for s in CONFIG.localserver.services
                    if s.serviceTemplate.serviceType.name.startswith("STAFF_")
                    or s.serviceTemplate.serviceType.name.startswith("DATABASE_")]:
        task = taskexecutor.task.Task(None,
                                      type(None),
                                      "LOCAL",
                                      "{}.update".format(service.name),
                                      "service",
                                      "update",
                                      params={"resource": service})
        new_task_queue.put(task)


def populate_restricted_uids_set(get_uids_queue):
    with ApiClient(**CONFIG.apigw) as api:
        uids = [u.uid for u in api.UnixAccount().filter(serverId=CONFIG.localserver.id, infected=True).get()]
    for uid in uids:
        get_uids_queue.put(uid)
    taskexecutor.logger.LOGGER.info("Restricted UIDs: {}".format(uids))


def main():
    signal.signal(signal.SIGINT, receive_signal)
    signal.signal(signal.SIGUSR1, receive_signal)
    executor = taskexecutor.executor.Executor()
    executor_thread = threading.Thread(target=executor.run)
    executor_thread.start()
    taskexecutor.logger.LOGGER.info("Executor thread started")
    amqp_listener = taskexecutor.constructor.get_listener("amqp")
    amqp_listener_thread = threading.Thread(target=amqp_listener.listen)
    amqp_listener_thread.start()
    taskexecutor.logger.LOGGER.info("AMQP listener thread started")
    time_listener = taskexecutor.constructor.get_listener("time")
    time_listener_thread = threading.Thread(target=time_listener.listen)
    time_listener_thread.start()
    taskexecutor.logger.LOGGER.info("Time listener thread started")
    process_watchdog = taskexecutor.watchdog.ProcessWatchdog(**CONFIG.process_watchdog._asdict())
    process_watchdog_thread = threading.Thread(target=process_watchdog.run)
    process_watchdog_thread.start()
    populate_restricted_uids_set(process_watchdog.get_uids_queue())
    taskexecutor.logger.LOGGER.info("Process watchdog thread started")
    while True:
        if not amqp_listener_thread.is_alive():
            taskexecutor.logger.LOGGER.error("AMQP Listener is dead, exiting")
            amqp_listener_thread.join()
            time_listener.stop()
            time_listener_thread.join()
            taskexecutor.logger.LOGGER.info("Scheduler stopped")
            executor.stop()
            executor_thread.join()
            taskexecutor.logger.LOGGER.info("Executor stopped")
            process_watchdog.stop()
            process_watchdog_thread.join()
            taskexecutor.logger.LOGGER.info("Process watchdog stopped")
            sys.exit(1)
        if STOP:
            taskexecutor.logger.LOGGER.info("Stopping AMQP listener")
            amqp_listener.stop()
            amqp_listener_thread.join()
            taskexecutor.logger.LOGGER.info("AMQP listener stopped")
            taskexecutor.logger.LOGGER.info("Stopping scheduler")
            time_listener.stop()
            time_listener_thread.join()
            taskexecutor.logger.LOGGER.info("Scheduler stopped")
            executor.stop()
            executor_thread.join()
            taskexecutor.logger.LOGGER.info("Executor stopped")
            taskexecutor.logger.LOGGER.info("Stopping process watchdog")
            process_watchdog.stop()
            process_watchdog_thread.join()
            taskexecutor.logger.LOGGER.info("Process watchdog stopped")
            break
        time.sleep(1)

if __name__ == "__main__":
    main()
