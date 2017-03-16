import sys
import time
import logging
import signal
import threading

from taskexecutor.config import CONFIG
import taskexecutor.constructor
import taskexecutor.logger

sys.stderr = taskexecutor.logger.StreamToLogger(taskexecutor.logger.LOGGER, logging.ERROR)
STOP = False


def receive_signal(signum, unused_stack):
    if signum == signal.SIGINT:
        taskexecutor.logger.LOGGER.info("SIGINT recieved")
        global STOP
        STOP = True


def main():
    signal.signal(signal.SIGINT, receive_signal)
    taskexecutor.logger.LOGGER.info("Perfoming initial Service updates")
    for service in CONFIG.localserver.services:
        processor = taskexecutor.constructor.get_resprocessor("service", service)
        processor.update()
    amqp_listener = taskexecutor.constructor.get_listener("amqp")
    amqp_listener_thread = threading.Thread(target=amqp_listener.listen)
    amqp_listener_thread.start()
    taskexecutor.logger.LOGGER.info("AMQP listener thread started")
    time_listener = taskexecutor.constructor.get_listener("time")
    time_listener_thread = threading.Thread(target=time_listener.listen)
    time_listener_thread.start()
    taskexecutor.logger.LOGGER.info("Time listener thread started")

    while True:
        if not amqp_listener_thread.is_alive():
            taskexecutor.logger.LOGGER.error("AMQP Listener is dead, exiting")
            amqp_listener_thread.join()
            time_listener.stop()
            time_listener_thread.join()
            taskexecutor.logger.LOGGER.info("Scheduler stopped")
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
            break
        time.sleep(.5)

if __name__ == "__main__":
    main()
