import sys
import time
import logging
import signal
import threading

import taskexecutor.listener
import taskexecutor.scheduler
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
    amqp_listener = taskexecutor.listener.Builder("amqp")
    amqp_listener_thread = threading.Thread(target=amqp_listener.listen)
    amqp_listener_thread.start()
    taskexecutor.logger.LOGGER.info("AMQP listener thread started")
    scheduler = taskexecutor.scheduler.Scheduler()
    scheduler_thread = threading.Thread(target=scheduler.start)
    scheduler_thread.start()
    taskexecutor.logger.LOGGER.info("Scheduler thread started")

    while True:
        if not amqp_listener_thread.is_alive():
            taskexecutor.logger.LOGGER.error("AMQP Listener is dead, exiting")
            amqp_listener_thread.join()
            scheduler.stop()
            scheduler_thread.join()
            taskexecutor.logger.LOGGER.info("Scheduler stopped")
            sys.exit(1)
        if STOP:
            taskexecutor.logger.LOGGER.info("Stopping AMQP listener")
            amqp_listener.stop()
            amqp_listener_thread.join()
            taskexecutor.logger.LOGGER.info("AMQP listener stopped")
            taskexecutor.logger.LOGGER.info("Stopping scheduler")
            scheduler.stop()
            scheduler_thread.join()
            taskexecutor.logger.LOGGER.info("Scheduler stopped")
            break
        time.sleep(.1)

if __name__ == "__main__":
    main()
