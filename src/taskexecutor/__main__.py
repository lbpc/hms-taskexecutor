import sys
import time
from logging import ERROR
from signal import SIGINT, signal
from threading import Thread
from taskexecutor.listener import ListenerBuilder
from taskexecutor.scheduler import Scheduler
from taskexecutor.logger import LOGGER, StreamToLogger

sys.stderr = StreamToLogger(LOGGER, ERROR)
STOP = False


def receive_signal(signum, stack):
    if signum == SIGINT:
        LOGGER.info("SIGINT recieved")
        global STOP
        STOP = True


def main():
    signal(SIGINT, receive_signal)
    amqp_listener = ListenerBuilder("amqp")()
    amqp_listener_thread = Thread(target=amqp_listener.listen)
    amqp_listener_thread.start()
    LOGGER.info("AMQP listener thread started")
    scheduler = Scheduler()
    scheduler_thread = Thread(target=scheduler.start)
    scheduler_thread.start()
    LOGGER.info("Scheduler thread started")

    while True:
        if not amqp_listener_thread.is_alive():
            LOGGER.error("AMQP Listener is dead, exiting")
            amqp_listener_thread.join()
            scheduler.stop()
            scheduler_thread.join()
            LOGGER.info("Scheduler stopped")
            sys.exit(1)
        if STOP:
            LOGGER.info("Stopping AMQP listener")
            amqp_listener.stop()
            amqp_listener_thread.join()
            LOGGER.info("AMQP listener stopped")
            LOGGER.info("Stopping scheduler")
            scheduler.stop()
            scheduler_thread.join()
            LOGGER.info("Scheduler stopped")
            break
        time.sleep(.1)

if __name__ == "__main__":
    main()
