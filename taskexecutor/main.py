#!/usr/bin/env python3
from signal import signal, SIGINT
import sys
from logging import ERROR
from threading import Thread

from taskexecutor.listener import ListenerBuilder
from taskexecutor.logger import LOGGER, StreamToLogger

sys.stderr = StreamToLogger(LOGGER, ERROR)
STOP = False

def receive_signal(signum, stack):
	if signum == SIGINT:
		LOGGER.info("SIGINT recieved")
		global STOP
		STOP = True

signal(SIGINT, receive_signal)
amqp_listener = ListenerBuilder("amqp")()
amqp_listener_thread = Thread(target=amqp_listener.listen)
amqp_listener_thread.start()
LOGGER.info("AMQP listener thread started")
while True:
	if not amqp_listener_thread.is_alive():
		LOGGER.error("AMQP Listener is dead, exiting")
		amqp_listener_thread.join()
		sys.exit(1)
	if STOP:
		LOGGER.info("Stopping AMQP listener")
		amqp_listener.stop()
		amqp_listener_thread.join()
		LOGGER.info("AMQP listener stopped")
		break
