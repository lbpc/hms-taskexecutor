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
amqp_listener = ListenerBuilder("amqp")
amqp_listener_thread = None
while True:
	if not amqp_listener_thread:
		amqp_listener_thread = Thread(target=amqp_listener.listen, name="AMQPListener")
		amqp_listener_thread.start()
		LOGGER.info("AMQP listener thread started")
	if STOP and amqp_listener_thread.is_alive():
		LOGGER.info("Stopping AMQP listener")
		amqp_listener.stop()
	elif STOP and not amqp_listener_thread.is_alive():
		LOGGER.info("AMQP listener stopped")
		amqp_listener_thread.join()
	elif not amqp_listener_thread.is_alive():
		LOGGER.warning("AMQP listener thread is dead")
		amqp_listener_thread.join()
		amqp_listener_thread = None
	elif STOP:
		LOGGER.info("All done")
		break
