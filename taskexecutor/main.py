#!/usr/bin/env python3
import signal
import sys
from logging import ERROR
from queue import Queue
from random import random
from time import sleep

from taskexecutor.config import CONFIG
from taskexecutor.listener import ListenerBuilder
from taskexecutor.logger import LOGGER, StreamToLogger

STOP = False
sys.stderr = StreamToLogger(LOGGER, ERROR)
amqp_listeners_pool = Queue(CONFIG["listeners"]["amqp"])

def receive_signal(signum, stack):
	if signum == signal.SIGINT:
		LOGGER.info("SIGINT recieved")
		global STOP
		STOP = True

signal.signal(signal.SIGINT, receive_signal)
while True:
	if not amqp_listeners_pool.full():
		amqp_listener = ListenerBuilder("amqp")
		amqp_listener.start()
		LOGGER.info(
				"AMQP listener thread '{}' started".format(amqp_listener.name)
		)
		amqp_listeners_pool.put(amqp_listener)
		continue
	else:
		for _ in range(amqp_listeners_pool.qsize()):
			amqp_listener = amqp_listeners_pool.get()
			if not amqp_listener.is_alive():
				amqp_listener.join()
			else:
				amqp_listeners_pool.put(amqp_listener)
	if STOP:
		LOGGER.info("Stopping all threads")
		for _ in range(amqp_listeners_pool.qsize()):
			amqp_listener = amqp_listeners_pool.get()
			if amqp_listener.is_alive():
				amqp_listener.stop()
			amqp_listener.join()
			LOGGER.info(
					"AMQP listener thread {} finished".format(amqp_listener.name)
			)
		break
	sleep(random())
