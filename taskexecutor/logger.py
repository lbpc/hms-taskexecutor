import logging
import logging.handlers
import os
import sys
from fcntl import fcntl, F_GETFL, F_SETFL

class StreamToLogger:
	def __init__(self, logger, log_level=logging.DEBUG):
		self.logger = logger
		self.log_level = log_level
		self.fd_read, self.fd_write = os.pipe()
		fcntl(self.fd_read,
		      F_SETFL,
		      fcntl(self.fd_read, F_GETFL) | os.O_NONBLOCK)
		self.pipe_reader = os.fdopen(self.fd_read)

	def fileno(self):
		return self.fd_write

	def write(self, buf):
		self.write_from_buffer(buf)
		self.write_from_pipe()

	def write_from_buffer(self, buf):
		_msg = str()
		for line in buf.rstrip().splitlines():
			_msg += line.strip() + " "
		self.logger.log(self.log_level, _msg)

	def write_from_pipe(self):
		for line in iter(self.pipe_reader.readline, ''):
			self.logger.log(self.log_level, line.strip())

	def flush(self):
		pass


logging.basicConfig(
		format="<%(levelname)s> %(threadName)s: "
		       "(%(module)s:%(lineno)d %(funcName)s) %(message)s",
		stream=sys.stdout
)
LOGGER = logging.getLogger("taskexecutor")
LOGGER.setLevel(logging.INFO)
logging.getLogger("pika").setLevel(logging.WARNING)
