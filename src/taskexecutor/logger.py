import logging
import logging.handlers
import sys


class StreamToLogger:
    def __init__(self, logger, log_level=logging.DEBUG):
        self.logger = logger
        self.log_level = log_level

    def write(self, buf):
        msg = str()
        for line in buf.rstrip().splitlines():
            msg += line.strip() + "\n\t"
        self.logger.log(self.log_level, msg)

    def flush(self):
        pass


logging.basicConfig(
    format="%(threadName)s LOG LEVEL: %(levelname)s "
           "(%(module)s:%(lineno)d %(funcName)s) %(message)s",
    stream=sys.stdout,
)
LOGGER = logging.getLogger("taskexecutor")
LOGGER.setLevel(logging.DEBUG)
logging.getLogger("pika").setLevel(logging.WARNING)
