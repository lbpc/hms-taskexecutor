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
    format="%(levelname)s\t(%(module)s:%(lineno)d %(funcName)s) --- %(threadName)s\t: %(message)s",
    stream=sys.stdout,
)
LOGGER = logging.getLogger("taskexecutor")
LOGGER.setLevel(logging.INFO)
logging.getLogger("pika").setLevel(logging.ERROR)
