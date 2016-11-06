import logging
import logging.handlers
import sys


class StreamToLogger:
    def __init__(self, logger, log_level=logging.DEBUG):
        self.logger = logger
        self.log_level = log_level

    def write(self, buf):
        _msg = str()
        for line in buf.rstrip().splitlines():
            _msg += line.strip() + "\n\t"
        self.logger.log(self.log_level, _msg)


logging.basicConfig(
    format="%(threadName)s LOG LEVEL: %(levelname)s "
           "(%(module)s:%(lineno)d %(funcName)s) %(message)s",
    stream=sys.stdout
)
LOGGER = logging.getLogger("taskexecutor")
LOGGER.setLevel(logging.DEBUG)
logging.getLogger("pika").setLevel(logging.WARNING)
