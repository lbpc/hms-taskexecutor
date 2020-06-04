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
        msg = msg.encode("utf-8", "replace").decode("utf-8")
        self.logger.log(self.log_level, msg)

    def flush(self):
        pass


logging.basicConfig(
    format="%(asctime)s.%(msecs)03d %(levelname)s --- "
           "(%(module)s:%(lineno)d %(funcName)s) %(threadName)s\t: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
LOGGER = logging.getLogger("taskexecutor")
LOGGER.setLevel(logging.INFO)
