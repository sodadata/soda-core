import logging

import sys
from logging import StreamHandler, DEBUG

from soda_core.common.logs import Emoticons


class SodaConsoleHandler(StreamHandler):
    def __init__(self):
        super().__init__(sys.stdout)
        self.setFormatter(SodaFormatter())


class SodaFormatter(logging.Formatter):
    level_emoticons: dict[str, str] = {
        "ERROR": f"{Emoticons.POLICE_CAR_LIGHT} ",
        "WARNING": f"{Emoticons.PINCHED_FINGERS} ",
        "CRITICAL": f"{Emoticons.EXPLODING_HEAD} "
    }

    def __init__(self):
        super().__init__(fmt="%(levelname)s%(message)s")

    def format(self, record):
        record.levelname = self.level_emoticons.get(record.levelname, "")
        return super().format(record)


if __name__ == "__main__":
    # Configure logger
    sys.stderr = sys.stdout

    logger = logging.getLogger("soda_core.something.test")

    for logger_to_mute in [
        "urllib3", "botocore", "pyathena", "faker", "snowflake", "matplotlib", "pyspark", "pyhive", "py4j", "segment"
    ]:
        logging.getLogger(logger_to_mute).setLevel(logging.ERROR)

    soda_handler: StreamHandler = StreamHandler(sys.stdout)
    soda_handler.setFormatter(SodaFormatter())

    logging.basicConfig(
        level=logging.DEBUG,
        force=True,  # Override any previously set handlers.
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        handlers=[SodaConsoleHandler()],
    )

    # Test logging at different levels
    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
