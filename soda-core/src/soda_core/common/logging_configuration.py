import logging
import sys
from logging import ERROR, StreamHandler, DEBUG, WARNING, Formatter


def configure_logging(
    verbose: bool = True,
) -> None:
    """
    Used exposing the Soda log configurations.
    """
    sys.stderr = sys.stdout
    for logger_to_mute in [
        "urllib3", "botocore", "pyathena", "faker", "snowflake", "matplotlib", "pyspark", "pyhive", "py4j", "segment"
    ]:
        logging.getLogger(logger_to_mute).setLevel(ERROR)

    soda_log_level: int = DEBUG if verbose else WARNING

    logging.basicConfig(
        level=soda_log_level,
        force=True,  # Override any previously set handlers.
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        handlers=[SodaTextHandler()],
    )


class SodaTextHandler(StreamHandler):
    def __init__(self):
        super().__init__(sys.stdout)
        self.setFormatter(SodaTextFormatter())


class SodaTextFormatter(Formatter):
    def __init__(self):
        super().__init__()

    def format(self, record) -> str:
        message: str = record.msg

        # if record.levelno >= ERROR:
        #     message = f"{Emoticons.POLICE_CAR_LIGHT} {message}"

        # In log formatters that send to cloud, consider truncating max log length

        if hasattr(record, 'location'):
            message = f"{message} | {record.location}"
        if hasattr(record, 'doc'):
            message = f"{message} | {record.doc}"
        if hasattr(record, 'exception'):
            message = f"{message} | Exception stack trace: {record.exception}"

        return message
