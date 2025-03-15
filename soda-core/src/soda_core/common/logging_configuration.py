import logging
import sys
from datetime import datetime
from logging import ERROR, StreamHandler, DEBUG, WARNING, Formatter, LogRecord, getLevelName, CRITICAL, INFO
from typing import Optional

from soda_core.common.logging_constants import ExtraKeys, Emoticons


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
        handlers=[SodaConsoleHandler()],
    )


class SodaConsoleHandler(StreamHandler):
    def __init__(self):
        super().__init__(sys.stdout)
        self.setFormatter(SodaConsoleFormatter())


class SodaConsoleFormatter(Formatter):
    def __init__(self):
        super().__init__()

    def format(self, record) -> str:
        parts: list[str] = [
            self.format_timestamp(record),
            self.format_level(record),
            self.format_message(record),
            self.format_location(record),
            self.format_doc(record),
            self.format_exception(record)
        ]
        return " | ".join(part for part in parts if part is not None)

    level_names: dict[int, str] = {
        CRITICAL: 'CRI',
        ERROR: 'ERR',
        WARNING: 'WAR',
        INFO: 'INF',
        DEBUG: 'DEB',
    }

    def format_level(self, record: LogRecord) -> Optional[str]:
        return self.level_names.get(record.levelno, "UNKNOWN")

    def format_message(self, record: LogRecord) -> str:
        if record.levelno >= ERROR:
            return f"{Emoticons.POLICE_CAR_LIGHT} {record.msg}"
        else:
            return record.msg

    def format_timestamp(self, record: LogRecord) -> Optional[str]:
        timestamp: datetime = datetime.fromtimestamp(record.created)
        # Format the time part (without milliseconds)
        time_part = timestamp.strftime('%Y-%m-%d %H:%M:%S')
        # Add milliseconds with comma as separator
        milliseconds = timestamp.microsecond // 1000  # Convert microseconds to milliseconds
        return f"{time_part},{milliseconds:03d}"

    def format_location(self, record: LogRecord) -> Optional[str]:
        if hasattr(record, ExtraKeys.LOCATION):
            return str(record.location)
        else:
            return None

    def format_doc(self, record: LogRecord) -> Optional[str]:
        if hasattr(record, ExtraKeys.DOC):
            return record.doc
        else:
            return None

    def format_exception(self, record: LogRecord) -> Optional[str]:
        if hasattr(record, ExtraKeys.EXCEPTION):
            return record.exception
        else:
            return None
