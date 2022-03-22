import logging
from dataclasses import dataclass
from enum import Enum
from textwrap import indent
from typing import Optional

from soda.common.exception_helper import get_exception_stacktrace
from soda.sodacl.location import Location

logger = logging.getLogger("soda.scan")


class LogLevel(Enum):
    WARNING = "WARNING"
    ERROR = "ERROR"
    INFO = "INFO"
    DEBUG = "DEBUG"


@dataclass
class Log:

    __log_level_mappings = {
        LogLevel.ERROR: logging.ERROR,
        LogLevel.WARNING: logging.WARNING,
        LogLevel.INFO: logging.INFO,
        LogLevel.DEBUG: logging.DEBUG,
    }

    level: LogLevel
    message: str
    location: Optional[Location]
    doc: Optional[str]
    exception: Optional[BaseException]

    def __str__(self):
        location_str = f" | {self.location}" if self.location else ""
        doc_str = f" | https://go.soda.io/{self.doc}" if self.doc else ""
        exception_str = f" | {self.exception}" if self.exception else ""
        return f"{self.level.value.ljust(7)}| {self.message}{location_str}{doc_str}{exception_str}"

    def get_cloud_dict(self) -> dict:
        log_cloud_dict = {"level": self.level.value, "message": self.message}
        if self.location:
            log_cloud_dict["location"] = self.location.to_soda_cloud_json()
        if self.doc:
            log_cloud_dict["doc"] = self.doc
        if self.exception:
            log_cloud_dict["exception"] = get_exception_stacktrace(self.exception)
        return log_cloud_dict

    @staticmethod
    def log_errors(error_logs):
        logger.info(f"ERRORS:")
        for error_log in error_logs:
            error_log.log_to_python_logging()

    def log_to_python_logging(self):
        python_log_level = Log.__log_level_mappings[self.level]
        logger.log(python_log_level, self.message)
        if self.exception is not None:
            exception_str = str(self.exception)
            exception_str = indent(text=exception_str, prefix="  | ")
            logger.log(python_log_level, exception_str)
            # Logging the stack trace
            stacktrace = get_exception_stacktrace(self.exception)
            indented_stacktrace = indent(text=stacktrace, prefix="  | ")
            logger.log(python_log_level, f"  | Stacktrace:")
            logger.log(python_log_level, indented_stacktrace)
        if self.level in [LogLevel.WARNING, LogLevel.ERROR] and self.location is not None:
            logger.log(python_log_level, f"  +-> {self.location}")
        if isinstance(self.doc, str):
            logger.log(python_log_level, f"  +-> See https://go.soda.io/{self.doc}")
