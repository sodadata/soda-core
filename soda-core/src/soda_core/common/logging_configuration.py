import hashlib
import logging
import os
import sys
from datetime import datetime
from logging import (
    CRITICAL,
    DEBUG,
    ERROR,
    INFO,
    WARNING,
    Formatter,
    LogRecord,
    StreamHandler,
)
from typing import Optional

from soda_core.common.logging_constants import Emoticons, ExtraKeys

verbose_mode: bool = False


def configure_logging(
    verbose: bool = False,
) -> None:
    """
    Used exposing the Soda log configurations.
    """
    global verbose_mode
    verbose_mode = verbose

    _prepare_masked_file()

    sys.stderr = sys.stdout
    for logger_to_mute in [
        "urllib3",
        "botocore",
        "pyathena",
        "faker",
        "snowflake",
        "matplotlib",
        "pyspark",
        "pyhive",
        "py4j",
        "segment",
    ]:
        logging.getLogger(logger_to_mute).setLevel(ERROR)

    soda_log_level: int = DEBUG if verbose else INFO

    logging.basicConfig(
        level=soda_log_level,
        force=True,  # Override any previously set handlers.
        # https://docs.python.org/3/library/logging.html#logrecord-attributes
        handlers=[SodaConsoleHandler()],
    )


_masked_values = set()


def _prepare_masked_file():
    global _masked_values
    file_with_masked_values = os.environ.get("SODA_MASKED_VALUES_FILE", None)
    if file_with_masked_values is not None:
        if not os.path.exists(file_with_masked_values):
            raise RuntimeError(f"Masked values file '{file_with_masked_values}' does not exist")
        sha_expected = os.environ.get("SODA_MASKED_VALUES_FILE_HASH", None)
        if not sha_expected:
            raise RuntimeError(f"Hash for the masked values file '{file_with_masked_values}' is not present")
        sha_hash = hashlib.sha256()
        with open(file_with_masked_values) as f:
            _masked_values.clear()
            read_lines = [l for l in f.readlines()]
            for l in read_lines:
                sha_hash.update(l.encode("utf-8"))
            sha_actual = sha_hash.hexdigest()
            if sha_actual != sha_expected:
                raise RuntimeError(
                    f"Hash mismatch for the masked values file '{file_with_masked_values}' (expected '{sha_expected}', got '{sha_actual}')"
                )
            _masked_values.update({l.strip() for l in read_lines})


def _mask_record(record: LogRecord):
    message = record.getMessage()
    updated = False
    if message:
        for masked in _masked_values:
            if masked not in message:
                continue
            updated = True
            message = message.replace(masked, "***")
    if updated:
        record.msg = message
        # since getMessage evaluates args, we need to clear them after the full message has been cleared
        record.args = ()


def _mask_message(message: str) -> str:
    if not message or not _masked_values:
        return message
    for masked in _masked_values:
        if masked not in message:
            continue
        message = message.replace(masked, "***")
    return message


def is_verbose() -> bool:
    return verbose_mode


class SodaConsoleHandler(StreamHandler):
    def __init__(self):
        super().__init__(sys.stdout)
        self.setFormatter(SodaConsoleFormatter())


class SodaConsoleFormatter(Formatter):
    def __init__(self):
        super().__init__()

    def format(self, record) -> str:
        parts: list[str] = [
            # self.format_timestamp(record),
            # self.format_level(record),
            self.format_message(record),
            self.format_location(record),
            self.format_doc(record),
            self.format_exception(record),
        ]
        return " | ".join(part for part in parts if part is not None)

    level_names: dict[int, str] = {
        CRITICAL: "CRI",
        ERROR: "ERR",
        WARNING: "WAR",
        INFO: "INF",
        DEBUG: "DEB",
    }

    def format_level(self, record: LogRecord) -> Optional[str]:
        return self.level_names.get(record.levelno, "UNKNOWN")

    def format_message(self, record: LogRecord) -> str:
        _mask_record(record)
        if record.levelno >= ERROR:
            return f"{Emoticons.POLICE_CAR_LIGHT} {record.getMessage()}"
        else:
            return record.getMessage()

    def format_timestamp(self, record: LogRecord) -> Optional[str]:
        timestamp: datetime = datetime.fromtimestamp(record.created)
        # Format the time part (without milliseconds)
        time_part = timestamp.strftime("%Y-%m-%d %H:%M:%S")
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
        if record.exc_info:
            # Cache the traceback text to avoid converting it multiple times
            # (it's constant anyway)
            if not record.exc_text:
                record.exc_text = _mask_message(self.formatException(record.exc_info))
                return record.exc_text

        if hasattr(record, ExtraKeys.EXCEPTION):
            return record.exception
