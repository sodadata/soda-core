from __future__ import annotations

import logging
import threading
from logging import ERROR, Handler, Logger, LogRecord
from typing import Optional


class Location:
    def __init__(self, file_path: Optional[str], line: Optional[int] = None, column: Optional[int] = None):
        self.file_path: Optional[str] = file_path
        self.line: Optional[int] = line
        self.column: Optional[int] = column

    def __str__(self) -> str:
        src_description: str = self.file_path if self.file_path else "location"
        if self.line is not None and self.column is not None:
            src_description += f"[{self.line},{self.column}]"
        return src_description

    def __hash__(self) -> int:
        return hash((self.line, self.column))

    def get_dict(self) -> dict:
        return {"file_path": self.file_path, "line": self.line, "column": self.column}


class LogCapturer(Handler):
    """
    Captures logging records for the current thread and stores them in the given records list
    """

    def __init__(self, records: list[LogRecord]):
        super().__init__()
        self.records: list[LogRecord] = records
        self.threading_ident: int = threading.get_ident()
        logging.root.addHandler(self)

    def remove_from_root_logger(self) -> None:
        logging.root.removeHandler(self)

    def emit(self, record: LogRecord):
        if self.threading_ident == record.thread:
            self.records.append(record)


class Logs:
    logger: Logger = logging.getLogger("soda.contracts")

    def __init__(self):
        # Stores all logs above debug level to be sent to soda cloud and for testing logs in the test suite.
        # self.logs: list[Log] = logs or []
        self.records: list[LogRecord] = []
        self.log_capturer: LogCapturer = LogCapturer(self.records)

    def remove_from_root_logger(self) -> None:
        self.log_capturer.remove_from_root_logger()

    def __str__(self) -> str:
        return self.get_logs_str()

    def get_logs(self) -> list[str]:
        return [r.msg for r in self.records]

    def get_logs_str(self):
        return "\n".join(self.get_logs())

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def get_errors(self) -> list[str]:
        return [r.msg for r in self.records if r.levelno >= ERROR]

    def has_errors(self) -> bool:
        return len(self.get_errors()) > 0

    def pop_log_records(self) -> list[LogRecord]:
        log_records: list[LogRecord] = self.records
        self.records = []
        return log_records
