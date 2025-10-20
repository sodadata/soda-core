from __future__ import annotations

import logging
import threading
from logging import Handler, LogRecord
from typing import Optional

from soda_core.common.logs_base import LogsBase
from soda_core.common.logs_collector import LogsCollector


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
    Captures logging records for the current thread and sends them to the provided LogsBase gatherer.
    """

    def __init__(self, gatherer: LogsBase):
        super().__init__()
        self.gatherer: LogsBase = gatherer
        self.threading_ident: int = threading.get_ident()
        logging.root.addHandler(self)

    def remove_from_root_logger(self) -> None:
        logging.root.removeHandler(self)

    def emit(self, record: LogRecord):
        if self.threading_ident == record.thread:
            self.gatherer.emit(record)


class Logs:
    """
    Configuration manager and entry point for log handling in soda core.
    On creation, it attaches a LogCapturer to the root logger to capture logs for the current thread.
    By default, it uses in-memory LogCollector to gather all logs.
    """

    def __init__(self, gatherer: Optional[LogsBase] = None):
        if gatherer is None:
            gatherer = LogsCollector()
        self.gatherer: LogsBase = gatherer
        self.log_capturer: LogCapturer = LogCapturer(gatherer)

    def remove_from_root_logger(self) -> None:
        self.log_capturer.remove_from_root_logger()

    def __str__(self) -> str:
        return self.get_logs_str()

    def get_log_records(self) -> list[LogRecord]:
        return self.gatherer.get_all_logs()

    def get_logs(self) -> list[str]:
        return [r.msg for r in self.gatherer.get_all_logs()]

    def get_logs_str(self):
        return "\n".join(self.get_logs())

    def get_errors_str(self) -> str:
        return "\n".join(self.get_errors())

    def get_errors(self) -> list[str]:
        return [r.msg for r in self.gatherer.get_error_logs()]

    @property
    def has_errors(self) -> bool:
        return len(self.get_errors()) > 0

    def pop_log_records(self) -> list[LogRecord]:
        log_records: list[LogRecord] = self.get_log_records()
        self.gatherer.close()
        return log_records
