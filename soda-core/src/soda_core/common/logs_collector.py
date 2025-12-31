from __future__ import annotations

import logging
from logging import LogRecord
from soda_core.common.logging_configuration import _mask_message

from soda_core.common.logs_base import LogsBase


class LogsCollector(LogsBase):
    def __init__(self):
        super().__init__()

    # Public API
    def get_error_logs(self) -> list[LogRecord]:
        return [log for log in self.logs if log.levelno == logging.ERROR]

    def get_error_or_warning_logs(self) -> list[LogRecord]:
        return [log for log in self.logs if log.levelno in [logging.ERROR, logging.WARNING]]

    def get_all_logs(self) -> list[LogRecord]:
        return self.logs

    def reset(self):
        self.logs: list[LogRecord] = []
        self.logs_buffer: list[LogRecord] = []
        self.verbose: bool = False
        self.has_error_logs = False
        self.has_warning_logs = False
        return self

    def emit(self, log_record: LogRecord):
        _mask_message(log_record)
        self.logs.append(log_record)
