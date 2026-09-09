from __future__ import annotations

import logging
from logging import LogRecord

from soda_core.common.logging_configuration import _mask_record
from soda_core.common.logs_base import LogsBase


class LogsCollector(LogsBase):
    def __init__(self):
        super().__init__()

    # Public API
    def get_error_logs(self) -> list[LogRecord]:
        return [log for log in self.logs if log.levelno == logging.ERROR]

    def get_all_logs(self) -> list[LogRecord]:
        return self.logs

    def emit(self, log_record: LogRecord):
        _mask_record(log_record)
        self.logs.append(log_record)
