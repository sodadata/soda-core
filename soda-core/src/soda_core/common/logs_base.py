from __future__ import annotations

from abc import ABC, abstractmethod
from logging import LogRecord


class LogsBase(ABC):
    def __init__(self):
        self.thread = None
        self.logs: list[LogRecord] = []
        self.logs_buffer: list[LogRecord] = []
        self.has_error_logs = False
        self.has_warning_logs = False
        self.verbose: bool = False

    @abstractmethod
    def get_error_logs(self) -> list[LogRecord]:
        pass

    @abstractmethod
    def get_error_or_warning_logs(self) -> list[LogRecord]:
        pass

    @abstractmethod
    def get_all_logs(self) -> list[LogRecord]:
        pass

    @abstractmethod
    def reset(self):
        pass

    @abstractmethod
    def emit(self, log_record: LogRecord):
        pass

    def close(self):
        # Close method is used to finish all remaining logger tasks.
        # By default it doesn't do anything
        pass
