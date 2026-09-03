from __future__ import annotations

from abc import ABC, abstractmethod
from logging import LogRecord

# Marks a record's ``thread`` value as a caller-set grouping label (Soda Cloud exposes it as a log
# filter). Every stdlib LogRecord already carries ``thread`` — the OS thread ident — so attribute
# existence cannot tell a label from the default; this marker can. Lives here so the writer
# (``logs._RootCapturer``) and the reader (``logs_queue.LogsQueue.emit``) cannot drift.
THREAD_LABEL_ATTR = "soda_thread_label"

# Logger name for a log stream's own diagnostics (batch sent / retried / dropped). Console-only by
# construction: ``logs._RootCapturer`` refuses to capture it, so a failing stream can never feed
# reports about itself into the queue it is reporting on.
STREAM_DIAGNOSTICS_LOGGER = "soda.logs_stream"


class LogsBase(ABC):
    def __init__(self):
        self.thread = None
        self.logs: list[LogRecord] = []
        self.logs_buffer: list[LogRecord] = []
        self.has_error_logs = False
        self.has_warning_logs = False

    @abstractmethod
    def get_error_logs(self) -> list[LogRecord]:
        pass

    @abstractmethod
    def get_error_or_warning_logs(self) -> list[LogRecord]:
        pass

    @abstractmethod
    def get_all_logs(self) -> list[LogRecord]:
        pass

    def records_for_failure_report(self) -> list[LogRecord]:
        # Records a failure report (sodaCoreMarkScanFailed) should attach. In-memory gatherers return
        # everything (nothing was sent elsewhere); streaming gatherers override to return only what Soda Cloud
        # has not confirmed received, so the report doesn't duplicate the stream.
        return self.get_all_logs()

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
