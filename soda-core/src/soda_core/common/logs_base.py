from __future__ import annotations

from abc import ABC, abstractmethod
from logging import LogRecord

# Marks a record's ``thread`` value as a caller-set grouping label rather than the OS thread ident
# every LogRecord carries by default. Shared by the writer (``logs._RootCapturer``) and the reader
# (``logs_queue.LogsQueue.emit``).
THREAD_LABEL_ATTR = "soda_thread_label"

# Logger name for a log stream's own diagnostics. Console-only: ``logs._RootCapturer`` refuses to
# capture it, so a failing stream cannot feed reports about itself into the queue it reports on.
STREAM_DIAGNOSTICS_LOGGER = "soda.logs_stream"

# The log stages Soda Cloud knows (its StageType enum). A name outside this set arrives as no
# stage at all, so those records drop out of the stage filter in the UI.
LOG_STAGE_MAIN = "main"
LOG_STAGE_DIAGNOSTIC_WAREHOUSE = "diagnosticWarehouse"


class LogsBase(ABC):
    def __init__(self):
        self.thread = None
        self.logs: list[LogRecord] = []

    @abstractmethod
    def get_error_logs(self) -> list[LogRecord]:
        pass

    @abstractmethod
    def get_all_logs(self) -> list[LogRecord]:
        pass

    def records_for_failure_report(self) -> list[LogRecord]:
        # What a failure report (sodaCoreMarkScanFailed) should attach. In-memory gatherers return
        # everything; streaming gatherers override to return only the undelivered records.
        return self.get_all_logs()

    @abstractmethod
    def emit(self, log_record: LogRecord):
        pass

    def close(self):
        # Close method is used to finish all remaining logger tasks.
        # By default it doesn't do anything
        pass
