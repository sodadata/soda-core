from __future__ import annotations

import json
import logging
import os
import threading
import uuid
from datetime import datetime, timezone
from logging import LogRecord
from typing import Optional

from soda_core.common import soda_cloud
from soda_core.common.datetime_conversions import convert_str_to_datetime
from soda_core.common.logging_configuration import _mask_record
from soda_core.common.logging_constants import Emoticons
from soda_core.common.logs_base import STREAM_DIAGNOSTICS_LOGGER, THREAD_LABEL_ATTR, LogsBase
from soda_core.common.soda_cloud import SodaCloud, to_jsonnable

DEFAULT_FLUSH_INTERVAL = 5
MAX_LOG_LINES = int(os.environ.get("SODA_LOGS_BATCH_LIMIT_COUNT", "1000"))

# 4xx that can heal (timeout, rate limit). Any other 4xx on the log endpoints means the scan
# permanently refuses uploads: deleted (404), or off its log-accepting state (400) — for good.
_TRANSIENT_4XX = {408, 429}

# The stream's own diagnostics. Console-only: logs._RootCapturer refuses this logger, so a failing
# stream can never feed reports about itself into the queue it reports on.
stream_logger = logging.getLogger(STREAM_DIAGNOSTICS_LOGGER)


def _is_permanent_rejection(status_code: int) -> bool:
    return 400 <= status_code < 500 and status_code not in _TRANSIENT_4XX


def _serialize_record(log_record: LogRecord, index: int) -> str:
    return json.dumps(to_jsonnable(soda_cloud.build_log_cloud_json_dict(log_record, index)))


def build_streaming_gatherer(soda_cloud: SodaCloud, scan_id: str) -> LogsQueue:
    """A ``LogsQueue`` streaming to the scan's main log stage over the scan-id-keyed ``batchV4``
    endpoint. Callers resolve the scan id themselves (``EnvConfigHelper`` is the one place that
    reads SODA_SCAN_ID). Scan-REFERENCE-keyed consumers construct their own queue: that posts to a
    different endpoint.
    """
    return LogsQueue(soda_cloud=soda_cloud, stage="main", scan_id=scan_id, dataset="")


class LogsQueue(LogsBase):
    """Streams captured log records to the scan's Soda Cloud log stream.

    ``_pending`` holds exactly the records Soda Cloud has not acknowledged: ``emit`` appends, a
    flush sends the head in one request and removes it only on a 2xx. A failed send leaves the
    records queued for the worker's next cadence tick — that cadence is the only retry mechanism.
    Delivery is at-least-once: an upload whose response was lost is re-sent, and the backend does
    not dedupe.
    """

    def __init__(
        self,
        soda_cloud: SodaCloud,
        stage: str,
        scan_reference: Optional[str] = None,
        dataset: str = "",
        scan_id: Optional[str] = None,
    ):
        super().__init__()
        self.index = 0
        self.soda_cloud = soda_cloud
        # When scan_id is set, logs are uploaded via the scan-id-keyed batchV4 endpoint;
        # otherwise we fall back to the scan-reference-keyed batchV3 endpoint that existing
        # library consumers rely on.
        self.scan_reference = scan_reference
        self.scan_id = scan_id
        if not scan_id and not scan_reference:
            # Without an identifier the flush would POST to /logs/None/batchV3, silently
            # discarding logs server-side and making failures hard to diagnose.
            raise ValueError("LogsQueue requires either scan_id (batchV4) or scan_reference (batchV3)")
        self.stage = stage
        self.thread = str(uuid.uuid4())
        self.dataset = dataset
        self.flush_interval = DEFAULT_FLUSH_INTERVAL
        self.batch_size = MAX_LOG_LINES
        # The undelivered records, oldest first. emit appends under _pending_lock, so it never
        # blocks on network; a flush removes an acknowledged prefix.
        self._pending: list[LogRecord] = []
        self._pending_lock = threading.Lock()
        # Serializes the worker's flushes against caller-thread flushes (close, failure report):
        # concurrent flushes would send the same head twice.
        self._flush_lock = threading.Lock()
        # Set when the backend permanently refuses uploads: posting stops, but records keep
        # queueing so a later failure report can still carry them.
        self._terminal_reason: Optional[str] = None
        # Set when a failure report has taken the pending records: nothing further is enqueued,
        # so the report's aftermath cannot raise a false undelivered alarm at close.
        self._retired: bool = False
        self.shutdown_flag = threading.Event()
        self._create_worker_thread()

    def _create_worker_thread(self):
        self.worker_thread = threading.Thread(target=self._background_worker, daemon=True)
        self.worker_thread.start()

    # Public API
    def get_error_logs(self) -> list[LogRecord]:
        # levelno with `==`, matching LogsCollector.get_error_logs: a streaming run's status
        # determination must agree with the in-memory behavior.
        return [log for log in self.logs if log.levelno == logging.ERROR]

    def get_error_or_warning_logs(self) -> list[LogRecord]:
        raise AssertionError("Warning logs unavailable in LogsQueue")

    def get_all_logs(self) -> list[LogRecord]:
        # Streamed records are not re-gatherable. The empty list keeps a streaming run's results
        # payload `logs` field empty; failure reports use records_for_failure_report().
        return []

    def records_for_failure_report(self) -> list[LogRecord]:
        """Hand the undelivered records over to a ``sodaCoreMarkScanFailed`` report.

        Soda Cloud replaces the scan's stored logs with the report's attached list, so: flush
        first (a healthy stream then has nothing pending and the report goes out empty, keeping
        the streamed history), and attach only what is still pending. The returned records leave
        the queue — the report is their delivery.
        """
        # Retired before the flush: the flush's own HTTP call can emit records on this thread,
        # and those must not land in a queue nothing drains again.
        self._retired = True
        self.flush()
        with self._pending_lock:
            handed_over = self._pending
            self._pending = []
        return handed_over

    def reset(self):
        self.thread = str(uuid.uuid4())
        self.logs: list[LogRecord] = []
        self.logs_buffer: list[LogRecord] = []
        self.has_error_logs = False
        self.has_warning_logs = False
        self._pending = []
        self._terminal_reason = None
        self._retired = False
        return self

    def close(self):
        """Flush the remaining records, stop the worker, and account for anything undelivered."""
        try:
            self.shutdown_flag.set()
            self.worker_thread.join()
            self._flush_logs(DEFAULT_FLUSH_INTERVAL)
        except Exception:
            # failure to close logs shouldn't crash the app
            stream_logger.exception("Error while closing the Soda Cloud log stream")
        with self._pending_lock:
            undelivered = len(self._pending)
        if undelivered:
            # The stream is the run's only log channel: undelivered records must never pass silently.
            reason = self._terminal_reason or "the final flush could not deliver them"
            stream_logger.error(
                f"{Emoticons.POLICE_CAR_LIGHT} {undelivered} log record(s) were not delivered to "
                f"Soda Cloud over the scan's log stream: {reason}."
            )

    def emit(self, log_record: LogRecord):
        with self._pending_lock:
            if self._retired:
                # Nothing can be delivered after the failure report; keep error records for status
                # determination. The console still shows everything through the regular handler.
                _mask_record(log_record)
                self._preserve_if_error_log(log_record)
                return
            log_record.__setattr__("stage", self.stage)
            log_record.__setattr__("index", self.index)
            # `thread` is a grouping label on Soda Cloud. Every LogRecord carries a default `thread`
            # (the OS thread ident), so only records marked by the active Logs keep theirs; the rest
            # get this queue's uuid.
            if not getattr(log_record, THREAD_LABEL_ATTR, False):
                log_record.__setattr__("thread", self.thread)
            log_record.__setattr__("dataset", self.dataset)
            self.index += 1
            _mask_record(log_record)
            self._pending.append(log_record)
            self._preserve_if_error_log(log_record)

    def flush(self) -> None:
        """Ship whatever is pending, now, on the calling thread."""
        self._flush_logs(self.flush_interval)

    # Private API

    def _preserve_if_error_log(self, log: LogRecord):
        if log.levelno >= logging.ERROR:
            self.logs.append(log)

    def _background_worker(self):
        flush_interval = self.flush_interval
        while not self.shutdown_flag.wait(timeout=flush_interval):
            try:
                flush_interval = self._flush_logs(flush_interval)
            except Exception as e:
                # A dead worker is a silently stopped stream; keep looping on the default cadence.
                stream_logger.warning(f"Log flush failed unexpectedly: {type(e).__name__}: {e}")
                flush_interval = self.flush_interval

    def _flush_logs(self, current_flush_interval):
        with self._flush_lock:
            while True:
                if self._terminal_reason is not None:
                    return current_flush_interval
                with self._pending_lock:
                    batch = self._pending[: self.batch_size]
                if not batch:
                    return current_flush_interval

                lines: list[str] = []
                evicted: list[LogRecord] = []
                for position, record in enumerate(batch):
                    try:
                        lines.append(_serialize_record(record, position))
                    except Exception as e:
                        # Serialization failures are deterministic, and pending records only leave
                        # the queue on delivery: kept, this record would block everything behind it.
                        evicted.append(record)
                        stream_logger.warning(
                            f"Evicting an unserializable record from the log stream "
                            f"(logger={record.name!r}, level={record.levelname}): {type(e).__name__}: {e}"
                        )
                if evicted:
                    with self._pending_lock:
                        for record in evicted:
                            self._pending.remove(record)
                    if not lines:
                        continue

                try:
                    response = (
                        self.soda_cloud.logs_batch_v4(scan_id=self.scan_id, body="\n".join(lines))
                        if self.scan_id
                        else self.soda_cloud.logs_batch(scan_reference=self.scan_reference, body="\n".join(lines))
                    )
                    # Read inside the guard: a malformed response object counts as a failed send.
                    accepted: bool = 200 <= response.status_code < 300
                except Exception as e:
                    stream_logger.warning(
                        f"Could not send a log batch to Soda Cloud ({type(e).__name__}: {e}); "
                        f"{len(lines)} record(s) stay queued for the next flush"
                    )
                    return current_flush_interval

                if accepted:
                    # The trace id is the only handle Cloud-side support has on a specific batch.
                    stream_logger.debug(
                        f"Sent {len(lines)} log record(s) to Soda Cloud, code={response.status_code}, "
                        f"trace={response.headers.get('X-Soda-Trace-Id')}"
                    )
                    with self._pending_lock:
                        # emit only appends and flushes are serialized, so after the eviction pass
                        # the sent records are exactly the head of the list.
                        del self._pending[: len(lines)]
                    current_flush_interval = (
                        self.get_next_batch_timeout(response.headers.get("X-Soda-Next-Batch-Time"))
                        or self.flush_interval
                    )
                    continue

                if _is_permanent_rejection(response.status_code):
                    # Stop posting; the pending records' only remaining route to Soda Cloud is a
                    # failure report, and close() accounts for them otherwise.
                    self._terminal_reason = f"Soda Cloud permanently refused the stream (HTTP {response.status_code})"
                    stream_logger.warning(f"{self._terminal_reason}; {len(lines)} record(s) remain undelivered")
                    return current_flush_interval

                stream_logger.warning(
                    f"Could not send a log batch to Soda Cloud (HTTP {response.status_code}); "
                    f"{len(lines)} record(s) stay queued for the next flush"
                )
                return current_flush_interval

    def get_next_batch_timeout(self, next_batch_time: Optional[str]) -> int:
        if next_batch_time is None:
            return 0

        try:
            next_batch_datetime = convert_str_to_datetime(next_batch_time)
            now = datetime.now(timezone.utc)
            timeout = (next_batch_datetime - now).total_seconds()

            return max(0, timeout)
        except Exception:
            stream_logger.debug(f"X-Soda-Next-Batch-Time invalid date format: {next_batch_time}")
            return 0
