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

# Gateway-shaped 4xx that can heal (timeout, rate limit). Every other 4xx on the log endpoints is
# the backend saying this scan permanently refuses uploads: the scan is gone (404), or it left its
# log-accepting state (400 invalid_scan_state) — a transition that never reverses.
_TRANSIENT_4XX = {408, 429}

# The stream's own diagnostics: batch sent, batch refused, record evicted. Console-only by
# construction — ``logs._RootCapturer`` refuses to capture this logger, so these records can never
# be fed into the queue they report on, no matter which thread the flush runs on.
stream_logger = logging.getLogger(STREAM_DIAGNOSTICS_LOGGER)


def _is_permanent_rejection(status_code: int) -> bool:
    return 400 <= status_code < 500 and status_code not in _TRANSIENT_4XX


def _serialize_record(log_record: LogRecord, index: int) -> str:
    return json.dumps(to_jsonnable(soda_cloud.build_log_cloud_json_dict(log_record, index)))


def build_streaming_gatherer(soda_cloud: SodaCloud, scan_id: str) -> LogsQueue:
    """A ``LogsQueue`` streaming to the scan's main log stage. Callers resolve the scan id (the
    Cloud-only concept the Runner/launcher sets as SODA_SCAN_ID — ``EnvConfigHelper`` is the one
    place that reads it) and decide whether their run streams at all; this is only the construction
    site for scan-id-keyed (``batchV4``) streaming. A scan-REFERENCE-keyed queue posts to a
    different endpoint, so consumers of that branch (the failed-rows extractor's
    ``diagnosticWarehouse`` stage) construct their own.
    """
    return LogsQueue(soda_cloud=soda_cloud, stage="main", scan_id=scan_id, dataset="")


class LogsQueue(LogsBase):
    """Streams captured log records to the scan's Soda Cloud log stream.

    One invariant carries the design: ``_pending`` holds exactly the records Soda Cloud has not
    acknowledged. ``emit`` appends; a flush sends the head of the list in one request and removes
    it only on a 2xx. A failed send therefore needs no bookkeeping — the records simply stay
    queued and the worker's next cadence tick re-sends them; retries as a mechanism don't exist
    here (transport-level concerns like re-authentication live on the ``SodaCloud`` client).
    Delivery is at-least-once: an upload whose response was lost is re-sent, and the backend does
    not dedupe.

    Two flags end a stream's life:
    - ``_terminal_reason``: the backend permanently refuses this scan's uploads (a non-transient
      4xx). Flushing stops, but records keep queueing — a later failure report is then their only
      remaining route to Soda Cloud, and ``close()`` accounts for them if the run never sends one.
    - ``_retired``: a failure report has taken the pending records (``records_for_failure_report``
      is the hand-over). Nothing further is enqueued — after ``sodaCoreMarkScanFailed`` attaches
      records, the backend refuses further uploads, and re-queueing would raise a false undelivered
      alarm at close right after the failure was reported successfully.
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
        # The undelivered records, oldest first — the queue's only delivery state. emit appends
        # under _pending_lock (never blocking on network); a flush removes an acked prefix.
        self._pending: list[LogRecord] = []
        self._pending_lock = threading.Lock()
        # Serialises flushes between the worker thread and a caller-thread flush()
        # (records_for_failure_report, close): concurrent flushes would send the same head twice.
        self._flush_lock = threading.Lock()
        self._terminal_reason: Optional[str] = None
        self._retired: bool = False
        self.shutdown_flag = threading.Event()
        self._create_worker_thread()

    def _create_worker_thread(self):
        self.worker_thread = threading.Thread(target=self._background_worker, daemon=True)
        self.worker_thread.start()

    # Public API
    def get_error_logs(self) -> list[LogRecord]:
        # levelno, not the nonexistent LogRecord.level, and `==` to match LogsCollector.get_error_logs exactly:
        # a streaming run's has_errors/status determination must agree with the in-memory ad-hoc behavior.
        return [log for log in self.logs if log.levelno == logging.ERROR]

    def get_error_or_warning_logs(self) -> list[LogRecord]:
        raise AssertionError("Warning logs unavailable in LogsQueue")

    def get_all_logs(self) -> list[LogRecord]:
        # Streamed records are not re-gatherable: they are shipped (or pending) to the scan's log
        # stream. The empty list is what keeps a streaming run's results payload `logs` field empty
        # at the existing fill sites. Callers needing failure-report content use
        # records_for_failure_report().
        return []

    def records_for_failure_report(self) -> list[LogRecord]:
        """Hand the undelivered records over to a ``sodaCoreMarkScanFailed`` report.

        ``sodaCoreMarkScanFailed`` does NOT merge its ``logs`` with what the stream already
        delivered — Soda Cloud replaces the scan's stored logs with exactly the attached list. So:
        flush first (a healthy stream then has nothing pending, the report goes out empty and Soda
        Cloud keeps the full streamed history), and attach only what is still pending — the records
        that reached Cloud through no other channel. The rule lives here, in the gatherer, so every
        ``mark_scan_as_failed`` call site gets it without knowing it exists.

        This is a hand-over: the returned records leave the queue (the report is their delivery),
        and the stream retires — retired BEFORE the flush, because the flush's own HTTP call can
        emit records on this very thread (transport-level DEBUG logging), and those must not land
        in a queue nothing will drain again.
        """
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
        except Exception as e:
            # failure to close logs shouldn't crash the app
            stream_logger.error(f"Error while closing the Soda Cloud log stream: {e}")
        with self._pending_lock:
            undelivered = len(self._pending)
        if undelivered:
            # The stream is the run's only log channel, so records it could not deliver must never
            # pass silently. Error records among them still ride the failure report when the run
            # sends one (records_for_failure_report empties the queue, so this doesn't fire then).
            reason = self._terminal_reason or "the final flush could not deliver them"
            stream_logger.error(
                f"{Emoticons.POLICE_CAR_LIGHT} {undelivered} log record(s) were not delivered to "
                f"Soda Cloud over the scan's log stream: {reason}."
            )

    def emit(self, log_record: LogRecord):
        with self._pending_lock:
            if self._retired:
                # A failure report took the queue: nothing further can be delivered over the
                # stream. Keep error records for status determination; the console still shows
                # everything through the regular handler.
                _mask_record(log_record)
                self._preserve_if_error_log(log_record)
                return
            log_record.__setattr__("stage", self.stage)
            log_record.__setattr__("index", self.index)
            # Every LogRecord already carries `thread` (the OS thread ident), so attribute existence cannot
            # tell a caller-set grouping label from the default. The active Logs stamps its label and marks
            # it with THREAD_LABEL_ATTR; anything else gets this queue's uuid — a stable per-stream identity
            # rather than a meaningless number.
            if not getattr(log_record, THREAD_LABEL_ATTR, False):
                log_record.__setattr__("thread", self.thread)
            log_record.__setattr__("dataset", self.dataset)
            self.index += 1
            _mask_record(log_record)
            self._pending.append(log_record)
            self._preserve_if_error_log(log_record)

    def flush(self) -> None:
        """Ship whatever is pending, now, on the calling thread (serialised against the worker)."""
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
                # Last line of defence: if this thread dies the stream stops with no accounting,
                # which is indistinguishable from a quiet run. Keep looping on the default cadence.
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
                        # Deterministic: the same record fails the same way forever, and pending
                        # records only leave the queue on delivery — an unserializable one would
                        # block everything behind it on every future flush. Evict it.
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
                    # Read inside the guard: a malformed response object is a failed send, not a
                    # worker-killing surprise.
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
                        # emit only appends and flushes are serialised, so after the eviction pass
                        # the sent records are exactly the head of the list.
                        del self._pending[: len(lines)]
                    current_flush_interval = (
                        self.get_next_batch_timeout(response.headers.get("X-Soda-Next-Batch-Time"))
                        or self.flush_interval
                    )
                    continue

                if _is_permanent_rejection(response.status_code):
                    # The scan will never accept another upload (deleted, or moved off its
                    # log-accepting state). Stop posting; the pending records' only remaining route
                    # to Soda Cloud is a failure report, and close() accounts for them otherwise.
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
