from __future__ import annotations

import json
import logging
import os
import queue
import threading
import uuid
from datetime import datetime, timezone
from logging import LogRecord
from typing import Optional

from soda_core.common import exceptions, soda_cloud
from soda_core.common.datetime_conversions import convert_str_to_datetime
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_configuration import _mask_record
from soda_core.common.logging_constants import Emoticons
from soda_core.common.logs_base import (
    STREAM_DIAGNOSTICS_LOGGER,
    THREAD_LABEL_ATTR,
    LogsBase,
)
from soda_core.common.soda_cloud import SodaCloud, to_jsonnable

DEFAULT_FLUSH_INTERVAL = 5
MAX_LOG_LINES = int(os.environ.get("SODA_LOGS_BATCH_LIMIT_COUNT", "1000"))
MAX_RETRIES = 3
# Separate from the flush cadence so a retry storm is not tied to how often batches ship (and so tests
# can drive the retry path without sleeping). Waited on the shutdown event rather than slept, so a
# closing queue never holds a scan's teardown open for the full backoff.
RETRY_DELAY_SECONDS = DEFAULT_FLUSH_INTERVAL

# The stream's own diagnostics: batch sent, retry, batch dropped. Console-only by construction —
# ``logs._RootCapturer`` refuses to capture this logger, so these records can never be fed into the
# queue they report on, no matter which thread the flush runs on.
stream_logger = logging.getLogger(STREAM_DIAGNOSTICS_LOGGER)

# A plain 4xx means the upload will never be accepted (unknown scan, wrong scan state, malformed
# batch); retrying only delays the run. 5xx — and the two 4xx that mean "later" — get another attempt.
_RETRYABLE_4XX = {408, 429}


def _is_retryable(status_code: int) -> bool:
    return status_code in _RETRYABLE_4XX or not 400 <= status_code < 500


def _to_jsonl(batch: list[LogRecord]) -> str:
    log_cloud_serialized_json_lines = (
        [
            json.dumps(to_jsonnable(soda_cloud.build_log_cloud_json_dict(log_record, index)))
            for index, log_record in enumerate(batch)
        ]
        if batch
        else []
    )
    return "\n".join(log_cloud_serialized_json_lines)


def build_streaming_gatherer(
    soda_cloud: Optional[SodaCloud],
    scan_id: Optional[str] = None,
) -> Optional[LogsQueue]:
    """A ``LogsQueue`` streaming to the scan's main log stage, or None when there is no scan id /
    Cloud client so callers keep their in-memory gatherer. The construction site for scan-id-keyed
    (``batchV4``) streaming — a scan-REFERENCE-keyed queue posts to a different endpoint, so
    consumers of that branch (the failed-rows extractor's ``diagnosticWarehouse`` stage) construct
    their own.
    """
    if scan_id is None:
        # The scan id is a Cloud-only concept set by the Runner/launcher as SODA_SCAN_ID; it is read
        # from the env helper rather than a CLI argument so the generic CLI stays Cloud-agnostic.
        scan_id = EnvConfigHelper().soda_scan_id
    if not scan_id or soda_cloud is None:
        return None
    return LogsQueue(soda_cloud=soda_cloud, stage="main", scan_id=scan_id, dataset="")


class LogsQueue(LogsBase):
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
        # Identities (id()) of ERROR-level records confirmed flushed (2xx) to the log stream, consulted by
        # records_for_failure_report(). Only error records are tracked: they stay alive in self.logs, so their id()
        # cannot be reused. Tracking freed non-error records would poison the set — CPython reuses a dead record's
        # address for the next allocation, and a later (unsent) error record landing on it would silently drop out
        # of the failure report.
        self._flushed_records: set[int] = set()
        # The stream is the run's only log channel once it is live, so a dropped batch is real data
        # loss and must never pass silently — counted here, reported per drop and summarised in close().
        self._dropped_record_count: int = 0
        self._dropped_batch_count: int = 0
        self._last_drop_reason: Optional[str] = None
        # Set by records_for_failure_report(): the caller is about to send sodaCoreMarkScanFailed,
        # after which the backend rejects every further batchV4 upload for this scan — so a retired
        # queue stops uploading (later records stay console-only and are not counted as stream
        # losses) while still preserving error records for status determination and any later report.
        self._retired: bool = False
        self.log_queue = queue.Queue()
        # Serialises _flush_logs between the worker thread and a caller-thread flush()
        # (records_for_failure_report, close): unserialised, the two would each take a disjoint
        # half of the queue and interleave their uploads.
        self._flush_lock = threading.Lock()
        self.shutdown_flag = threading.Event()
        self.condition = threading.Condition()
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
        # Streamed records are not re-gatherable: they are shipped (or in flight) to the scan's log stream. The
        # empty list is what keeps a streaming run's results payload `logs` field empty at the existing fill
        # sites. Callers needing failure-report content use records_for_failure_report().
        return []

    def records_for_failure_report(self) -> list[LogRecord]:
        """The records ``sodaCoreMarkScanFailed`` should attach — and the flush that makes that answer safe.

        The flush is not a side effect, it is how the answer is computed: ``sodaCoreMarkScanFailed`` does NOT
        merge its ``logs`` with what the stream already delivered — Soda Cloud replaces the scan's stored logs
        with exactly the attached list and moves the scan off the ongoing-log store, after which the streamed
        records are deleted rather than promoted. Flushing first means a healthy stream leaves nothing unsent,
        the report goes out empty, and Soda Cloud keeps the full streamed history; only records the stream
        genuinely could not deliver are attached — the one case where attaching them is right, because they
        reached Cloud through no other channel. The rule lives here, in the gatherer, so every
        ``mark_scan_as_failed`` call site gets it without knowing it exists.

        This call is also the hand-over: it RETIRES the stream. Once the report lands, the backend
        rejects every further ``batchV4`` upload for the scan, so anything logged afterwards (the
        report's own confirmation line, post-processing on a run that continues) can only be
        console-visible — attempting to upload it would end the run with a false "records could not
        be delivered" alarm right after the failure was reported successfully. Retired BEFORE the
        flush: the flush's own HTTP call can emit records on this very thread (transport-level DEBUG
        logging), and those must not land in a queue nothing will drain for delivery again.
        """
        self._retired = True
        self.flush()
        return [record for record in self.logs if id(record) not in self._flushed_records]

    def reset(self):
        self.thread = str(uuid.uuid4())
        self.logs: list[LogRecord] = []
        self.logs_buffer: list[LogRecord] = []
        self.verbose: bool = False
        self.has_error_logs = False
        self.has_warning_logs = False
        self._flushed_records = set()
        self._dropped_record_count = 0
        self._dropped_batch_count = 0
        self._last_drop_reason = None
        self._retired = False
        return self

    # To make sure all logs have been sent trigger close method
    def close(self):
        """
        Flush remaining logs, stop the background thread, and report any records the stream dropped.
        """
        try:
            self.shutdown_flag.set()
            with self.condition:
                self.condition.notify()  # Wake up the thread to process remaining logs
            self.worker_thread.join()
            self._flush_logs(DEFAULT_FLUSH_INTERVAL)
        except Exception as e:
            # failure to close logs shouldn't crash the app
            stream_logger.error(f"Error while closing the Soda Cloud log stream: {e}")
        if self._dropped_batch_count:
            # "could not be delivered over the stream", not "missing from the scan's logs": error
            # records among the drops still ride the failure report when the run sends one.
            stream_logger.error(
                f"{Emoticons.POLICE_CAR_LIGHT} {self._dropped_record_count} log record(s) in "
                f"{self._dropped_batch_count} batch(es) could not be delivered to Soda Cloud over the "
                f"scan's log stream. Last failure: {self._last_drop_reason}"
            )

    def emit(self, log_record: LogRecord):
        with self.condition:
            if self._retired:
                # The scan reached a terminal state on Soda Cloud: nothing further can be delivered.
                # Keep error records for status determination and a possible later failure report;
                # the console still shows everything through the regular handler.
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
            self.log_queue.put(log_record)
            self._preserve_if_error_log(log_record)
            if self.log_queue.qsize() >= self.batch_size:
                self.condition.notify()

    def flush(self) -> None:
        """Ship whatever is queued, now, on the calling thread (serialised against the worker)."""
        self._flush_logs(self.flush_interval)

    # Private API

    def _preserve_if_error_log(self, log: LogRecord):
        if log.levelno >= logging.ERROR:
            self.logs.append(log)

    def _background_worker(self):
        flush_interval = self.flush_interval
        while not self.shutdown_flag.is_set():
            with self.condition:
                self.condition.wait(timeout=flush_interval)

            try:
                flush_interval = self._flush_logs(flush_interval)
            except Exception as e:
                # Last line of defence: if this thread dies the stream stops with no accounting,
                # which is indistinguishable from a quiet run. Keep looping on the default cadence.
                stream_logger.warning(f"Log flush failed unexpectedly: {type(e).__name__}: {e}")
                stream_logger.debug(exceptions.get_exception_stacktrace(e))
                flush_interval = self.flush_interval

    def _validate_prerequisites(self):
        # Wait until soda cloud is correctly configured and scan starts, throw only on shutdown.
        while not self.soda_cloud:
            if not self.shutdown_flag.is_set():
                stream_logger.debug("Soda Cloud has not been configured properly yet.")
                self.shutdown_flag.wait(DEFAULT_FLUSH_INTERVAL)
            else:
                raise AssertionError("You have not configured Soda Library to work with Soda Cloud Async Mode.")

    def _flush_logs(self, current_flush_interval):
        # Soda Cloud need to be configured before first flush
        self._validate_prerequisites()

        with self._flush_lock:
            batch = []
            while not self.log_queue.empty():
                batch.append(self.log_queue.get())

            if not batch or self.soda_cloud is None:
                return current_flush_interval

            try:
                body: str = _to_jsonl(batch)
            except Exception as e:
                # Not retryable: the same records would fail to serialise the same way.
                stream_logger.debug(exceptions.get_exception_stacktrace(e))
                return self._drop_batch(batch, f"could not be serialised: {type(e).__name__}: {e}", attempts=1)

            for attempt in range(MAX_RETRIES):
                last_attempt: bool = attempt == MAX_RETRIES - 1
                try:
                    stream_logger.debug(f"Sending {len(batch)} log record(s) to Soda Cloud")
                    response = (
                        self.soda_cloud.logs_batch_v4(scan_id=self.scan_id, body=body)
                        if self.scan_id
                        else self.soda_cloud.logs_batch(scan_reference=self.scan_reference, body=body)
                    )
                    if 200 <= response.status_code < 300:
                        # The trace id is the only handle Cloud-side support has on a specific batch.
                        stream_logger.debug(
                            f"Sent {len(batch)} log record(s) to Soda Cloud, code={response.status_code}, "
                            f"trace={response.headers.get('X-Soda-Trace-Id')}"
                        )
                        # Only a 2xx confirms the batch reached the stream. Only error-record ids are
                        # tracked — see the _flushed_records comment in __init__.
                        self._flushed_records.update(id(r) for r in batch if r.levelno >= logging.ERROR)
                        return (
                            self.get_next_batch_timeout(response.headers.get("X-Soda-Next-Batch-Time"))
                            or self.flush_interval
                        )
                    reason: str = f"HTTP {response.status_code}"
                    retryable: bool = _is_retryable(response.status_code)
                except Exception as e:
                    reason, retryable = f"{type(e).__name__}: {e}", True
                    if last_attempt:
                        stream_logger.debug(exceptions.get_exception_stacktrace(e))

                if not retryable or last_attempt:
                    return self._drop_batch(batch, reason, attempts=attempt + 1)
                stream_logger.warning(
                    f"Could not send a log batch to Soda Cloud ({reason}); "
                    f"retrying {attempt + 2}/{MAX_RETRIES} in {RETRY_DELAY_SECONDS}s"
                )
                # Not time.sleep: close() sets the shutdown flag before its final flush, and a scan must
                # not wait out the backoff to exit — the remaining attempts still run, back to back.
                self.shutdown_flag.wait(RETRY_DELAY_SECONDS)

    def _drop_batch(self, batch: list[LogRecord], reason: str, attempts: int) -> int:
        """Give up on a batch: count it, say so, and return the default cadence.

        The records are already off the queue, so this is real loss. It is reported per drop here and
        summarised in close(), because a silently truncated log stream looks exactly like a quiet run.
        """
        self._dropped_batch_count += 1
        self._dropped_record_count += len(batch)
        self._last_drop_reason = reason
        # The real attempt count, not MAX_RETRIES: a non-retryable 4xx drops after ONE post, and telling an
        # operator it was tried three times sends them down the rate-limit path for a permanent rejection.
        stream_logger.warning(
            f"Dropping {len(batch)} log record(s) after {attempts} attempt(s): {reason}. "
            f"The scan's log stream will not carry these records."
        )
        return self.flush_interval

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
