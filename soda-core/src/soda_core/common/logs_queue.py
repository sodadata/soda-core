from __future__ import annotations

import json
import logging
import os
import queue
import threading
import time
import uuid
from datetime import datetime, timezone
from logging import LogRecord
from typing import Optional

from soda_core.common import exceptions, soda_cloud
from soda_core.common.datetime_conversions import convert_str_to_datetime
from soda_core.common.logging_configuration import _mask_record
from soda_core.common.logs_base import LogsBase
from soda_core.common.soda_cloud import SodaCloud, to_jsonnable

DEFAULT_FLUSH_INTERVAL = 5
MAX_LOG_LINES = int(os.environ.get("SODA_LOGS_BATCH_LIMIT_COUNT", "1000"))
MAX_RETRIES = 3


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


class LogsQueue(LogsBase):
    def __init__(self, soda_cloud: SodaCloud, stage: str, scan_reference: str, dataset: str):
        super().__init__()
        self.index = 0
        self.soda_cloud = soda_cloud
        self.scan_reference = scan_reference
        self.stage = stage
        self.thread = str(uuid.uuid4())
        self.dataset = dataset
        self.flush_interval = DEFAULT_FLUSH_INTERVAL
        self.batch_size = MAX_LOG_LINES
        self.log_queue = queue.Queue()
        self.shutdown_flag = threading.Event()
        self.condition = threading.Condition()
        self._create_worker_thread()

    def _create_worker_thread(self):
        self.worker_thread = threading.Thread(target=self._background_worker, daemon=True)
        self.worker_thread.start()

    # Public API
    def get_error_logs(self) -> list[LogRecord]:
        return [log for log in self.logs if log.level == logging.ERROR]

    def get_error_or_warning_logs(self) -> list[LogRecord]:
        raise AssertionError("Warning logs unavailable in LogsQueue")

    def get_all_logs(self) -> list[LogRecord]:
        raise AssertionError("All logs unavailable in LogsQueue")

    def reset(self):
        self.thread = str(uuid.uuid4())
        self.logs: list[LogRecord] = []
        self.logs_buffer: list[LogRecord] = []
        self.verbose: bool = False
        self.has_error_logs = False
        self.has_warning_logs = False
        return self

    # To make sure all logs have been sent trigger close method
    def close(self):
        """
        Flush remaining logs and stop the background thread.
        """
        try:
            self.shutdown_flag.set()
            with self.condition:
                self.condition.notify()  # Wake up the thread to process remaining logs
            self.worker_thread.join()
            self._flush_logs(DEFAULT_FLUSH_INTERVAL)
        except Exception as e:
            # failure to close logs shouldn't crash the app
            logging.error(f"Error while closing LogsQueue: {e}")

    def emit(self, log_record: LogRecord):
        with self.condition:
            log_record.__setattr__("stage", self.stage)
            log_record.__setattr__("index", self.index)
            log_record.__setattr__("thread", self.thread)
            log_record.__setattr__("dataset", self.dataset)
            self.index += 1
            _mask_record(log_record)
            self.log_queue.put(log_record)
            self._preserve_if_error_log(log_record)
            if self.log_queue.qsize() >= self.batch_size:
                self.condition.notify()

    # Private API

    def _preserve_if_error_log(self, log: LogRecord):
        if log.levelno >= logging.ERROR:
            self.logs.append(log)

    def _background_worker(self):
        flush_interval = self.flush_interval
        while not self.shutdown_flag.is_set():
            with self.condition:
                self.condition.wait(timeout=flush_interval)

            flush_interval = self._flush_logs(flush_interval)

    def _validate_prerequisites(self):
        # Wait until soda cloud is correctly configured and scan starts, throw only on shutdown.
        while not self.soda_cloud:
            if not self.shutdown_flag.is_set():
                logging.debug("Soda Cloud has not been configured properly yet.")
                time.sleep(DEFAULT_FLUSH_INTERVAL)
            else:
                raise AssertionError("You have not configured Soda Library to work with Soda Cloud Async Mode.")

    def _flush_logs(self, current_flush_interval):
        # Soda Cloud need to be configured before first flush
        self._validate_prerequisites()

        batch = []
        while not self.log_queue.empty():
            batch.append(self.log_queue.get())

        if batch and self.soda_cloud is not None:
            for attempt in range(MAX_RETRIES):
                try:
                    if self.verbose:
                        print(f"Sending logs to the cloud, {len(batch)} logs in the batch.")

                    response = self.soda_cloud.logs_batch(scan_reference=self.scan_reference, body=_to_jsonl(batch))

                    if self.verbose:
                        print(
                            f"Logs sent to the cloud, trace={response.headers.get('X-Soda-Trace-Id')}, code={response.status_code}"
                        )

                    return (
                        self.get_next_batch_timeout(response.headers.get("X-Soda-Next-Batch-Time"))
                        or self.flush_interval
                    )
                except Exception as e:
                    if self.verbose:
                        print(f"Attempt {attempt + 1}/{MAX_RETRIES} failed to send logs: {e}")

                    if attempt == MAX_RETRIES - 1:
                        if self.verbose:
                            print("Max retries reached. Logs not sent. Returning to default flush interval.")
                            print(exceptions.get_exception_stacktrace(e))
                        return self.flush_interval

                    if self.verbose:
                        print(f"Retrying in {DEFAULT_FLUSH_INTERVAL} seconds...")
                    time.sleep(DEFAULT_FLUSH_INTERVAL)

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
            if self.verbose:
                print(f"X-Soda-Next-Batch-Time invalid date format: {next_batch_time}")
            return 0
