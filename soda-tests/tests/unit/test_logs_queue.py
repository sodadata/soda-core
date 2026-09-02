import json
import logging
import threading
from unittest.mock import MagicMock, patch

import pytest
import soda_core.common.logs_queue as logs_queue_module
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.logs_base import THREAD_LABEL_ATTR
from soda_core.common.logs_collector import LogsCollector
from soda_core.common.logs_queue import LogsQueue, build_streaming_gatherer


@pytest.fixture(autouse=True)
def _no_retry_backoff(monkeypatch):
    # The retry path waits on the shutdown event between attempts; zero the delay so tests that
    # drive retries (on a live or stopped queue) never sleep.
    monkeypatch.setattr(logs_queue_module, "RETRY_DELAY_SECONDS", 0)


def test_logs_queue_requires_an_identifier():
    with pytest.raises(ValueError):
        LogsQueue(soda_cloud=MagicMock(), stage="main")


def _response(status_code: int) -> MagicMock:
    # A real int status code: the flush classifies retryability on it.
    response = MagicMock(status_code=status_code)
    response.headers.get.return_value = None
    return response


def _stopped_queue(**kwargs) -> LogsQueue:
    soda_cloud = MagicMock()
    soda_cloud.logs_batch_v4.return_value = _response(200)
    soda_cloud.logs_batch.return_value = _response(200)
    logs_queue = LogsQueue(soda_cloud=soda_cloud, stage="main", **kwargs)
    # Stop the background worker so it does not race with the manual _flush_logs call.
    logs_queue.shutdown_flag.set()
    with logs_queue.condition:
        logs_queue.condition.notify()
    logs_queue.worker_thread.join()
    return logs_queue


def _record(level: int, msg: str) -> logging.LogRecord:
    return logging.LogRecord(name="soda", level=level, pathname=__file__, lineno=1, msg=msg, args=(), exc_info=None)


# Endpoint keying: scan-id-keyed streams post to batchV4, scan-reference-keyed ones to the
# batchV3 endpoint existing library consumers rely on.


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_flush_uses_batch_v4_when_scan_id_set(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.log_queue.put(_record(logging.INFO, "queued line"))

    logs_queue._flush_logs(logs_queue.flush_interval)

    logs_queue.soda_cloud.logs_batch_v4.assert_called_once_with(scan_id="scan-id-123", body="")
    logs_queue.soda_cloud.logs_batch.assert_not_called()


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_flush_uses_batch_v3_when_only_scan_reference_set(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_reference="scan-ref-abc")
    logs_queue.log_queue.put(_record(logging.INFO, "queued line"))

    logs_queue._flush_logs(logs_queue.flush_interval)

    logs_queue.soda_cloud.logs_batch.assert_called_once_with(scan_reference="scan-ref-abc", body="")
    logs_queue.soda_cloud.logs_batch_v4.assert_not_called()


# build_streaming_gatherer: the construction site for scan-id-keyed streaming.


@patch("soda_core.common.logs_queue.EnvConfigHelper")
def test_build_streaming_gatherer_returns_none_without_scan_id(mock_env_config_helper_cls):
    mock_env_config_helper_cls.return_value.soda_scan_id = None

    assert build_streaming_gatherer(MagicMock()) is None


def test_build_streaming_gatherer_returns_none_without_soda_cloud():
    assert build_streaming_gatherer(None, scan_id="scan-id-123") is None


def test_build_streaming_gatherer_builds_a_main_stage_scan_id_keyed_queue():
    soda_cloud = MagicMock()

    gatherer = build_streaming_gatherer(soda_cloud, scan_id="scan-id-123")
    try:
        assert isinstance(gatherer, LogsQueue)
        assert gatherer.scan_id == "scan-id-123"
        assert gatherer.scan_reference is None
        assert gatherer.stage == "main"
    finally:
        gatherer.close()


@patch("soda_core.common.logs_queue.EnvConfigHelper")
def test_build_streaming_gatherer_reads_scan_id_from_env_when_not_passed(mock_env_config_helper_cls):
    mock_env_config_helper_cls.return_value.soda_scan_id = "scan-id-env"

    gatherer = build_streaming_gatherer(MagicMock())
    try:
        assert gatherer.scan_id == "scan-id-env"
    finally:
        gatherer.close()


# Streaming-mode accessors: get_all_logs is empty (streamed records are not re-gatherable — this
# is what keeps a streaming run's results payload `logs` field empty), and failure reports flush
# first, then attach only what the stream could not deliver.


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_get_all_logs_returns_empty_list_for_streaming_gatherer(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "streamed away"))

    assert logs_queue.get_all_logs() == []


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_failure_report_is_empty_when_the_stream_is_healthy(mock_to_jsonl):
    # The choke point: sodaCoreMarkScanFailed REPLACES a scan's stored logs, so the report must be
    # empty whenever the stream can deliver — records_for_failure_report flushes to make that so.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.ERROR, "boom"))

    assert logs_queue.records_for_failure_report() == []
    # The flush is how the answer is computed: the error was shipped, not forgotten.
    logs_queue.soda_cloud.logs_batch_v4.assert_called_once()


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_failure_report_attaches_only_what_the_stream_could_not_deliver(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = [_response(200), _response(400)]
    logs_queue.emit(_record(logging.ERROR, "delivered error"))
    logs_queue.flush()
    logs_queue.emit(_record(logging.ERROR, "undelivered error"))

    reported = logs_queue.records_for_failure_report()

    assert [record.getMessage() for record in reported] == ["undelivered error"]


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_failure_report_only_carries_error_records(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _response(400)
    logs_queue.emit(_record(logging.INFO, "progress line"))
    logs_queue.emit(_record(logging.ERROR, "error line"))

    assert [record.getMessage() for record in logs_queue.records_for_failure_report()] == ["error line"]


def test_logs_collector_failure_report_returns_all_records():
    collector = LogsCollector()
    collector.emit(_record(logging.INFO, "info line"))
    collector.emit(_record(logging.ERROR, "error line"))

    reported = collector.records_for_failure_report()

    assert [record.getMessage() for record in reported] == ["info line", "error line"]


def test_logs_records_for_failure_report_delegates_to_gatherer():
    gatherer = MagicMock()
    logs = Logs(gatherer=gatherer)
    try:
        assert logs.records_for_failure_report() is gatherer.records_for_failure_report.return_value
    finally:
        logs.close()


def test_switch_gatherer_replays_history_and_closes_the_old_gatherer():
    class _ClosableCollector(LogsCollector):
        closed = False

        def close(self):
            self.closed = True

    old_gatherer = _ClosableCollector()
    old_gatherer.emit(_record(logging.INFO, "captured before the switch"))
    new_gatherer = MagicMock()
    logs = Logs(gatherer=old_gatherer)
    try:
        logs.switch_gatherer(new_gatherer)

        assert logs.gatherer is new_gatherer
        (replayed_record,), _ = new_gatherer.emit.call_args
        assert replayed_record.getMessage() == "captured before the switch"
        assert old_gatherer.closed
    finally:
        logs.close()


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_streaming_gatherer_serves_error_status_determination(mock_to_jsonl):
    # The status-determination surface (has_errors/get_errors) must work on a real LogsQueue: a streaming
    # run's error records are preserved and reported exactly like the in-memory collector's.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs = Logs(gatherer=logs_queue)
    try:
        logs_queue.emit(_record(logging.INFO, "progress line"))
        assert logs.has_errors is False

        logs_queue.emit(_record(logging.ERROR, "error line"))

        assert [record.getMessage() for record in logs_queue.get_error_logs()] == ["error line"]
        assert logs.has_errors is True
        assert logs.get_errors() == ["error line"]
    finally:
        logs.close()


# The retry/drop matrix. The stream is the run's only log channel once live, so a failed upload is
# retried when retrying can help (5xx, 408, 429, exceptions) and every dropped batch is counted and
# reported — a silently truncated log stream looks exactly like a quiet run.


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_retryable_failure_is_retried_and_recovers(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = [_response(503), _response(200)]
    logs_queue.emit(_record(logging.ERROR, "eventually delivered"))

    logs_queue.flush()

    assert logs_queue.soda_cloud.logs_batch_v4.call_count == 2
    assert logs_queue._dropped_batch_count == 0
    # Confirmed on the retry: the error no longer belongs in a failure report.
    assert [r.getMessage() for r in logs_queue.logs if id(r) in logs_queue._flushed_records] == ["eventually delivered"]


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_persistent_retryable_failure_drops_the_batch_after_max_retries(mock_to_jsonl, caplog):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _response(503)
    logs_queue.emit(_record(logging.INFO, "lost line"))

    with caplog.at_level(logging.WARNING, logger="soda.logs_stream"):
        logs_queue.flush()

    assert logs_queue.soda_cloud.logs_batch_v4.call_count == logs_queue_module.MAX_RETRIES
    assert logs_queue._dropped_batch_count == 1
    assert logs_queue._dropped_record_count == 1
    assert "Dropping 1 log record(s) after 3 attempt(s): HTTP 503" in caplog.text


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_plain_4xx_drops_immediately_without_retrying(mock_to_jsonl, caplog):
    # A rejected upload never becomes acceptable; retrying only delays the run. The reported
    # attempt count is the real one, not MAX_RETRIES.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _response(400)
    logs_queue.emit(_record(logging.INFO, "rejected line"))

    with caplog.at_level(logging.WARNING, logger="soda.logs_stream"):
        logs_queue.flush()

    logs_queue.soda_cloud.logs_batch_v4.assert_called_once()
    assert logs_queue._dropped_batch_count == 1
    assert "after 1 attempt(s): HTTP 400" in caplog.text


@pytest.mark.parametrize("status_code", [408, 429])
@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_retry_later_4xx_is_retried(mock_to_jsonl, status_code):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = [_response(status_code), _response(200)]
    logs_queue.emit(_record(logging.INFO, "delayed line"))

    logs_queue.flush()

    assert logs_queue.soda_cloud.logs_batch_v4.call_count == 2
    assert logs_queue._dropped_batch_count == 0


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_transport_exception_is_retried_then_dropped(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = ConnectionError("network down")
    logs_queue.emit(_record(logging.INFO, "lost line"))

    logs_queue.flush()

    assert logs_queue.soda_cloud.logs_batch_v4.call_count == logs_queue_module.MAX_RETRIES
    assert logs_queue._dropped_batch_count == 1
    assert "network down" in logs_queue._last_drop_reason


@patch("soda_core.common.logs_queue._to_jsonl", side_effect=TypeError("not serialisable"))
def test_serialisation_failure_drops_the_batch_without_posting(mock_to_jsonl):
    # Not retryable — the same records would fail to serialise the same way — and still accounted
    # for: an escaping serialisation error would otherwise kill the worker with zero drops counted.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "unserialisable line"))

    logs_queue.flush()

    logs_queue.soda_cloud.logs_batch_v4.assert_not_called()
    assert logs_queue._dropped_batch_count == 1
    assert "could not be serialised" in logs_queue._last_drop_reason


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_close_summarises_the_dropped_records(mock_to_jsonl, caplog):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _response(400)
    logs_queue.emit(_record(logging.INFO, "lost line one"))
    logs_queue.emit(_record(logging.INFO, "lost line two"))
    logs_queue.flush()

    with caplog.at_level(logging.ERROR, logger="soda.logs_stream"):
        logs_queue.close()

    assert "2 log record(s) in 1 batch(es) could not be streamed to Soda Cloud" in caplog.text
    assert "HTTP 400" in caplog.text


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_close_stays_silent_when_nothing_was_dropped(mock_to_jsonl, caplog):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "delivered line"))

    with caplog.at_level(logging.ERROR, logger="soda.logs_stream"):
        logs_queue.close()

    assert "could not be streamed" not in caplog.text


def test_worker_survives_an_unexpected_flush_failure():
    soda_cloud = MagicMock()
    soda_cloud.logs_batch_v4.return_value = _response(200)
    logs_queue = LogsQueue(soda_cloud=soda_cloud, stage="main", scan_id="scan-id-123")
    try:
        # After a failed flush the worker re-arms on self.flush_interval; keep that short so the
        # second cycle fires without depending on a notify racing the worker's wait.
        logs_queue.flush_interval = 0.05
        flushed = threading.Event()

        def boom(interval):
            flushed.set()
            raise RuntimeError("boom")

        logs_queue._flush_logs = boom
        with logs_queue.condition:
            logs_queue.condition.notify()
        assert flushed.wait(timeout=5)
        # A second cycle still reaches the flush: the worker did not die on the first raise —
        # a dead worker is a silent stream stop, indistinguishable from a quiet run.
        flushed.clear()
        assert flushed.wait(timeout=5)
    finally:
        logs_queue._flush_logs = lambda interval: interval
        logs_queue.close()


# The `thread` value on the wire: Soda Cloud groups log lines by it. A caller-set grouping label
# (marked by the active Logs) survives; anything else gets the queue's own uuid — never the OS
# thread ident every LogRecord carries by default.


def test_thread_label_wire_value_for_labelled_and_unlabelled_records():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs = Logs(gatherer=logs_queue)
    try:
        soda_logger.info("unlabelled line")
        with logs.activate("my_collection"):
            soda_logger.info("labelled line")
        logs_queue.flush()
    finally:
        logs.close()

    body: str = logs_queue.soda_cloud.logs_batch_v4.call_args.kwargs["body"]
    thread_by_message = {line["message"]: line["thread"] for line in map(json.loads, body.splitlines())}
    assert thread_by_message["labelled line"] == "my_collection"
    assert thread_by_message["unlabelled line"] == logs_queue.thread


def test_emit_stamps_the_queue_uuid_over_the_os_thread_ident():
    # Every stdlib LogRecord carries `thread` (an OS ident int); only a marked label survives.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    unlabelled = _record(logging.INFO, "unlabelled")
    labelled = _record(logging.INFO, "labelled")
    labelled.thread = "my_collection"
    setattr(labelled, THREAD_LABEL_ATTR, True)

    logs_queue.emit(unlabelled)
    logs_queue.emit(labelled)

    assert unlabelled.thread == logs_queue.thread
    assert labelled.thread == "my_collection"


def test_stream_diagnostics_are_never_captured_by_an_active_logs():
    # The stream's own diagnostics must not feed back into the queue they report on (or into any
    # run's captured logs): _RootCapturer refuses the stream-diagnostics logger.
    logs = Logs()
    try:
        logs_queue_module.stream_logger.warning("dropping a batch")
        soda_logger.warning("a regular record")
    finally:
        logs.close()

    captured = [record.getMessage() for record in logs.get_log_records()]
    assert "a regular record" in captured
    assert "dropping a batch" not in captured
