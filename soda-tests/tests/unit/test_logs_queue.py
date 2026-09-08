import json
import logging
import threading
from unittest.mock import MagicMock

import pytest
import soda_core.common.logs_queue as logs_queue_module
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.logs_base import THREAD_LABEL_ATTR
from soda_core.common.logs_collector import LogsCollector
from soda_core.common.logs_queue import LogsQueue, build_streaming_gatherer


def test_logs_queue_requires_an_identifier():
    with pytest.raises(ValueError):
        LogsQueue(soda_cloud=MagicMock(), stage="main")


def _response(status_code: int) -> MagicMock:
    # A real int status code: the flush classifies the response on it.
    response = MagicMock(status_code=status_code)
    response.headers.get.return_value = None
    return response


def _stopped_queue(**kwargs) -> LogsQueue:
    soda_cloud = MagicMock()
    soda_cloud.logs_batch_v4.return_value = _response(200)
    soda_cloud.logs_batch.return_value = _response(200)
    logs_queue = LogsQueue(soda_cloud=soda_cloud, stage="main", **kwargs)
    # Stop the background worker so it does not race with the manual flush calls.
    logs_queue.shutdown_flag.set()
    logs_queue.worker_thread.join()
    return logs_queue


def _record(level: int, msg: str) -> logging.LogRecord:
    return logging.LogRecord(name="soda", level=level, pathname=__file__, lineno=1, msg=msg, args=(), exc_info=None)


def _sent_messages(mock_post: MagicMock) -> list[list[str]]:
    """Per upload, the messages in its jsonl body."""
    return [
        [json.loads(line)["message"] for line in call.kwargs["body"].splitlines()] for call in mock_post.call_args_list
    ]


# Endpoint keying: scan-id-keyed streams post to batchV4, scan-reference-keyed ones to the
# batchV3 endpoint existing library consumers rely on.


def test_flush_uses_batch_v4_when_scan_id_set():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "queued line"))

    logs_queue.flush()

    assert logs_queue.soda_cloud.logs_batch_v4.call_args.kwargs["scan_id"] == "scan-id-123"
    logs_queue.soda_cloud.logs_batch.assert_not_called()


def test_flush_uses_batch_v3_when_only_scan_reference_set():
    logs_queue = _stopped_queue(scan_reference="scan-ref-abc")
    logs_queue.emit(_record(logging.INFO, "queued line"))

    logs_queue.flush()

    assert logs_queue.soda_cloud.logs_batch.call_args.kwargs["scan_reference"] == "scan-ref-abc"
    logs_queue.soda_cloud.logs_batch_v4.assert_not_called()


# build_streaming_gatherer: the construction site for scan-id-keyed streaming. Callers resolve
# the scan id themselves (EnvConfigHelper is the one place that reads SODA_SCAN_ID).


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


# The delivery invariant: _pending holds exactly the unacknowledged records. A 2xx removes the
# sent head; any failure leaves the records queued, and the next flush — the worker's cadence in
# production — re-sends them. There is no retry logic to test: the cadence IS the retry.


def test_acknowledged_records_leave_the_queue():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "line one"))
    logs_queue.emit(_record(logging.INFO, "line two"))

    logs_queue.flush()
    logs_queue.flush()

    # The second flush had nothing to send: the ack removed both records.
    assert _sent_messages(logs_queue.soda_cloud.logs_batch_v4) == [["line one", "line two"]]


@pytest.mark.parametrize("failure", [_response(503), _response(408), _response(429), ConnectionError("network down")])
def test_unacknowledged_records_stay_queued_and_ride_the_next_flush(failure):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = [failure, _response(200)]
    logs_queue.emit(_record(logging.INFO, "survives the outage"))

    logs_queue.flush()
    logs_queue.flush()

    # One attempt per flush cycle, the same record both times: nothing was dropped in between.
    assert _sent_messages(logs_queue.soda_cloud.logs_batch_v4) == [["survives the outage"]] * 2


def test_records_emitted_during_an_outage_ride_along_afterwards():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = [_response(503), _response(200)]
    logs_queue.emit(_record(logging.INFO, "before the outage"))
    logs_queue.flush()
    logs_queue.emit(_record(logging.INFO, "during the outage"))

    logs_queue.flush()

    assert _sent_messages(logs_queue.soda_cloud.logs_batch_v4)[-1] == ["before the outage", "during the outage"]


def test_permanent_rejection_ends_the_stream(caplog):
    # A non-transient 4xx is the backend saying this scan permanently refuses uploads (deleted, or
    # off its log-accepting state — a transition that never reverses). Posting stops; the records'
    # only remaining route to Soda Cloud is a failure report, and close() accounts for them.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _response(400)
    logs_queue.emit(_record(logging.INFO, "refused line"))

    logs_queue.flush()
    logs_queue.emit(_record(logging.ERROR, "error after the refusal"))
    logs_queue.flush()

    logs_queue.soda_cloud.logs_batch_v4.assert_called_once()
    with caplog.at_level(logging.ERROR, logger="soda.logs_stream"):
        logs_queue.close()
    assert "2 log record(s) were not delivered" in caplog.text
    assert "HTTP 400" in caplog.text


def test_records_pending_after_a_permanent_rejection_ride_a_failure_report():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _response(404)
    logs_queue.emit(_record(logging.INFO, "refused line"))
    logs_queue.flush()

    reported = logs_queue.records_for_failure_report()

    assert [record.getMessage() for record in reported] == ["refused line"]


@pytest.mark.parametrize("status_code", [408, 429])
def test_transient_4xx_does_not_end_the_stream(status_code):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = [_response(status_code), _response(200)]
    logs_queue.emit(_record(logging.INFO, "delayed line"))

    logs_queue.flush()
    logs_queue.flush()

    assert logs_queue.soda_cloud.logs_batch_v4.call_count == 2


# Serialization: per record, evicting the incurables. Pending records only leave the queue on
# delivery, so an unserializable record (e.g. an exotic object in a serialized attribute like
# ``doc`` — to_jsonnable raises on unknown types) would otherwise re-fail every future flush and
# block everything behind it. (A %-format mismatch cannot reach the queue: _mask_record formats the
# message at capture time and _RootCapturer swallows that raise.)


def _unserializable_record(msg: str) -> logging.LogRecord:
    record = _record(logging.INFO, msg)
    record.doc = object()
    return record


def test_unserializable_record_is_evicted_and_its_neighbours_still_deliver(caplog):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "healthy before"))
    logs_queue.emit(_unserializable_record("bad record"))
    logs_queue.emit(_record(logging.INFO, "healthy after"))

    with caplog.at_level(logging.WARNING, logger="soda.logs_stream"):
        logs_queue.flush()

    assert _sent_messages(logs_queue.soda_cloud.logs_batch_v4) == [["healthy before", "healthy after"]]
    assert "Evicting an unserializable record" in caplog.text
    # Gone for good: the next flush does not resend it, and no failure report carries it (the
    # report payload would hit the same serialization failure).
    logs_queue.flush()
    assert logs_queue.soda_cloud.logs_batch_v4.call_count == 1
    assert logs_queue.records_for_failure_report() == []


def test_fully_unserializable_batch_posts_nothing_and_clears():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_unserializable_record("bad record"))

    logs_queue.flush()

    logs_queue.soda_cloud.logs_batch_v4.assert_not_called()
    assert logs_queue.records_for_failure_report() == []


# Failure reports: sodaCoreMarkScanFailed REPLACES a scan's stored logs, so the report must be
# empty whenever the stream could deliver — records_for_failure_report flushes to make that so —
# and otherwise attach exactly the undelivered records, which it takes off the queue (hand-over).


def test_failure_report_is_empty_when_the_stream_is_healthy():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.ERROR, "boom"))

    assert logs_queue.records_for_failure_report() == []
    # The flush is how the answer is computed: the error was shipped, not forgotten.
    logs_queue.soda_cloud.logs_batch_v4.assert_called_once()


def test_failure_report_attaches_every_undelivered_record():
    # All levels, not just errors: whatever the stream could not deliver reached Cloud through no
    # other channel, and the report is its delivery.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.side_effect = [_response(200), _response(503)]
    logs_queue.emit(_record(logging.ERROR, "delivered error"))
    logs_queue.flush()
    logs_queue.emit(_record(logging.INFO, "undelivered progress"))
    logs_queue.emit(_record(logging.ERROR, "undelivered error"))

    reported = logs_queue.records_for_failure_report()

    assert [record.getMessage() for record in reported] == ["undelivered progress", "undelivered error"]


def test_failure_report_retires_the_stream(caplog):
    # The hand-over: the caller's next act is sodaCoreMarkScanFailed, after which the backend
    # refuses further uploads for the scan. Later records stay console-only and are NOT counted as
    # undelivered at close (the false alarm right after a successful failure report).
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.ERROR, "boom"))

    assert logs_queue.records_for_failure_report() == []
    logs_queue.emit(_record(logging.INFO, "reported the failure to Soda Cloud"))
    logs_queue.flush()

    # Only the pre-report flush went out; the post-report record was never queued for upload.
    logs_queue.soda_cloud.logs_batch_v4.assert_called_once()
    with caplog.at_level(logging.ERROR, logger="soda.logs_stream"):
        logs_queue.close()
    assert "were not delivered" not in caplog.text


def test_retired_stream_still_serves_status_determination():
    # A run can continue after a mid-run failure report (metric monitoring's errored_without_results
    # branch): error records emitted after retirement must keep driving has_errors, even though
    # they can no longer reach Soda Cloud.
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.records_for_failure_report()

    logs_queue.emit(_record(logging.ERROR, "post-report error"))

    assert [record.getMessage() for record in logs_queue.get_error_logs()] == ["post-report error"]
    logs_queue.soda_cloud.logs_batch_v4.assert_not_called()


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


# Streaming-mode accessors: get_all_logs is empty (streamed records are not re-gatherable — this
# is what keeps a streaming run's results payload `logs` field empty); the status-determination
# surface works exactly like the in-memory collector's.


def test_get_all_logs_returns_empty_list_for_streaming_gatherer():
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "streamed away"))

    assert logs_queue.get_all_logs() == []


def test_streaming_gatherer_serves_error_status_determination():
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


# close(): deliver what remains, then account for anything that could not be delivered — a
# silently truncated log stream looks exactly like a quiet run.


def test_close_delivers_the_remaining_records_silently(caplog):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "delivered at close"))

    with caplog.at_level(logging.ERROR, logger="soda.logs_stream"):
        logs_queue.close()

    assert _sent_messages(logs_queue.soda_cloud.logs_batch_v4) == [["delivered at close"]]
    assert "were not delivered" not in caplog.text


def test_close_reports_records_the_final_flush_could_not_deliver(caplog):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _response(503)
    logs_queue.emit(_record(logging.INFO, "lost line one"))
    logs_queue.emit(_record(logging.INFO, "lost line two"))

    with caplog.at_level(logging.ERROR, logger="soda.logs_stream"):
        logs_queue.close()

    assert "2 log record(s) were not delivered" in caplog.text
    assert "final flush" in caplog.text


def test_worker_survives_an_unexpected_flush_failure(monkeypatch):
    monkeypatch.setattr(logs_queue_module, "DEFAULT_FLUSH_INTERVAL", 0.05)
    soda_cloud = MagicMock()
    soda_cloud.logs_batch_v4.return_value = _response(200)
    logs_queue = LogsQueue(soda_cloud=soda_cloud, stage="main", scan_id="scan-id-123")
    try:
        flushed = threading.Event()

        def boom(interval):
            flushed.set()
            raise RuntimeError("boom")

        logs_queue._flush_logs = boom
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
