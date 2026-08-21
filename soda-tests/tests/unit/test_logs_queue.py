import logging
from unittest.mock import MagicMock, patch

import pytest
from soda_core.common.logs import Logs
from soda_core.common.logs_collector import LogsCollector
from soda_core.common.logs_queue import LogsQueue


def test_logs_queue_requires_an_identifier():
    with pytest.raises(ValueError):
        LogsQueue(soda_cloud=MagicMock(), stage="test_connection")


def _stopped_queue(**kwargs) -> LogsQueue:
    soda_cloud = MagicMock()
    soda_cloud.logs_batch_v4.return_value.headers.get.return_value = None
    soda_cloud.logs_batch.return_value.headers.get.return_value = None
    logs_queue = LogsQueue(soda_cloud=soda_cloud, stage="test_connection", **kwargs)
    # Stop the background worker so it does not race with the manual _flush_logs call.
    logs_queue.shutdown_flag.set()
    with logs_queue.condition:
        logs_queue.condition.notify()
    logs_queue.worker_thread.join()
    return logs_queue


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_flush_uses_batch_v4_when_scan_id_set(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.log_queue.put(MagicMock())

    logs_queue._flush_logs(logs_queue.flush_interval)

    logs_queue.soda_cloud.logs_batch_v4.assert_called_once_with(scan_id="scan-id-123", body="")
    logs_queue.soda_cloud.logs_batch.assert_not_called()


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_flush_uses_batch_v3_when_only_scan_reference_set(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_reference="scan-ref-abc")
    logs_queue.log_queue.put(MagicMock())

    logs_queue._flush_logs(logs_queue.flush_interval)

    logs_queue.soda_cloud.logs_batch.assert_called_once_with(scan_reference="scan-ref-abc", body="")
    logs_queue.soda_cloud.logs_batch_v4.assert_not_called()


# Streaming-mode accessors: get_all_logs is empty (streamed records are not
# re-gatherable — this is what keeps a streaming run's results payload `logs`
# field empty), and failure reports attach only records not confirmed flushed.


def _record(level: int, msg: str) -> logging.LogRecord:
    return logging.LogRecord(name="soda", level=level, pathname=__file__, lineno=1, msg=msg, args=(), exc_info=None)


def _flush_response(status_code: int) -> MagicMock:
    response = MagicMock(status_code=status_code)
    response.headers.get.return_value = None
    return response


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_get_all_logs_returns_empty_list_for_streaming_gatherer(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.emit(_record(logging.INFO, "streamed away"))

    assert logs_queue.get_all_logs() == []


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_confirmed_flushed_error_records_left_out_of_failure_report(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _flush_response(200)
    logs_queue.emit(_record(logging.ERROR, "flushed error"))
    logs_queue._flush_logs(logs_queue.flush_interval)
    logs_queue.emit(_record(logging.ERROR, "pending error"))

    reported = logs_queue.records_for_failure_report()

    assert [record.getMessage() for record in reported] == ["pending error"]


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_non_2xx_flush_keeps_error_records_in_failure_report(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
    logs_queue.soda_cloud.logs_batch_v4.return_value = _flush_response(500)
    logs_queue.emit(_record(logging.ERROR, "rejected error"))

    logs_queue._flush_logs(logs_queue.flush_interval)

    assert [record.getMessage() for record in logs_queue.records_for_failure_report()] == ["rejected error"]
    # The drop-after-response flush behavior itself is unchanged: the batch is consumed.
    assert logs_queue.log_queue.empty()


@patch("soda_core.common.logs_queue._to_jsonl", return_value="")
def test_failure_report_only_carries_error_records(mock_to_jsonl):
    logs_queue = _stopped_queue(scan_id="scan-id-123")
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
