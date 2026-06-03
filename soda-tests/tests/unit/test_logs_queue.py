from unittest.mock import MagicMock, patch

import pytest

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
