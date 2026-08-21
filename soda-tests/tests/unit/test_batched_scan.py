import logging
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.batched_scan import BatchedScanContext, run_batched_scan
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.logs_collector import LogsCollector

# run_batched_scan: the opt-in bracket for Cloud-launched results-publishing
# commands. The flow opens the bracket itself via context.start_scan once its
# dependencies resolve; the fallback matrix — no scan id, rejected start,
# happy path — plus the failure-report and end-of-scan orderings.

DATA_TIMESTAMP = datetime(2026, 7, 13, 8, 30, tzinfo=timezone.utc)


def _payload() -> dict:
    return {"type": "sodaCoreInsertScanResults", "definitionName": "my_scan"}


class _StreamingGathererStub(LogsCollector):
    """LogsQueue stand-in with its streaming-mode contract: records preserved on emit (via the collector
    base), get_all_logs empty, failure reports select error records only. ``on_close`` lets ordering tests
    observe the final flush."""

    def __init__(self, on_close=None):
        super().__init__()
        self.reset()
        self.closed = False
        self._on_close = on_close

    def get_all_logs(self):
        return []

    def records_for_failure_report(self):
        return [record for record in self.logs if record.levelno >= logging.ERROR]

    def close(self):
        self.closed = True
        if self._on_close is not None:
            self._on_close()


def test_context_insert_results_batches_when_scan_reference_set():
    soda_cloud = MagicMock()
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id="scan-123", scan_reference="org/ref-1")

    context.insert_results(_payload())

    soda_cloud.insert_scan_data_batch.assert_called_once_with(_payload(), "org/ref-1")
    soda_cloud.insert_scan_results.assert_not_called()
    context.logs.close()


def test_context_insert_results_falls_back_to_sync_without_scan_reference():
    soda_cloud = MagicMock()
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id=None)

    context.insert_results(_payload())

    soda_cloud.insert_scan_results.assert_called_once_with(_payload())
    soda_cloud.insert_scan_data_batch.assert_not_called()
    context.logs.close()


def test_start_scan_is_a_no_op_without_scan_id():
    soda_cloud = MagicMock()
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id=None)

    context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

    soda_cloud.scan_start.assert_not_called()
    assert context.scan_reference is None
    assert isinstance(context.logs.gatherer, LogsCollector)
    context.logs.close()


@patch("soda_core.common.logs_queue.LogsQueue")
def test_start_scan_switches_to_streaming_and_replays_captured_records(mock_logs_queue_cls):
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    stub = _StreamingGathererStub()
    mock_logs_queue_cls.return_value = stub

    logs = Logs()
    try:
        soda_logger.info("captured before the scan started")
        context = BatchedScanContext(logs=logs, soda_cloud=soda_cloud, scan_id="scan-123", stage="main")

        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

        soda_cloud.scan_start.assert_called_once_with("scan-123", "my_scan", "postgres", DATA_TIMESTAMP)
        mock_logs_queue_cls.assert_called_once_with(soda_cloud=soda_cloud, stage="main", scan_id="scan-123", dataset="")
        assert context.scan_reference == "org/ref-1"
        assert logs.gatherer is stub
        # The pre-start history was replayed into the stream.
        assert any("captured before the scan started" in record.getMessage() for record in stub.logs)
        # From now on the payload log fill sites see no records: the stream is
        # the single log channel.
        assert logs.get_log_records() == []
    finally:
        logs.close()


@patch("soda_core.common.logs_queue.LogsQueue")
def test_start_scan_is_idempotent(mock_logs_queue_cls):
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    mock_logs_queue_cls.return_value = _StreamingGathererStub()
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id="scan-123")

    context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
    context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)

    soda_cloud.scan_start.assert_called_once()
    context.logs.close()


def test_start_scan_rejected_stays_on_the_sync_path_with_in_memory_logs():
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = None
    logs = Logs()
    context = BatchedScanContext(logs=logs, soda_cloud=soda_cloud, scan_id="scan-123")

    context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
    context.insert_results(_payload())

    assert context.scan_reference is None
    # In-memory logs survive a rejected start: the payload keeps carrying them
    # (the backend would refuse batchV4 uploads for a never-started scan).
    assert isinstance(logs.gatherer, LogsCollector)
    soda_cloud.insert_scan_results.assert_called_once()
    soda_cloud.insert_scan_data_batch.assert_not_called()
    logs.close()


def test_run_batched_scan_without_scan_id_is_fully_sync(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    soda_cloud = MagicMock()
    seen = {}

    def command(context: BatchedScanContext) -> ExitCode:
        seen["context"] = context
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        context.insert_results(_payload())
        return ExitCode.OK

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.OK
    soda_cloud.scan_start.assert_not_called()
    soda_cloud.scan_end_async.assert_not_called()
    soda_cloud.insert_scan_results.assert_called_once()
    soda_cloud.insert_scan_data_batch.assert_not_called()
    assert seen["context"].scan_id is None
    assert isinstance(seen["context"].logs.gatherer, LogsCollector)


@patch("soda_core.common.logs_queue.LogsQueue")
def test_run_batched_scan_happy_path_starts_batches_flushes_then_ends(mock_logs_queue_cls, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    order = []
    mock_logs_queue_cls.return_value = _StreamingGathererStub(on_close=lambda: order.append("flush"))
    soda_cloud.insert_scan_data_batch.side_effect = lambda *args, **kwargs: order.append("insert") or True
    soda_cloud.scan_end_async.side_effect = lambda *args, **kwargs: order.append("end") or True

    def command(context: BatchedScanContext) -> ExitCode:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        assert context.insert_results(_payload()) is True
        return ExitCode.OK

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.OK
    soda_cloud.scan_start.assert_called_once_with("scan-123", "my_scan", "postgres", DATA_TIMESTAMP)
    # The stream's final flush (gatherer close) happens BEFORE the scan is ended.
    assert order == ["insert", "flush", "end"]
    soda_cloud.scan_end_async.assert_called_once_with("org/ref-1")


def test_run_batched_scan_degrades_to_sync_when_scan_start_fails(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = None

    def command(context: BatchedScanContext) -> ExitCode:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        context.insert_results(_payload())
        return ExitCode.OK

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.OK
    soda_cloud.insert_scan_results.assert_called_once()
    soda_cloud.insert_scan_data_batch.assert_not_called()
    soda_cloud.scan_end_async.assert_not_called()


@patch("soda_core.common.logs_queue.LogsQueue")
def test_run_batched_scan_failure_after_start_reports_unsent_errors_and_leaves_the_scan_unended(
    mock_logs_queue_cls, monkeypatch
):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    mock_logs_queue_cls.return_value = _StreamingGathererStub()
    soda_cloud.mark_scan_as_failed.return_value = True

    def command(context: BatchedScanContext) -> ExitCode:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        raise ValueError("boom")

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_called_once()
    # No results were delivered: the scan is NOT ended — the failure report owns its terminal state.
    soda_cloud.scan_end_async.assert_not_called()
    # The report carries the streaming gatherer's selection: error records only.
    reported = soda_cloud.mark_scan_as_failed.call_args.kwargs["logs"]
    assert reported and all(record.levelno >= logging.ERROR for record in reported)
    assert any("boom" in record.getMessage() for record in reported)


def test_run_batched_scan_failure_before_start_reports_the_full_record_list(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = True

    def command(context: BatchedScanContext) -> ExitCode:
        soda_logger.info("resolution progress")
        raise ValueError("resolution failed")

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.scan_end_async.assert_not_called()
    reported = soda_cloud.mark_scan_as_failed.call_args.kwargs["logs"]
    # Nothing was streamed: today's behavior — the full record list rides the report.
    assert any("resolution progress" in record.getMessage() for record in reported)


@patch("soda_core.common.logs_queue.LogsQueue")
def test_run_batched_scan_end_failure_is_not_fatal(mock_logs_queue_cls, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    soda_cloud.insert_scan_data_batch.return_value = True
    soda_cloud.scan_end_async.return_value = False
    mock_logs_queue_cls.return_value = _StreamingGathererStub()

    def command(context: BatchedScanContext) -> ExitCode:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        assert context.insert_results(_payload()) is True
        return ExitCode.OK

    assert run_batched_scan(soda_cloud, stage="main", command=command) == ExitCode.OK
    soda_cloud.scan_end_async.assert_called_once()


@patch("soda_core.common.logs_queue.LogsQueue")
def test_run_batched_scan_rejected_results_leave_the_scan_unended(mock_logs_queue_cls, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    soda_cloud.insert_scan_data_batch.return_value = False
    mock_logs_queue_cls.return_value = _StreamingGathererStub()

    def command(context: BatchedScanContext) -> ExitCode:
        context.start_scan("my_scan", "postgres", DATA_TIMESTAMP)
        return ExitCode.OK if context.insert_results(_payload()) else ExitCode.RESULTS_NOT_SENT_TO_CLOUD

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    # Undelivered results: ending the scan would close it "cleanly" with nothing in it — the exit-code
    # fallback (launcher) owns the terminal state instead.
    soda_cloud.scan_end_async.assert_not_called()
