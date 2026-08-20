import logging
from unittest.mock import MagicMock, patch

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.batched_scan import BatchedScanContext, run_batched_scan
from soda_core.common.logs import Logs
from soda_core.common.logs_collector import LogsCollector

# run_batched_scan: the opt-in bracket for Cloud-launched results-publishing
# commands. The fallback matrix — no scan id, scan_start failure, happy path —
# plus the failure-report and end-of-scan orderings.


def _payload() -> dict:
    return {"type": "sodaCoreInsertScanResults", "definitionName": "my_scan"}


def test_context_insert_results_batches_when_scan_reference_set():
    soda_cloud = MagicMock()
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id="scan-123", scan_reference="org/ref-1")

    context.insert_results(_payload())

    soda_cloud.insert_scan_data_batch.assert_called_once_with(_payload(), "org/ref-1")
    soda_cloud.insert_scan_results.assert_not_called()
    context.logs.close()


def test_context_insert_results_falls_back_to_sync_without_scan_reference():
    soda_cloud = MagicMock()
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id=None, scan_reference=None)

    context.insert_results(_payload())

    soda_cloud.insert_scan_results.assert_called_once_with(_payload())
    soda_cloud.insert_scan_data_batch.assert_not_called()
    context.logs.close()


def test_run_batched_scan_without_scan_id_is_fully_sync(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    soda_cloud = MagicMock()
    seen = {}

    def command(context: BatchedScanContext) -> ExitCode:
        seen["context"] = context
        context.insert_results(_payload())
        return ExitCode.OK

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.OK
    soda_cloud.scan_start.assert_not_called()
    soda_cloud.scan_end_async.assert_not_called()
    soda_cloud.insert_scan_results.assert_called_once()
    soda_cloud.insert_scan_data_batch.assert_not_called()
    assert seen["context"].scan_id is None
    assert seen["context"].scan_reference is None
    assert isinstance(seen["context"].logs.gatherer, LogsCollector)


@patch("soda_core.cli.handlers.data_source.build_streaming_logs", return_value=None)
def test_run_batched_scan_happy_path_batches_then_ends(mock_build_streaming_logs, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    order = []
    soda_cloud.insert_scan_data_batch.side_effect = lambda *args, **kwargs: order.append("insert") or True
    soda_cloud.scan_end_async.side_effect = lambda *args, **kwargs: order.append("end") or True

    def command(context: BatchedScanContext) -> ExitCode:
        assert context.insert_results(_payload()) is True
        return ExitCode.OK

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.OK
    soda_cloud.scan_start.assert_called_once_with("scan-123", None, None)
    assert order == ["insert", "end"]
    mock_build_streaming_logs.assert_called_once_with(soda_cloud, "main", "scan-123")


@patch("soda_core.cli.handlers.data_source.build_streaming_logs", return_value=None)
def test_run_batched_scan_degrades_to_sync_when_scan_start_fails(mock_build_streaming_logs, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = None

    def command(context: BatchedScanContext) -> ExitCode:
        context.insert_results(_payload())
        return ExitCode.OK

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.OK
    soda_cloud.insert_scan_results.assert_called_once()
    soda_cloud.insert_scan_data_batch.assert_not_called()
    soda_cloud.scan_end_async.assert_not_called()


@patch("soda_core.cli.handlers.data_source.build_streaming_logs", return_value=None)
def test_run_batched_scan_failure_reports_then_ends(mock_build_streaming_logs, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    soda_cloud.mark_scan_as_failed.return_value = True
    order = []
    soda_cloud.mark_scan_as_failed.side_effect = lambda *args, **kwargs: order.append("mark") or True
    soda_cloud.scan_end_async.side_effect = lambda *args, **kwargs: order.append("end") or True

    def command(context: BatchedScanContext) -> ExitCode:
        raise ValueError("boom")

    exit_code = run_batched_scan(soda_cloud, stage="main", command=command)

    assert exit_code == ExitCode.LOG_ERRORS
    assert order == ["mark", "end"]
    soda_cloud.scan_end_async.assert_called_once_with("org/ref-1")


@patch("soda_core.cli.handlers.data_source.build_streaming_logs")
def test_run_batched_scan_failure_report_attaches_gatherer_selected_records(mock_build_streaming_logs, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    soda_cloud.mark_scan_as_failed.return_value = True

    selected_records = [
        logging.LogRecord(
            name="soda", level=logging.ERROR, pathname=__file__, lineno=1, msg="unsent", args=(), exc_info=None
        )
    ]
    gatherer = MagicMock()
    gatherer.records_for_failure_report.return_value = selected_records
    mock_build_streaming_logs.return_value = Logs(gatherer=gatherer)

    def command(context: BatchedScanContext) -> ExitCode:
        raise ValueError("boom")

    run_batched_scan(soda_cloud, stage="main", command=command)

    assert soda_cloud.mark_scan_as_failed.call_args.kwargs["logs"] is selected_records
    # The stream's final flush happens via the wrapper's close.
    gatherer.close.assert_called_once()


@patch("soda_core.cli.handlers.data_source.build_streaming_logs", return_value=None)
def test_run_batched_scan_end_failure_is_not_fatal(mock_build_streaming_logs, monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.scan_start.return_value = "org/ref-1"
    soda_cloud.scan_end_async.return_value = False

    exit_code = run_batched_scan(soda_cloud, stage="main", command=lambda context: ExitCode.OK)

    assert exit_code == ExitCode.OK
