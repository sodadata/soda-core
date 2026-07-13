import logging
from unittest.mock import MagicMock

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.failure_reporting import (
    ScanExecutionFailedException,
    report_scan_execution_failure,
)
from soda_core.common.exceptions import SodaCoreException


def test_scan_execution_failed_exception_is_a_soda_core_exception():
    # Callers handling package exceptions via the SodaCoreException base must
    # see ScanExecutionFailedException too.
    assert issubclass(ScanExecutionFailedException, SodaCoreException)


def _log_records() -> list[logging.LogRecord]:
    return [logging.LogRecord("soda", logging.ERROR, __file__, 1, "engine failure details", None, None)]


def test_without_scan_id_exits_log_errors_without_sending(monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    soda_cloud = MagicMock()

    exit_code = report_scan_execution_failure(soda_cloud, _log_records())

    # Ad-hoc run: no Cloud scan to update, errors stay on the console.
    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_not_called()


def test_with_scan_id_but_no_usable_soda_cloud_exits_results_not_sent(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")

    exit_code = report_scan_execution_failure(None, _log_records())

    # Exit > 3 so the managed launcher's fallback marks the scan failed.
    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_with_scan_id_and_mark_accepted_exits_log_errors(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = True
    log_records = _log_records()

    exit_code = report_scan_execution_failure(soda_cloud, log_records)

    assert exit_code == ExitCode.LOG_ERRORS
    soda_cloud.mark_scan_as_failed.assert_called_once_with(scan_id="scan-123", logs=log_records)


def test_with_scan_id_and_mark_rejected_exits_results_not_sent(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.return_value = False

    exit_code = report_scan_execution_failure(soda_cloud, _log_records())

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD


def test_with_scan_id_and_mark_raising_exits_results_not_sent(monkeypatch):
    monkeypatch.setenv("SODA_SCAN_ID", "scan-123")
    soda_cloud = MagicMock()
    soda_cloud.mark_scan_as_failed.side_effect = Exception("cloud unreachable")

    exit_code = report_scan_execution_failure(soda_cloud, _log_records())

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
