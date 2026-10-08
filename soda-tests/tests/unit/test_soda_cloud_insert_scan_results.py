from unittest.mock import MagicMock, patch

from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.soda_cloud_dto import ReportOutcome, SodaCoreInsertScanResultsDTO

# SodaCloud.insert_scan_results: the transport for DTO-building flows
# (discovery today, profiling next). Reports the outcome in full, so a caller can
# tell a refusal apart from a scan Soda Cloud has already finished with.


def _soda_cloud() -> SodaCloud:
    return SodaCloud(
        host="cloud.soda.io", api_key_id="id", api_key_secret="secret", token=None, port=None, scheme="https"
    )


def _payload() -> SodaCoreInsertScanResultsDTO:
    return {
        "type": "sodaCoreInsertScanResults",
        "definitionName": "my_scan",
        "defaultDataSource": "postgres",
        "dataTimestamp": "2026-07-13T08:30:00+00:00",
        "scanStartTimestamp": "2026-07-13T08:30:00+00:00",
        "scanEndTimestamp": "2026-07-13T08:30:05+00:00",
        "hasErrors": False,
        "metadata": [{"datasetQualifiedName": "postgres/soda/public/customers"}],
    }


@patch.object(SodaCloud, "_execute_command")
def test_insert_scan_results_passes_payload_through_and_reports_accepted(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True)
    payload = _payload()

    assert _soda_cloud().insert_scan_results(payload) is ReportOutcome.ACCEPTED

    mock_execute_command.assert_called_once_with(
        command_json_dict=payload,
        request_log_name="insert_scan_results",
    )


@patch.object(SodaCloud, "_execute_command")
def test_insert_scan_results_reports_refused_when_rejected(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=False)

    assert _soda_cloud().insert_scan_results(_payload()) is ReportOutcome.REFUSED


@patch.object(SodaCloud, "_execute_command")
def test_insert_scan_results_reports_refused_without_response(mock_execute_command):
    mock_execute_command.return_value = None

    assert _soda_cloud().insert_scan_results(_payload()) is ReportOutcome.REFUSED
