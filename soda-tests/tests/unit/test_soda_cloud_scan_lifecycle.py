from unittest.mock import MagicMock, patch

from soda_core.common.soda_cloud import SodaCloud

# SodaCloud.scan_start / insert_scan_data_batch / scan_end_async: the async
# batched-ingestion bracket. Same bool/None-on-failure contracts as
# insert_scan_results / mark_scan_as_failed — no exceptions on non-200.


def _soda_cloud() -> SodaCloud:
    return SodaCloud(
        host="cloud.soda.io", api_key_id="id", api_key_secret="secret", token=None, port=None, scheme="https"
    )


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_sends_scan_id_and_version_and_returns_scan_reference(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True, json=lambda: {"scanReference": "org/ref-1"})

    assert _soda_cloud().scan_start("scan-123") == "org/ref-1"

    command = mock_execute_command.call_args.kwargs["command_json_dict"]
    assert command == {"type": "sodaCoreScanStart", "scanId": "scan-123", "version": "4"}
    assert mock_execute_command.call_args.kwargs["request_log_name"] == "scan_start"


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_includes_optional_fields_when_given(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True, json=lambda: {"scanReference": "org/ref-1"})

    _soda_cloud().scan_start("scan-123", definition_name="my_scan", default_data_source="postgres")

    command = mock_execute_command.call_args.kwargs["command_json_dict"]
    assert command["definitionName"] == "my_scan"
    assert command["defaultDataSource"] == "postgres"


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_on_rejection(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=False)

    assert _soda_cloud().scan_start("scan-123") is None


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_without_response(mock_execute_command):
    mock_execute_command.return_value = None

    assert _soda_cloud().scan_start("scan-123") is None


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_when_response_lacks_scan_reference(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True, json=lambda: {})

    assert _soda_cloud().scan_start("scan-123") is None


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_on_unparsable_body(mock_execute_command):
    response = MagicMock(ok=True)
    response.json.side_effect = ValueError("not json")
    mock_execute_command.return_value = response

    assert _soda_cloud().scan_start("scan-123") is None


@patch.object(SodaCloud, "_execute_command")
def test_insert_scan_data_batch_stamps_type_and_scan_reference_without_mutating_payload(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True)
    payload = {"type": "sodaCoreInsertScanResults", "definitionName": "my_scan", "version": "4"}

    assert _soda_cloud().insert_scan_data_batch(payload, scan_reference="org/ref-1") is True

    command = mock_execute_command.call_args.kwargs["command_json_dict"]
    assert command["type"] == "sodaCoreInsertScanDataBatch"
    assert command["scanReference"] == "org/ref-1"
    assert command["definitionName"] == "my_scan"
    assert command["version"] == "4"
    assert mock_execute_command.call_args.kwargs["request_log_name"] == "insert_scan_data_batch"
    # The caller's DTO is untouched: the batch stamps land on a copy.
    assert payload["type"] == "sodaCoreInsertScanResults"
    assert "scanReference" not in payload


@patch.object(SodaCloud, "_execute_command")
def test_insert_scan_data_batch_returns_false_when_rejected(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=False)

    assert _soda_cloud().insert_scan_data_batch({"type": "sodaCoreInsertScanResults"}, scan_reference="r") is False


@patch.object(SodaCloud, "_execute_command")
def test_insert_scan_data_batch_returns_false_without_response(mock_execute_command):
    mock_execute_command.return_value = None

    assert _soda_cloud().insert_scan_data_batch({"type": "sodaCoreInsertScanResults"}, scan_reference="r") is False


@patch.object(SodaCloud, "_execute_command")
def test_scan_end_async_sends_scan_reference(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True)

    assert _soda_cloud().scan_end_async("org/ref-1") is True

    command = mock_execute_command.call_args.kwargs["command_json_dict"]
    assert command == {"type": "sodaCoreScanEndAsync", "scanReference": "org/ref-1"}
    assert mock_execute_command.call_args.kwargs["request_log_name"] == "scan_end_async"


@patch.object(SodaCloud, "_execute_command")
def test_scan_end_async_returns_false_when_rejected(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=False)

    assert _soda_cloud().scan_end_async("org/ref-1") is False


@patch.object(SodaCloud, "_execute_command")
def test_scan_end_async_returns_false_without_response(mock_execute_command):
    mock_execute_command.return_value = None

    assert _soda_cloud().scan_end_async("org/ref-1") is False
