from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from soda_core.common.soda_cloud import SodaCloud

# SodaCloud.scan_start / insert_scan_data_batch / scan_end_async: the async
# batched-ingestion bracket. All three follow the family's bool/None-on-failure
# contracts (no exceptions on non-200).


def _soda_cloud() -> SodaCloud:
    return SodaCloud(
        host="cloud.soda.io", api_key_id="id", api_key_secret="secret", token=None, port=None, scheme="https"
    )


def _scan_start(soda_cloud: SodaCloud, **overrides):
    kwargs = {
        "scan_id": "scan-123",
        "definition_name": "my_scan",
        "default_data_source": "postgres",
        "data_timestamp": datetime(2026, 7, 13, 8, 30, tzinfo=timezone.utc),
    }
    kwargs.update(overrides)
    return soda_cloud.scan_start(**kwargs)


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_sends_the_backend_mandatory_fields_and_returns_scan_reference(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True, json=lambda: {"scanReference": "org/ref-1"})

    assert _scan_start(_soda_cloud()) == "org/ref-1"

    command = mock_execute_command.call_args.kwargs["command_json_dict"]
    # definitionName / defaultDataSource / dataTimestamp are backend-mandatory
    # (bean validation on SodaCoreScanStartCommand); version routes v4 handling.
    assert command == {
        "type": "sodaCoreScanStart",
        "scanId": "scan-123",
        "version": "4",
        "definitionName": "my_scan",
        "defaultDataSource": "postgres",
        "dataTimestamp": "2026-07-13T08:30:00+00:00",
    }
    assert mock_execute_command.call_args.kwargs["request_log_name"] == "scan_start"


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_defaults_data_timestamp_to_now(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True, json=lambda: {"scanReference": "org/ref-1"})

    _scan_start(_soda_cloud(), data_timestamp=None)

    command = mock_execute_command.call_args.kwargs["command_json_dict"]
    assert command["dataTimestamp"]  # stamped, never omitted: the backend rejects a null


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_on_rejection(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=False)

    assert _scan_start(_soda_cloud()) is None


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_without_response(mock_execute_command):
    mock_execute_command.return_value = None

    assert _scan_start(_soda_cloud()) is None


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_when_response_lacks_scan_reference(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=True, json=lambda: {})

    assert _scan_start(_soda_cloud()) is None


@patch.object(SodaCloud, "_execute_command")
def test_scan_start_returns_none_on_unparsable_body(mock_execute_command):
    response = MagicMock(ok=True)
    response.json.side_effect = ValueError("not json")
    mock_execute_command.return_value = response

    assert _scan_start(_soda_cloud()) is None


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
    mock_execute_command.return_value = MagicMock(ok=True, status_code=200)

    assert _soda_cloud().scan_end_async("org/ref-1") is True
    command = mock_execute_command.call_args.kwargs["command_json_dict"]
    assert command == {"type": "sodaCoreScanEndAsync", "scanReference": "org/ref-1"}
    assert mock_execute_command.call_args.kwargs["request_log_name"] == "scan_end_async"


@patch.object(SodaCloud, "_execute_command")
def test_scan_end_async_rejected_returns_false(mock_execute_command):
    mock_execute_command.return_value = MagicMock(ok=False, status_code=400)

    assert _soda_cloud().scan_end_async("org/ref-1") is False


@patch.object(SodaCloud, "_execute_command")
def test_scan_end_async_without_response_returns_false(mock_execute_command):
    mock_execute_command.return_value = None

    assert _soda_cloud().scan_end_async("org/ref-1") is False


# The log-batch uploads (batchV3/batchV4): plain REST posts outside the command path, so they carry
# their own once-only 401 re-authentication — a token can expire mid-run on a long scan, and
# without the refresh every subsequent batch would fail the same way.


@patch.object(SodaCloud, "_get_token", side_effect=["expired-token", "fresh-token"])
@patch.object(SodaCloud, "_http_post")
def test_logs_batch_v4_reauthenticates_once_on_a_401(mock_http_post, mock_get_token):
    mock_http_post.side_effect = [MagicMock(status_code=401), MagicMock(status_code=200)]
    soda_cloud = _soda_cloud()

    response = soda_cloud.logs_batch_v4(scan_id="scan-123", body="{}")

    assert response.status_code == 200
    assert mock_http_post.call_count == 2
    # The cached token was cleared before the second attempt, forcing a fresh login.
    assert soda_cloud.token is None
    assert mock_http_post.call_args.kwargs["headers"]["Authorization"] == "fresh-token"


@patch.object(SodaCloud, "_get_token", return_value="expired-token")
@patch.object(SodaCloud, "_http_post", return_value=MagicMock(status_code=401))
def test_logs_batch_v4_gives_up_after_one_reauthentication(mock_http_post, mock_get_token):
    response = _soda_cloud().logs_batch_v4(scan_id="scan-123", body="{}")

    # A 401 that survives a fresh token is a real rejection (revoked key), not an expiry.
    assert response.status_code == 401
    assert mock_http_post.call_count == 2


@patch.object(SodaCloud, "_get_token", return_value="valid-token")
@patch.object(SodaCloud, "_http_post", return_value=MagicMock(status_code=200))
def test_logs_batch_v4_posts_once_when_authenticated(mock_http_post, mock_get_token):
    assert _soda_cloud().logs_batch_v4(scan_id="scan-123", body="{}").status_code == 200
    mock_http_post.assert_called_once()
    assert mock_http_post.call_args.kwargs["url"].endswith("/logs/scan-123/batchV4")
