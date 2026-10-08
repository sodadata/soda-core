"""Backpressure between soda-core and Soda Cloud (PLATL-1282).

A 429 from Soda Cloud is produced before the request did any work, so the same body is safe to
resend after Retry-After. soda-core announces that it can wait with X-Soda-Backpressure.
"""

from unittest.mock import patch

from helpers.mock_soda_cloud import MockRequest, MockResponse, MockSodaCloud
from soda_core.common import soda_cloud as soda_cloud_module
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.soda_cloud import (
    BACKPRESSURE_OPT_IN_HEADER,
    DEFAULT_RETRY_AFTER_SECONDS,
    DEFERRAL_BUDGET_SECONDS_DEFAULT,
    deferral_budget_seconds,
    retry_after_seconds,
)
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession


def _insert_scan_results_command() -> dict:
    return {"type": "sodaCoreInsertScanResults", "definitionName": "my_scan"}


def test_every_request_announces_that_the_client_can_wait():
    mock_cloud = MockSodaCloud()

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert mock_cloud.requests[0].headers[BACKPRESSURE_OPT_IN_HEADER] == "1"


def test_deferral_budget_defaults_to_fifteen_minutes(monkeypatch):
    monkeypatch.delenv("SODA_CLOUD_DEFERRAL_BUDGET_SECONDS", raising=False)
    assert deferral_budget_seconds() == DEFERRAL_BUDGET_SECONDS_DEFAULT == 900


def test_deferral_budget_can_be_overridden_and_ignores_garbage(monkeypatch):
    monkeypatch.setenv("SODA_CLOUD_DEFERRAL_BUDGET_SECONDS", "30")
    assert deferral_budget_seconds() == 30.0
    monkeypatch.setenv("SODA_CLOUD_DEFERRAL_BUDGET_SECONDS", "soon")
    assert deferral_budget_seconds() == DEFERRAL_BUDGET_SECONDS_DEFAULT


def test_retry_after_reads_delta_seconds_and_falls_back_to_the_default():
    assert retry_after_seconds(MockResponse(status_code=429, headers={"Retry-After": "17"}, json_object={})) == 17.0
    assert retry_after_seconds(MockResponse(status_code=429, json_object={})) == DEFAULT_RETRY_AFTER_SECONDS
    assert (
        retry_after_seconds(
            MockResponse(status_code=429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}, json_object={})
        )
        == DEFAULT_RETRY_AFTER_SECONDS
    )


@patch.object(soda_cloud_module, "sleep")
def test_a_429_waits_retry_after_with_jitter_and_resends_the_same_body(sleep_mock):
    mock_cloud = MockSodaCloud(
        responses=[
            MockResponse(status_code=429, headers={"Retry-After": "7"}, json_object={"code": "too_many_requests"}),
            MockResponse(status_code=200, json_object={"scanId": "scan-1"}),
        ]
    )

    response = mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert response.status_code == 200
    assert len(mock_cloud.requests) == 2
    assert mock_cloud.requests[0].json == mock_cloud.requests[1].json
    (waited,), _ = sleep_mock.call_args
    assert 7.0 <= waited <= 7.0 * 1.3


@patch.object(soda_cloud_module, "sleep")
def test_a_429_without_retry_after_waits_the_default(sleep_mock):
    mock_cloud = MockSodaCloud(
        responses=[
            MockResponse(status_code=429, json_object={"code": "too_many_requests"}),
            MockResponse(status_code=200, json_object={"scanId": "scan-1"}),
        ]
    )

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    (waited,), _ = sleep_mock.call_args
    assert 15.0 <= waited <= 15.0 * 1.3


@patch.object(soda_cloud_module, "sleep")
def test_gives_up_with_the_last_429_once_the_budget_is_spent(sleep_mock, monkeypatch):
    monkeypatch.setenv("SODA_CLOUD_DEFERRAL_BUDGET_SECONDS", "0")
    mock_cloud = MockSodaCloud(
        responses=[
            MockResponse(status_code=429, headers={"Retry-After": "7"}, json_object={"code": "too_many_requests"}),
            MockResponse(status_code=200, json_object={"scanId": "never-reached"}),
        ]
    )

    response = mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert response.status_code == 429
    assert len(mock_cloud.requests) == 1
    sleep_mock.assert_not_called()


@patch.object(soda_cloud_module, "sleep")
def test_the_wait_never_exceeds_what_is_left_of_the_budget(sleep_mock, monkeypatch):
    monkeypatch.setenv("SODA_CLOUD_DEFERRAL_BUDGET_SECONDS", "3")
    mock_cloud = MockSodaCloud(
        responses=[
            MockResponse(status_code=429, headers={"Retry-After": "60"}, json_object={"code": "too_many_requests"}),
            MockResponse(status_code=200, json_object={"scanId": "scan-1"}),
        ]
    )

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    (waited,), _ = sleep_mock.call_args
    assert waited <= 3.0


_DATA_SOURCE_YAML = """
type: duckdb
name: test_ds
connection:
    database: ":memory:"
    schema: main
"""

_CONTRACT_YAML = """
dataset: test_ds/main/my_table
columns:
  - name: id
"""


class _CloudBusyOnEveryUpload(MockSodaCloud):
    def _http_handle(self, method, url, headers, json, data):
        if isinstance(json, dict) and json.get("type") == "sodaCoreInsertScanResults":
            self.requests.append(MockRequest(url=url, headers=headers, json=json, data=data))
            return MockResponse(
                status_code=429, headers={"Retry-After": "1"}, json_object={"code": "too_many_requests"}
            )
        return super()._http_handle(method, url, headers, json, data)


@patch.object(soda_cloud_module, "sleep")
def test_results_are_marked_not_sent_when_soda_cloud_stays_busy_past_the_budget(sleep_mock, monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    monkeypatch.setenv("SODA_CLOUD_DEFERRAL_BUDGET_SECONDS", "0")
    data_source_impl = DataSourceImpl.from_yaml_source(DataSourceYamlSource.from_str(_DATA_SOURCE_YAML))
    mock_cloud = _CloudBusyOnEveryUpload()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    with patch(
        "soda_duckdb.common.data_sources.duckdb_data_source.DuckDBDataSourceConnection._create_connection",
        side_effect=RuntimeError("Invalid access token"),
    ):
        session_result = ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(_CONTRACT_YAML)],
            data_source_impls=[data_source_impl],
            soda_cloud_impl=mock_cloud,
            soda_cloud_publish_results=True,
        )

    uploads = [
        r for r in mock_cloud.requests if isinstance(r.json, dict) and r.json.get("type") == "sodaCoreInsertScanResults"
    ]
    assert len(uploads) == 1
    assert session_result.contract_verification_results[0].sending_results_to_soda_cloud_failed is True
