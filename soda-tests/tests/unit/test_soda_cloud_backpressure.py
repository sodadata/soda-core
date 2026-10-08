"""Backpressure between soda-core and Soda Cloud (PLATL-1282).

A 429 from Soda Cloud is produced before the request did any work, so the same body is safe to
resend after Retry-After. soda-core announces that it can wait with X-Soda-Backpressure.
"""


from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.common.soda_cloud import (
    BACKPRESSURE_OPT_IN_HEADER,
    DEFAULT_RETRY_AFTER_SECONDS,
    DEFERRAL_BUDGET_SECONDS_DEFAULT,
    deferral_budget_seconds,
    retry_after_seconds,
)


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
