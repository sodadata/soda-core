"""Backpressure between soda-core and Soda Cloud.

A 429 from Soda Cloud is produced before the request did any work, so the same body is safe to
resend after Retry-After. soda-core announces that it can wait with X-Soda-Backpressure.
"""

import copy
import logging
from typing import Optional
from unittest.mock import patch

import duckdb
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import handle_verify_contract
from soda_core.cli.handlers.dependencies import resolve_soda_cloud_for_failure_report
from soda_core.cli.handlers.scan import run_scan
from soda_core.common import soda_cloud as soda_cloud_module
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.soda_cloud import (
    BACKPRESSURE_OPT_IN_HEADER,
    DEFERRAL_BUDGET_ENV_VAR,
    deferral_budget_seconds,
    retry_after_seconds,
)
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession


@pytest.fixture(autouse=True)
def unset_deferral_budget_env_var(monkeypatch):
    """A developer's shell must not change the budget under a test. A bad budget is warned about once per
    value, so the parse cache is cleared too. Otherwise only the first test to hit a value would see the warning."""
    monkeypatch.delenv(DEFERRAL_BUDGET_ENV_VAR, raising=False)
    soda_cloud_module._parse_deferral_budget.cache_clear()


@pytest.fixture
def fake_clock(monkeypatch) -> list[float]:
    """Swaps sleep and monotonic for a clock that only moves when the code sleeps, so a long wait takes
    no real time. Returns the list of waits, in the order they happened."""
    waits: list[float] = []
    now_seconds: float = 1000.0

    def fake_monotonic() -> float:
        return now_seconds

    def fake_sleep(seconds: float) -> None:
        nonlocal now_seconds
        waits.append(seconds)
        now_seconds += seconds

    monkeypatch.setattr(soda_cloud_module, "monotonic", fake_monotonic)
    monkeypatch.setattr(soda_cloud_module, "sleep", fake_sleep)
    return waits


def _busy(retry_after: Optional[str] = "7") -> MockResponse:
    """The answer Soda Cloud gives while it is busy. retry_after=None leaves the Retry-After header out."""
    headers: Optional[dict[str, str]] = None if retry_after is None else {"Retry-After": retry_after}
    return MockResponse(status_code=429, headers=headers, json_object={"code": "too_many_requests"})


def _insert_scan_results_command() -> dict:
    return {"type": "sodaCoreInsertScanResults", "definitionName": "my_scan"}


def _logged(caplog, level: int, containing: str) -> list[str]:
    """The messages logged at exactly this level that contain the given text."""
    return [
        record.getMessage()
        for record in caplog.records
        if record.levelno == level and containing in record.getMessage()
    ]


def test_a_command_request_announces_that_the_client_can_wait():
    mock_cloud = MockSodaCloud()

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert mock_cloud.requests[0].headers[BACKPRESSURE_OPT_IN_HEADER] == "1"


@pytest.mark.parametrize(
    "env_value, expected_budget_seconds, expected_warning_count",
    [
        pytest.param(None, 900.0, 0, id="not set"),
        pytest.param("30", 30.0, 0, id="seconds"),
        pytest.param("0", 0.0, 0, id="zero means do not wait"),
        pytest.param("", 900.0, 0, id="empty, which is how Helm renders an unset value"),
        pytest.param("  ", 900.0, 0, id="blank"),
        pytest.param("soon", 900.0, 1, id="not a number"),
        pytest.param("nan", 900.0, 1, id="nan"),
        pytest.param("inf", 900.0, 1, id="infinite"),
        pytest.param("-5", 900.0, 1, id="negative"),
    ],
)
def test_deferral_budget_is_read_from_the_environment_and_a_bad_value_gets_the_default(
    monkeypatch, caplog, env_value, expected_budget_seconds, expected_warning_count
):
    if env_value is not None:
        monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, env_value)

    assert deferral_budget_seconds() == expected_budget_seconds
    assert len(_logged(caplog, logging.WARNING, DEFERRAL_BUDGET_ENV_VAR)) == expected_warning_count


def test_the_same_bad_deferral_budget_is_warned_about_once_not_on_every_request(monkeypatch, caplog):
    monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, "soon")

    deferral_budget_seconds()
    deferral_budget_seconds()

    warnings = _logged(caplog, logging.WARNING, DEFERRAL_BUDGET_ENV_VAR)
    assert len(warnings) == 1
    assert "'soon'" in warnings[0]


@pytest.mark.parametrize(
    "retry_after, expected_wait_seconds",
    [
        pytest.param("17", 17.0, id="seconds"),
        pytest.param("0", 1.0, id="zero is raised to one second"),
        pytest.param("-5", 1.0, id="negative is raised to one second"),
        pytest.param("1e9", 600.0, id="huge is cut to ten minutes"),
        pytest.param("inf", 15.0, id="infinite gets the default"),
        pytest.param("nan", 15.0, id="nan gets the default"),
        pytest.param("Wed, 21 Oct 2026 07:28:00 GMT", 15.0, id="a date gets the default"),
        pytest.param(None, 15.0, id="missing gets the default"),
    ],
)
def test_retry_after_is_clamped_and_unreadable_values_get_the_default(retry_after, expected_wait_seconds):
    assert retry_after_seconds(_busy(retry_after)) == expected_wait_seconds


class _CloudRecordingBodies(MockSodaCloud):
    def __init__(self, responses):
        super().__init__(responses=responses)
        self.bodies: list[dict] = []

    def _http_handle(self, method, url, headers, json, data):
        # Both attempts share one dict, so a copy is the only way to see what each attempt sent.
        self.bodies.append(copy.deepcopy(json))
        return super()._http_handle(method, url, headers, json, data)


@pytest.mark.parametrize(
    "pick_from_jitter_range, expected_wait_seconds",
    [
        pytest.param(lambda low, high: low, 7.0, id="bottom of the jitter range: no extra wait"),
        pytest.param(lambda low, high: high, 9.1, id="top of the jitter range: 30 percent extra"),
    ],
)
def test_a_429_waits_retry_after_with_jitter_and_resends_the_same_body(
    fake_clock, monkeypatch, pick_from_jitter_range, expected_wait_seconds
):
    monkeypatch.setattr(soda_cloud_module.random, "uniform", pick_from_jitter_range)
    mock_cloud = _CloudRecordingBodies(
        responses=[_busy("7"), MockResponse(status_code=200, json_object={"scanId": "scan-1"})]
    )

    response = mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert response.status_code == 200
    assert len(mock_cloud.requests) == 2
    assert mock_cloud.bodies[0] == mock_cloud.bodies[1]
    assert fake_clock == pytest.approx([expected_wait_seconds])


def test_a_request_that_had_to_wait_logs_each_wait_and_then_the_total_wait_and_the_tries(
    fake_clock, monkeypatch, caplog
):
    caplog.set_level(logging.INFO, logger="soda")
    monkeypatch.setattr(soda_cloud_module.random, "uniform", lambda low, high: high)
    mock_cloud = MockSodaCloud(
        responses=[_busy("7"), _busy("7"), MockResponse(status_code=200, json_object={"scanId": "scan-1"})]
    )

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert _logged(caplog, logging.INFO, "is busy") == ["Soda Cloud is busy, sending send again in 9.1s"] * 2
    assert _logged(caplog, logging.INFO, "accepted") == ["Soda Cloud accepted send after waiting 18.2s (3 tries)"]


def test_a_request_that_did_not_have_to_wait_logs_no_wait_summary(caplog):
    caplog.set_level(logging.INFO, logger="soda")
    mock_cloud = MockSodaCloud()

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert _logged(caplog, logging.INFO, "accepted") == []


def test_a_429_without_retry_after_waits_the_default(fake_clock):
    mock_cloud = MockSodaCloud(responses=[_busy(None), MockResponse(status_code=200, json_object={"scanId": "scan-1"})])

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert len(fake_clock) == 1
    assert 15.0 <= fake_clock[0] <= 15.0 * 1.3


def test_gives_up_with_the_last_429_once_the_budget_is_spent(fake_clock, monkeypatch):
    monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, "0")
    mock_cloud = MockSodaCloud(
        responses=[_busy("7"), MockResponse(status_code=200, json_object={"scanId": "never-reached"})]
    )

    response = mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert response.status_code == 429
    assert len(mock_cloud.requests) == 1
    assert fake_clock == []


def test_waiting_stops_at_the_budget_when_soda_cloud_stays_busy(fake_clock, monkeypatch, caplog):
    monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, "30")
    mock_cloud = MockSodaCloud(responses=[_busy("10") for _ in range(10)])

    response = mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert response.status_code == 429
    assert sum(fake_clock) == pytest.approx(30.0)
    assert len(mock_cloud.requests) == len(fake_clock) + 1
    assert _logged(caplog, logging.ERROR, "still busy") == [
        f"Soda Cloud was still busy after 30s, giving up on send. Set {DEFERRAL_BUDGET_ENV_VAR} to wait longer."
    ]


def test_the_wait_never_exceeds_what_is_left_of_the_budget(fake_clock, monkeypatch):
    monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, "3")
    mock_cloud = MockSodaCloud(responses=[_busy("60"), MockResponse(status_code=200, json_object={"scanId": "scan-1"})])

    mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert fake_clock == pytest.approx([3.0])


def test_a_token_that_expired_during_the_wait_is_renewed_and_the_request_resent(fake_clock):
    mock_cloud = MockSodaCloud(
        responses=[
            _busy("7"),
            MockResponse(status_code=401, json_object={}),
            MockResponse(status_code=200, json_object={"token": "fresh-token"}),
            MockResponse(status_code=200, json_object={"scanId": "scan-1"}),
        ]
    )

    response = mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    assert response.status_code == 200
    assert len(mock_cloud.requests) == 4
    assert mock_cloud.requests[2].json["type"] == "login"
    assert mock_cloud.requests[-1].json["token"] == "fresh-token"
    assert len(fake_clock) == 1


def test_a_re_login_does_not_start_a_new_budget(fake_clock, monkeypatch):
    monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, "10")
    mock_cloud = MockSodaCloud(
        responses=[
            _busy("7"),
            _busy("7"),
            MockResponse(status_code=401, json_object={}),
            MockResponse(status_code=200, json_object={"token": "fresh-token"}),
            _busy("7"),
            _busy("7"),
            MockResponse(status_code=200, json_object={"scanId": "never-reached"}),
        ]
    )

    response = mock_cloud._execute_command(command_json_dict=_insert_scan_results_command(), request_log_name="send")

    # The two waits before the 401 used up the whole budget, so the first 429 after the re-login ends it.
    assert response.status_code == 429
    assert sum(fake_clock) == pytest.approx(10.0)
    assert len(mock_cloud.requests) == 5


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
        # The parent records the request and checks that its body can be serialized, as for any other request.
        response = super()._http_handle(method, url, headers, json, data)
        return _busy("1") if self._is_send_scan_results_request(json) else response


def _scan_results_uploads(mock_cloud: MockSodaCloud) -> list:
    return [request for request in mock_cloud.requests if mock_cloud._is_send_scan_results_request(request.json)]


def test_results_are_marked_not_sent_when_soda_cloud_stays_busy_past_the_budget(fake_clock, monkeypatch):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, "0")
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

    assert len(_scan_results_uploads(mock_cloud)) == 1
    assert session_result.contract_verification_results[0].sending_results_to_soda_cloud_failed is True


def _handle_verify_contract_with_files(tmp_path, mock_cloud: MockSodaCloud, data_source_yaml: str) -> ExitCode:
    """The CLI verify flow, wired as in test_contract_marks_scan_failed_on_connection_error.py: a real session
    and a real duckdb data source, with SodaCloud.from_config returning the given mock, inside run_scan (the
    place where the CLI marks a scan as failed)."""
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(_CONTRACT_YAML)
    data_source_path = tmp_path / "ds.yaml"
    data_source_path.write_text(data_source_yaml)

    with patch("soda_core.common.soda_cloud.SodaCloud.from_config", return_value=mock_cloud):
        soda_cloud = resolve_soda_cloud_for_failure_report("sc.yaml", {})
        return run_scan(
            soda_cloud,
            lambda logs: handle_verify_contract(
                contract_file_path=str(contract_path),
                dataset_identifier=None,
                data_source_file_paths=[str(data_source_path)],
                soda_cloud_file_path="sc.yaml",
                variables={},
                publish=True,
                verbose=False,
                use_runner=False,
                blocking_timeout_in_minutes=10,
                check_paths=None,
                check_selectors=[],
                diagnostics_warehouse_file_path=None,
                logs=logs,
            ),
        )


def test_cli_boundary_deferred_upload_exits_results_not_sent_without_marking_the_scan_failed(
    fake_clock, monkeypatch, tmp_path
):
    # A managed scan: the runner created it, so SODA_SCAN_ID is set. The verification itself succeeds,
    # only the upload of its results is refused by a Soda Cloud that never gets less busy.
    monkeypatch.setenv("SODA_SCAN_ID", "scan-under-test")
    monkeypatch.setenv(DEFERRAL_BUDGET_ENV_VAR, "0")
    db_path = tmp_path / "test.duckdb"
    connection = duckdb.connect(str(db_path))
    connection.execute("CREATE TABLE my_table (id VARCHAR)")
    connection.execute("INSERT INTO my_table VALUES ('a')")
    connection.close()
    data_source_yaml = f'type: duckdb\nname: test_ds\nconnection:\n    database: "{db_path}"\n    schema: main\n'
    mock_cloud = _CloudBusyOnEveryUpload()
    mock_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"

    exit_code = _handle_verify_contract_with_files(tmp_path, mock_cloud, data_source_yaml)

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    assert len(_scan_results_uploads(mock_cloud)) == 1
    request_types = [r.json.get("type") for r in mock_cloud.requests if isinstance(r.json, dict)]
    assert "sodaCoreMarkScanFailed" not in request_types
    assert fake_clock == []
