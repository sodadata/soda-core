"""Contract verification with -d/--dataset against a real Soda Cloud client whose HTTP transport is stubbed.

The fetch is not mocked here: SodaCloud is built from a config file and only its HTTP calls are
stubbed, so the exceptions the client raises for each response, and the failure report a managed
run sends, are exercised end to end.
"""

import json
import sys
from logging import ERROR
from pathlib import Path
from typing import Callable, Optional

import duckdb
import pytest
import requests
from requests import Response
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import interpret_contract_verification_result
from soda_core.common.soda_cloud import SodaCloud
from soda_core.contracts.api.verify_api import verify_contract

DATASET = "my_ds/main/customers"
SCAN_ID = "scan-under-test"

CONTRACT_YAML = f"""dataset: {DATASET}
columns:
  - name: id
    checks:
      - missing:
  - name: name
checks:
  - row_count:
      threshold:
        must_be_greater_than: 1
  - schema:
"""


def _response(status_code: int, body: object, content_type: str = "application/json") -> Response:
    response = Response()
    response.status_code = status_code
    response.headers["Content-Type"] = content_type
    response._content = body.encode() if isinstance(body, str) else json.dumps(body).encode()
    return response


class _Transport:
    """Answers login, the contract query, the failure report and anything else, and records the
    request bodies.

    Set on the class as ``SodaCloud._http_post``: an instance is not a descriptor, so it is
    called without the SodaCloud instance."""

    def __init__(self, get_contract: Callable[[], Response], mark_scan_failed_status: int = 200):
        self.get_contract = get_contract
        self.mark_scan_failed_status = mark_scan_failed_status
        self.requests: list[dict] = []

    def __call__(self, request_log_name: str = None, **kwargs) -> Response:
        body: dict = kwargs.get("json") or {}
        self.requests.append(body)
        request_type: Optional[str] = body.get("type")
        if request_type == "login":
            return _response(200, {"token": "token"})
        if request_type == "sodaCoreGetContract":
            return self.get_contract()
        if request_type == "sodaCoreMarkScanFailed":
            return _response(self.mark_scan_failed_status, {})
        if request_type == "sodaCoreUploadContractFile":
            return _response(200, {"fileId": "file-id"})
        if request_type == "sodaCoreInsertScanResults":
            return _response(200, {"scanId": "scan-id"})
        return _response(200, {})

    @property
    def request_types(self) -> list[Optional[str]]:
        return [body.get("type") for body in self.requests if body.get("type") != "login"]

    def request(self, request_type: str) -> dict:
        [body] = [body for body in self.requests if body.get("type") == request_type]
        return body


@pytest.fixture
def config_files(tmp_path: Path, monkeypatch) -> tuple[str, str]:
    monkeypatch.delenv("SODA_CLOUD_TOKEN", raising=False)
    database = tmp_path / "data.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute("create table customers(id int, name varchar)")
    connection.execute("insert into customers values (1, 'a'), (2, 'b'), (null, 'c')")
    connection.close()

    data_source_file = tmp_path / "ds.yml"
    data_source_file.write_text(
        f'type: duckdb\nname: my_ds\nconnection:\n    database: "{database}"\n    schema: main\n'
    )
    soda_cloud_file = tmp_path / "sc.yml"
    soda_cloud_file.write_text(
        "soda_cloud:\n  host: soda.invalid\n  scheme: http\n  api_key_id: key\n  api_key_secret: secret\n"
    )
    return str(data_source_file), str(soda_cloud_file)


def _run_cli(monkeypatch, *args: str) -> int:
    monkeypatch.setattr(sys, "argv", ["soda", "contract", "verify", *args])
    parsed = create_cli_parser().parse_args()
    with pytest.raises(SystemExit) as exit_info:
        parsed.handler_func(parsed)
    return exit_info.value.code


def _connection_refused() -> Response:
    raise requests.exceptions.ConnectionError("Connection refused")


NO_CONTRACT = "Soda Cloud returned no contract"

# Each way Soda Cloud can fail to hand over the contract, with the reason the error line gives.
REJECTING_RESPONSES = [
    pytest.param(_connection_refused, "no response from Soda Cloud", id="unreachable"),
    pytest.param(
        lambda: _response(500, {"message": "boom"}), "Soda Cloud returned status 500: boom", id="server-error"
    ),
    pytest.param(
        lambda: _response(502, "<html>bad gateway</html>", "text/html"),
        "Soda Cloud returned status 502: <html>bad gateway</html>",
        id="gateway-html",
    ),
    pytest.param(
        lambda: _response(403, {"code": "forbidden", "message": "nope"}),
        "Soda Cloud returned status 403: nope",
        id="forbidden",
    ),
    pytest.param(
        lambda: _response(400, {"code": "contract_not_found"}),
        "the dataset has no published contract in Soda Cloud",
        id="contract-not-found",
    ),
    pytest.param(
        lambda: _response(400, {"code": "dataset_not_found"}),
        "the dataset is unknown in Soda Cloud",
        id="dataset-not-found",
    ),
    pytest.param(
        lambda: _response(400, {"code": "datasource_not_found"}),
        "data source 'my_ds' is unknown in Soda Cloud",
        id="datasource-not-found",
    ),
    pytest.param(lambda: _response(200, {}), NO_CONTRACT, id="no-contents"),
    pytest.param(lambda: _response(200, {"contents": None}), NO_CONTRACT, id="null-contents"),
    pytest.param(lambda: _response(200, {"contents": ""}), NO_CONTRACT, id="empty-contents"),
    pytest.param(lambda: _response(200, {"contents": "\n"}), NO_CONTRACT, id="newline-contents"),
    pytest.param(lambda: _response(200, {"contents": "  \n\t \n"}), NO_CONTRACT, id="whitespace-contents"),
]


def _fetch_failure_line(reason: str) -> str:
    return f"Could not fetch the contract for dataset '{DATASET}': {reason}"


def _error_messages(caplog) -> list[str]:
    return [record.getMessage() for record in caplog.records if record.levelno >= ERROR]


@pytest.mark.parametrize("extra_args", [[], ["-p"], ["-r"]], ids=["local", "publish", "runner"])
@pytest.mark.parametrize("get_contract, reason", REJECTING_RESPONSES)
def test_cli_exits_3_and_sends_nothing_when_cloud_cannot_hand_over_the_contract(
    monkeypatch, caplog, config_files, get_contract, reason, extra_args
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    transport = _Transport(get_contract)
    monkeypatch.setattr(SodaCloud, "_http_post", transport)
    data_source_file, soda_cloud_file = config_files

    exit_code = _run_cli(monkeypatch, "-d", DATASET, "-ds", data_source_file, "-sc", soda_cloud_file, *extra_args)

    assert exit_code == ExitCode.LOG_ERRORS
    # One error line names the dataset, once, as it was typed.
    assert [message for message in _error_messages(caplog) if DATASET in message] == [_fetch_failure_line(reason)]
    assert not any("DatasetIdentifier(" in record.getMessage() for record in caplog.records)
    assert transport.request_types == ["sodaCoreGetContract"]


@pytest.mark.parametrize("extra_args", [[], ["-p"], ["-r"]], ids=["local", "publish", "runner"])
@pytest.mark.parametrize("get_contract, reason", REJECTING_RESPONSES)
def test_cli_on_a_managed_run_marks_the_scan_failed_with_the_error_when_cloud_cannot_hand_over_the_contract(
    monkeypatch, config_files, get_contract, reason, extra_args
):
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    transport = _Transport(get_contract)
    monkeypatch.setattr(SodaCloud, "_http_post", transport)
    data_source_file, soda_cloud_file = config_files

    exit_code = _run_cli(monkeypatch, "-d", DATASET, "-ds", data_source_file, "-sc", soda_cloud_file, *extra_args)

    assert exit_code == ExitCode.LOG_ERRORS
    assert transport.request_types == ["sodaCoreGetContract", "sodaCoreMarkScanFailed"]
    mark = transport.request("sodaCoreMarkScanFailed")
    assert mark["scanId"] == SCAN_ID
    # The report carries the run's logs and the one error line that names the dataset.
    assert [log["message"] for log in mark["logs"] if log["level"] == "error" and DATASET in log["message"]] == [
        _fetch_failure_line(reason)
    ]


def test_cli_on_a_managed_run_exits_4_when_cloud_rejects_the_failure_report(monkeypatch, config_files):
    monkeypatch.setenv("SODA_SCAN_ID", SCAN_ID)
    transport = _Transport(lambda: _response(400, {"code": "contract_not_found"}), mark_scan_failed_status=500)
    monkeypatch.setattr(SodaCloud, "_http_post", transport)
    data_source_file, soda_cloud_file = config_files

    exit_code = _run_cli(monkeypatch, "-d", DATASET, "-ds", data_source_file, "-sc", soda_cloud_file, "-p")

    assert exit_code == ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    assert transport.request_types == ["sodaCoreGetContract", "sodaCoreMarkScanFailed"]


@pytest.mark.parametrize("scan_id", [None, SCAN_ID], ids=["ad-hoc", "managed"])
@pytest.mark.parametrize("get_contract, reason", REJECTING_RESPONSES)
def test_api_reports_the_failed_fetch_in_the_result_without_raising_or_marking(
    monkeypatch, config_files, get_contract, reason, scan_id
):
    if scan_id:
        monkeypatch.setenv("SODA_SCAN_ID", scan_id)
    else:
        monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    transport = _Transport(get_contract)
    monkeypatch.setattr(SodaCloud, "_http_post", transport)
    data_source_file, soda_cloud_file = config_files

    result = verify_contract(
        contract_file_path=None,
        dataset_identifier=DATASET,
        data_source_file_path=data_source_file,
        soda_cloud_file_path=soda_cloud_file,
        publish=True,
    )

    assert result.has_errors
    assert not result.is_ok
    assert result.get_errors() == [_fetch_failure_line(reason)]
    assert transport.request_types == ["sodaCoreGetContract"]


def _check_outcomes(result) -> list[tuple]:
    return [
        (check_result.check.name, check_result.outcome, check_result.diagnostic_metric_values)
        for contract_result in result.contract_verification_results
        for check_result in contract_result.check_results
    ]


def test_fetched_contract_verifies_like_the_same_contract_from_a_file(monkeypatch, tmp_path, config_files):
    transport = _Transport(lambda: _response(200, {"contents": CONTRACT_YAML}))
    monkeypatch.setattr(SodaCloud, "_http_post", transport)
    data_source_file, soda_cloud_file = config_files
    contract_file = tmp_path / "contract.yml"
    contract_file.write_text(CONTRACT_YAML)

    from_file = verify_contract(
        contract_file_path=str(contract_file),
        dataset_identifier=None,
        data_source_file_path=data_source_file,
        soda_cloud_file_path=None,
        publish=False,
    )
    fetched = verify_contract(
        contract_file_path=None,
        dataset_identifier=DATASET,
        data_source_file_path=data_source_file,
        soda_cloud_file_path=soda_cloud_file,
        publish=False,
    )

    assert "sodaCoreGetContract" in transport.request_types
    assert _check_outcomes(fetched) == _check_outcomes(from_file)
    assert len(_check_outcomes(fetched)) == 3
    assert not fetched.has_errors
    assert interpret_contract_verification_result(fetched) == interpret_contract_verification_result(from_file)
    assert interpret_contract_verification_result(fetched) == ExitCode.CHECK_FAILURES


@pytest.mark.parametrize("scan_id", [None, SCAN_ID], ids=["ad-hoc", "managed"])
def test_cli_publishes_a_fetched_contract_without_marking_the_scan(monkeypatch, config_files, scan_id):
    if scan_id:
        monkeypatch.setenv("SODA_SCAN_ID", scan_id)
    else:
        monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    transport = _Transport(lambda: _response(200, {"contents": CONTRACT_YAML}))
    monkeypatch.setattr(SodaCloud, "_http_post", transport)
    data_source_file, soda_cloud_file = config_files

    exit_code = _run_cli(monkeypatch, "-d", DATASET, "-ds", data_source_file, "-sc", soda_cloud_file, "-p")

    assert exit_code == ExitCode.CHECK_FAILURES
    assert "sodaCoreInsertScanResults" in transport.request_types
    assert "sodaCoreMarkScanFailed" not in transport.request_types
