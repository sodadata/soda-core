"""``soda contract verify --use-runner`` with check paths and check filters, from the command line to the command
Soda Cloud receives.

The runner command carries the ``-cp`` paths and the ``-cf`` filters as its ``executionOptions``. A scope key the
contract file does not declare stops the run with exit code 3 before any request to Soda Cloud.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from helpers.mock_soda_cloud import MockHttpMethod, MockResponse, MockSodaCloud
from soda_core.cli.cli import create_cli_parser
from soda_core.cli.exit_codes import ExitCode
from soda_core.common.soda_cloud import SodaCloud

_CONTRACT_YAML: str = """
dataset: test/some/schema/CUSTOMERS
scopes:
  eu:
    name: EU
  us:
    name: US
  apac:
    name: APAC
columns:
- name: id
checks:
- row_count:
- row_count:
    scope: eu
"""

_EU_US_CONTRACT_YAML: str = """
dataset: test/some/schema/CUSTOMERS
scopes:
  eu:
    name: EU
  us:
    name: US
columns:
- name: id
"""


def _completed_on_runner() -> list[MockResponse]:
    return [
        MockResponse(status_code=200, json_object={"allowed": True}),
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": "fffileid"}),
        MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"scanId": "ssscanid"}),
        MockResponse(
            method=MockHttpMethod.GET, status_code=200, json_object={"scanId": "ssscanid", "state": "completed"}
        ),
        MockResponse(
            method=MockHttpMethod.GET,
            status_code=200,
            json_object={
                "content": [],
                "totalElements": 0,
                "totalPages": 1,
                "number": 0,
                "size": 0,
                "last": True,
                "first": True,
            },
        ),
    ]


def _contract_file(tmp_path: Path, contract_yaml: str) -> str:
    contract_file = tmp_path / "customers.yml"
    contract_file.write_text(contract_yaml.lstrip())
    return str(contract_file)


def _soda(argv: list[str], cloud: MockSodaCloud, monkeypatch: pytest.MonkeyPatch) -> int:
    """Runs the command line and returns its exit code. The failure report channel and ``verify_contract`` both
    build their client from the Soda Cloud configuration file, so both get ``cloud``."""
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    monkeypatch.setattr(SodaCloud, "from_config", lambda *args, **kwargs: cloud)
    args = create_cli_parser().parse_args(argv)
    with pytest.raises(SystemExit) as exit_info:
        args.handler_func(args)
    return exit_info.value.code


def _runner_commands(cloud: MockSodaCloud) -> list[dict]:
    return [
        request.json
        for request in cloud.requests
        if isinstance(request.json, dict)
        and request.json.get("type") in ("sodaCoreVerifyContract", "sodaCoreTestContract")
    ]


@pytest.mark.parametrize(
    "publish_flags, command_type", [(["-p"], "sodaCoreVerifyContract"), ([], "sodaCoreTestContract")]
)
def test_verify_on_runner_sends_check_paths_and_check_filters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, publish_flags: list[str], command_type: str
):
    cloud = MockSodaCloud(_completed_on_runner())
    # -cp takes several values in one use, -cf repeats.
    argv = [
        "contract",
        "verify",
        "-c",
        _contract_file(tmp_path, _CONTRACT_YAML),
        "-sc",
        "soda-cloud.yml",
        "-cp",
        "a",
        "b",
        "-cf",
        "scope=eu",
        "-cf",
        "scope=us",
        "-cf",
        "scope!=apac",
        "--use-runner",
        *publish_flags,
    ]

    assert _soda(argv, cloud, monkeypatch) == ExitCode.OK

    [command] = _runner_commands(cloud)
    assert command["type"] == command_type
    assert command["executionOptions"] == {
        "checkPaths": ["a", "b"],
        "checkFilters": [
            {"field": "scope", "values": ["eu", "us"], "negate": False},
            {"field": "scope", "values": ["apac"], "negate": True},
        ],
    }


def test_verify_on_runner_exits_3_on_an_undeclared_scope_key_without_a_cloud_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    # test_soda_cloud.py covers the negated form and the list syntax.
    check_filter = "scope=apac"
    cloud = MockSodaCloud([])
    argv = [
        "contract",
        "verify",
        "-c",
        _contract_file(tmp_path, _EU_US_CONTRACT_YAML),
        "-sc",
        "soda-cloud.yml",
        "-cf",
        check_filter,
        "--use-runner",
        "-p",
    ]

    assert _soda(argv, cloud, monkeypatch) == ExitCode.LOG_ERRORS
    assert cloud.requests == []
