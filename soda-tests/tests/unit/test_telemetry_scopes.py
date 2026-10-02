"""Telemetry counts of scopes and scoped checks on verify, test and publish.

``SodaTelemetry`` is a process singleton and OpenTelemetry keeps the first global tracer provider, so the span tests
run each command in a child process with telemetry test mode on. The child imports the API module first, which sets
up telemetry, then opens a span, calls the API inside it and prints the attributes the memory exporter recorded.
"""

from __future__ import annotations

import inspect
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import duckdb
import pytest
import soda_core
from soda_core.check_collections.base import CheckCollectionResult
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api import publish_api, test_api
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    ContractVerificationResult,
    ContractVerificationSession,
    ContractVerificationSessionResult,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_yaml import ContractYaml
from soda_core.telemetry.memory_span_exporter import MemorySpanExporter

SCOPED_CONTRACT: str = """\
dataset: telemetry_ds/telemetry/main/telemetry_scopes
scopes:
  eu:
    name: EU rows
    filter: region = 'eu'
  us:
    name: US rows
    filter: region = 'us'
columns:
  - name: id
    checks:
      - missing:
      - missing:
          scope: eu
  - name: region
checks:
  - row_count:
  - row_count:
      scope: eu
  - row_count:
      scope: us
"""

UNSCOPED_CONTRACT: str = """\
dataset: telemetry_ds/telemetry/main/telemetry_scopes
columns:
  - name: id
    checks:
      - missing:
  - name: region
checks:
  - row_count:
"""

# Keys that name no scope a check can run in. The checks that use them still count: 'base' as unscoped, an
# undeclared key as scoped, the way the engine places them.
INVALID_SCOPES_CONTRACT: str = """\
dataset: telemetry_ds/telemetry/main/telemetry_scopes
scopes:
  eu:
    name: EU rows
    filter: region = 'eu'
  base:
    name: Base rows
  7:
    name: Seven
columns:
  - name: id
    checks:
      - missing:
          scope: base
      - missing:
          scope: apac
  - name: region
checks:
  - row_count:
      scope: eu
  - row_count:
"""

# A scope value that is not a string names no declared scope, so its check counts as scoped.
NON_STRING_SCOPES_CONTRACT: str = """\
dataset: telemetry_ds/telemetry/main/telemetry_scopes
scopes:
  eu:
    name: EU rows
    filter: region = 'eu'
columns:
  - name: id
    checks:
      - missing:
          scope: 7
  - name: region
checks:
  - row_count:
      scope: true
  - row_count:
"""

# The engine reads a tagged 'base' as the base scope, so its check counts as unscoped.
TAGGED_BASE_SCOPE_CONTRACT: str = """\
dataset: telemetry_ds/telemetry/main/telemetry_scopes
scopes:
  eu:
    name: EU rows
    filter: region = 'eu'
columns:
  - name: id
    checks:
      - missing:
          scope: !custom base
  - name: region
checks:
  - row_count:
      scope: eu
"""

SPAN_NAME: str = "telemetry_scopes_test"
OUTPUT_PREFIX: str = "TELEMETRY_TEST_OUTPUT "

# Runs one command per contract, each inside its own span, and prints what the memory exporter recorded.
CHILD_SCRIPT: str = """
import inspect
import json
import sys

arguments = json.loads(sys.argv[1])
command = arguments["command"]

if command == "import":
    import soda_core.cli.cli
    import soda_core.contracts.api.test_api
    import soda_core.contracts.api.verify_api

    def run(contract_file_path):
        return None

elif command == "verify":
    from soda_core.contracts.api.verify_api import verify_contract_locally

    def run(contract_file_path):
        return verify_contract_locally(
            data_source_file_path=arguments["data_source_file_path"], contract_file_path=contract_file_path
        )

elif command == "test":
    from soda_core.contracts.api.test_api import test_contract

    def run(contract_file_path):
        return test_contract(contract_file_path=contract_file_path)

else:
    from soda_core.contracts.api.publish_api import publish_contract
    from helpers.mock_soda_cloud import MockHttpMethod, MockResponse, MockSodaCloud
    from soda_core.common.soda_cloud import SodaCloud

    def from_yaml_source(cls, **kwargs):
        return MockSodaCloud(
            [
                MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"allowed": "true"}),
                MockResponse(method=MockHttpMethod.POST, status_code=200, json_object={"fileId": "fake_file_id"}),
                MockResponse(
                    method=MockHttpMethod.POST,
                    json_object={
                        "publishedContract": {"checksum": "check", "fileId": "fake_file_id"},
                        "metadata": {"source": {"filePath": "contract.yml", "type": "local"}},
                    },
                ),
            ]
        )

    SodaCloud.from_yaml_source = classmethod(from_yaml_source)

    def run(contract_file_path):
        return publish_contract(contract_file_path, arguments["soda_cloud_file_path"])


import soda_core
from opentelemetry import trace
from soda_core.telemetry.memory_span_exporter import MemorySpanExporter


def describe(result):
    if result is None:
        return None
    if not hasattr(result, "contract_verification_results"):
        return {"errors": result.logs.get_errors(), "published": len(result.items)}
    from soda_core.cli.handlers.contract import interpret_contract_verification_result

    results = result.contract_verification_results
    return {
        "exit_code": int(interpret_contract_verification_result(result)),
        "errors": result.get_errors(),
        "statuses": [r.status.name for r in results],
        "outcomes": [sorted(check_result.outcome.name for check_result in r.check_results) for r in results],
        "result_counts": [[r.scopes_count, r.scoped_checks_count, r.unscoped_checks_count] for r in results],
        "session_counts": [
            result.number_of_scopes,
            result.number_of_scoped_checks,
            result.number_of_unscoped_checks,
            result.number_of_checks_excluded,
        ],
    }


outputs = []
for contract_file_path in arguments["contract_file_paths"]:
    with trace.get_tracer(__name__).start_as_current_span(arguments["span_name"]):
        result = run(contract_file_path)
    span = MemorySpanExporter.get_instance().spans[-1]
    outputs.append({"span_name": span.name, "attributes": dict(span.attributes), "result": describe(result)})

print(arguments["output_prefix"] + json.dumps({"soda_core_file": inspect.getfile(soda_core), "outputs": outputs}))
"""


def _counts(
    scopes: int,
    scoped: int,
    unscoped: int,
    excluded: int | None = None,
    checks: int | None = None,
    passed: int | None = None,
) -> dict[str, int]:
    attributes: dict[str, int] = {
        "result__scopes_count": scopes,
        "result__scoped_checks_count": scoped,
        "result__unscoped_checks_count": unscoped,
    }
    if checks is not None:
        attributes.update(
            {
                "result__checks_count": checks,
                "result__checks_failed_count": 0,
                "result__checks_passed_count": passed,
                "result__checks_excluded_count": excluded,
            }
        )
    return attributes


# The excluded count on test is 0 because 'soda contract test' builds no check results.
EXPECTED_RESULT_ATTRIBUTES: dict[str, tuple[dict[str, int], dict[str, int]]] = {
    "verify": (
        _counts(scopes=2, scoped=3, unscoped=2, excluded=3, checks=5, passed=2),
        _counts(scopes=0, scoped=0, unscoped=2, excluded=0, checks=2, passed=2),
    ),
    "test": (
        _counts(scopes=2, scoped=3, unscoped=2, excluded=0, checks=0, passed=0),
        _counts(scopes=0, scoped=0, unscoped=2, excluded=0, checks=0, passed=0),
    ),
    "publish": (
        _counts(scopes=2, scoped=3, unscoped=2),
        _counts(scopes=0, scoped=0, unscoped=2),
    ),
}


def _write_files(directory: Path) -> dict[str, str]:
    """The two contracts, a duckdb file the verify command reads, and data source and Soda Cloud files."""
    database_path: Path = directory / "telemetry.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute("CREATE TABLE main.telemetry_scopes (id INTEGER, region VARCHAR)")
        connection.execute("INSERT INTO main.telemetry_scopes VALUES (1, 'eu'), (2, 'us'), (3, 'eu'), (4, 'us')")
    finally:
        connection.close()

    paths: dict[str, Path] = {
        "scoped_contract": directory / "telemetry_scopes.yml",
        "unscoped_contract": directory / "telemetry_unscoped.yml",
        "data_source": directory / "data_source.yml",
        "soda_cloud": directory / "soda_cloud.yml",
    }
    paths["scoped_contract"].write_text(SCOPED_CONTRACT)
    paths["unscoped_contract"].write_text(UNSCOPED_CONTRACT)
    paths["data_source"].write_text(
        f"type: duckdb\nname: telemetry_ds\nconnection:\n  database: {json.dumps(str(database_path))}\n"
    )
    paths["soda_cloud"].write_text(
        "soda_cloud:\n  host: host.soda.io\n  api_key_id: key-id\n  api_key_secret: key-secret\n"
    )
    return {name: str(path) for name, path in paths.items()}


def _run_child(command: str, contract_file_paths: list[str], paths: dict[str, str]) -> dict:
    """Runs ``command`` in a child process with telemetry test mode on and returns what the child printed."""
    environment: dict[str, str] = dict(os.environ)
    environment["SODA_CORE_TELEMETRY_ENABLED"] = "true"
    environment["SODA_CORE_TELEMETRY_TEST_MODE"] = "true"
    # The child imports from the same places as this process.
    environment["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)
    arguments: dict = {
        "command": command,
        "contract_file_paths": contract_file_paths,
        "data_source_file_path": paths.get("data_source"),
        "soda_cloud_file_path": paths.get("soda_cloud"),
        "span_name": SPAN_NAME,
        "output_prefix": OUTPUT_PREFIX,
    }
    completed = subprocess.run(
        [sys.executable, "-c", CHILD_SCRIPT, json.dumps(arguments)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr
    output_lines: list[str] = [line for line in completed.stdout.splitlines() if line.startswith(OUTPUT_PREFIX)]
    assert len(output_lines) == 1, completed.stdout + completed.stderr
    child_output: dict = json.loads(output_lines[0][len(OUTPUT_PREFIX) :])
    assert child_output["soda_core_file"] == inspect.getfile(soda_core)
    return child_output


def _result_attributes(attributes: dict) -> dict:
    return {key: value for key, value in attributes.items() if key.startswith("result__")}


def test_telemetry_test_mode_imports_and_records_spans_in_the_memory_exporter():
    child_output = _run_child(command="import", contract_file_paths=[""], paths={})

    [output] = child_output["outputs"]
    assert output["span_name"] == SPAN_NAME
    assert _result_attributes(output["attributes"]) == {}


@pytest.mark.parametrize("command", ["verify", "test", "publish"])
def test_span_carries_scope_counts(tmp_path, command):
    paths: dict[str, str] = _write_files(tmp_path)

    child_output = _run_child(
        command=command, contract_file_paths=[paths["scoped_contract"], paths["unscoped_contract"]], paths=paths
    )

    scoped_output, unscoped_output = child_output["outputs"]
    expected_scoped, expected_unscoped = EXPECTED_RESULT_ATTRIBUTES[command]
    assert scoped_output["span_name"] == SPAN_NAME
    assert _result_attributes(scoped_output["attributes"]) == expected_scoped
    assert unscoped_output["span_name"] == SPAN_NAME
    assert _result_attributes(unscoped_output["attributes"]) == expected_unscoped

    if command == "publish":
        assert scoped_output["result"] == {"errors": [], "published": 1}
        assert unscoped_output["result"] == {"errors": [], "published": 1}
        return

    scoped_result: dict = scoped_output["result"]
    unscoped_result: dict = unscoped_output["result"]
    assert scoped_result["errors"] == []
    assert unscoped_result["errors"] == []
    assert scoped_result["exit_code"] == 0
    assert unscoped_result["exit_code"] == 0
    assert scoped_result["result_counts"] == [[2, 3, 2]]
    assert unscoped_result["result_counts"] == [[0, 0, 2]]
    if command == "verify":
        # A partial run ends UNKNOWN: the scoped checks are excluded without an extension that runs scopes.
        assert scoped_result["statuses"] == ["UNKNOWN"]
        assert scoped_result["outcomes"] == [["EXCLUDED", "EXCLUDED", "EXCLUDED", "PASSED", "PASSED"]]
        assert scoped_result["session_counts"] == [2, 3, 2, 3]
        assert unscoped_result["statuses"] == ["PASSED"]
        assert unscoped_result["outcomes"] == [["PASSED", "PASSED"]]
        assert unscoped_result["session_counts"] == [0, 0, 2, 0]
    else:
        assert scoped_result["outcomes"] == [[]]
        assert scoped_result["session_counts"] == [2, 3, 2, 0]
        assert unscoped_result["outcomes"] == [[]]
        assert unscoped_result["session_counts"] == [0, 0, 2, 0]


def test_memory_span_exporter_is_one_object():
    assert MemorySpanExporter.get_instance() is MemorySpanExporter.get_instance()
    assert MemorySpanExporter() is MemorySpanExporter.get_instance()


def _make_result(
    result_class: type[CheckCollectionResult] = ContractVerificationResult, **counts: int
) -> CheckCollectionResult:
    now = datetime.now(tz=timezone.utc)
    return result_class(
        check_collection=Contract(
            data_source_name="test_ds",
            dataset_prefix=["s"],
            dataset_name="t",
            soda_qualified_dataset_name="test_ds/s/t",
            source=YamlFileContentInfo(source_content_str=None, local_file_path=None),
        ),
        data_source=None,
        data_timestamp=now,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        **counts,
    )


@dataclass
class _KeywordBuiltResult(CheckCollectionResult):
    """A result subclass in an extension, built by keyword without the scope counts."""


def test_a_result_built_without_the_counts_has_zero_counts():
    result = _make_result(result_class=_KeywordBuiltResult)

    assert (result.scopes_count, result.scoped_checks_count, result.unscoped_checks_count) == (0, 0, 0)


def test_session_result_sums_the_counts_of_its_results():
    session_result = ContractVerificationSessionResult(
        contract_verification_results=[
            _make_result(scopes_count=2, scoped_checks_count=3, unscoped_checks_count=2),
            _make_result(scopes_count=1, scoped_checks_count=1, unscoped_checks_count=4),
        ]
    )

    assert session_result.number_of_scopes == 3
    assert session_result.number_of_scoped_checks == 4
    assert session_result.number_of_unscoped_checks == 6


def _record_attributes(monkeypatch, send: bool = True) -> list[dict]:
    recorded: list[dict] = []
    monkeypatch.setattr(test_api.soda_telemetry, "set_attributes", recorded.append)
    monkeypatch.setattr(test_api.soda_telemetry, "_SodaTelemetry__send", send)
    return recorded


def test_session_ingest_sends_the_scope_and_excluded_counts(monkeypatch):
    recorded = _record_attributes(monkeypatch)
    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(SCOPED_CONTRACT)], only_validate_without_execute=True
    )

    test_api.soda_telemetry.ingest_contract_verification_session_result(
        contract_verification_session_result=session_result
    )

    assert recorded == [_counts(scopes=2, scoped=3, unscoped=2, excluded=0, checks=0, passed=0)]


@pytest.mark.parametrize(
    "contract, expected_counts",
    [
        pytest.param(INVALID_SCOPES_CONTRACT, (1, 2, 2), id="invalid_keys"),
        pytest.param(NON_STRING_SCOPES_CONTRACT, (1, 2, 1), id="non_string_values"),
        pytest.param(TAGGED_BASE_SCOPE_CONTRACT, (1, 1, 1), id="tagged_base"),
    ],
)
def test_publication_and_verification_count_invalid_scopes_alike(monkeypatch, contract, expected_counts):
    """Keys and values that name no scope add no scope, and their checks count the way the engine places them."""
    recorded = _record_attributes(monkeypatch)
    contract_yaml = ContractYaml.parse(yaml_source=ContractYamlSource.from_str(contract))

    test_api.soda_telemetry.ingest_contract_publication([contract_yaml])
    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract)],
        only_validate_without_execute=True,
    )

    scopes, scoped, unscoped = expected_counts
    assert recorded == [_counts(scopes=scopes, scoped=scoped, unscoped=unscoped)]
    [result] = session_result.contract_verification_results
    assert result.status is CheckCollectionStatus.ERROR
    assert (result.scopes_count, result.scoped_checks_count, result.unscoped_checks_count) == expected_counts


def test_publication_counts_a_contract_without_checks(monkeypatch):
    recorded = _record_attributes(monkeypatch)
    contract_yaml = ContractYaml.parse(
        yaml_source=ContractYamlSource.from_str("dataset: ds/db/schema/table\ncolumns:\n  - name: id\n")
    )

    test_api.soda_telemetry.ingest_contract_publication([contract_yaml])

    assert recorded == [_counts(scopes=0, scoped=0, unscoped=0)]


def test_publication_sums_the_counts_of_its_contracts(monkeypatch):
    recorded = _record_attributes(monkeypatch)
    contract_yamls = [
        ContractYaml.parse(yaml_source=ContractYamlSource.from_str(contract))
        for contract in (INVALID_SCOPES_CONTRACT, TAGGED_BASE_SCOPE_CONTRACT)
    ]

    test_api.soda_telemetry.ingest_contract_publication(contract_yamls)

    assert recorded == [_counts(scopes=2, scoped=3, unscoped=3)]


def test_publication_counts_nothing_with_telemetry_off(monkeypatch):
    recorded = _record_attributes(monkeypatch, send=False)
    contract_yaml = ContractYaml.parse(yaml_source=ContractYamlSource.from_str(INVALID_SCOPES_CONTRACT))

    test_api.soda_telemetry.ingest_contract_publication([contract_yaml])

    assert recorded == []


def test_a_failing_count_never_fails_a_publish(monkeypatch):
    publication_result = object()

    class _Publication:
        contract_publication_impl = SimpleNamespace(contract_yamls=[])

        def execute(self):
            return publication_result

    class _Builder:
        def with_contract_yaml_file(self, contract_file_path):
            pass

        def with_soda_cloud_yaml_file(self, soda_cloud_file_path):
            pass

        def build(self):
            return _Publication()

    def _raise(contract_yamls):
        raise RuntimeError("counting failed")

    monkeypatch.setattr(publish_api.ContractPublication, "builder", staticmethod(_Builder))
    monkeypatch.setattr(publish_api.soda_telemetry, "ingest_contract_publication", _raise)

    assert publish_api.publish_contract("contract.yml", "soda-cloud.yml") is publication_result
