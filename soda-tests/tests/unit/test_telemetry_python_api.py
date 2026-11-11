"""Python API calls open their own span, one per call, and the span names no dataset, data source or file.

Runs in a child process with telemetry test mode on, like test_telemetry_scopes.py: SodaTelemetry is a process
singleton and OpenTelemetry keeps the first tracer provider.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import duckdb

OUTPUT_PREFIX: str = "TELEMETRY_TEST_OUTPUT "

CHILD_SCRIPT: str = """
import json, sys
paths = json.loads(sys.argv[1])
from opentelemetry import trace
from soda_core.contracts.api import verify_contract_locally
from soda_core.telemetry.memory_span_exporter import MemorySpanExporter

verify_contract_locally(data_source_file_path=paths["data_source"], contract_file_path=paths["contract"])
verify_contract_locally(data_source_file_path=paths["data_source"], contract_file_path=paths["contract"])
try:
    verify_contract_locally(data_source_file_path=paths["missing_data_source"], contract_file_path=paths["contract"])
except Exception as e:
    raised = type(e).__name__
with trace.get_tracer("caller").start_as_current_span("caller_span"):
    verify_contract_locally(data_source_file_path=paths["data_source"], contract_file_path=paths["contract"])

sys.argv = ["soda", "contract", "verify", "-c", paths["contract"], "-ds", paths["data_source"]]
from soda_core.cli.cli import execute
try:
    execute()
except SystemExit:
    pass

spans = MemorySpanExporter.get_instance().spans
print("%s" + json.dumps({"raised": raised, "spans": [json.loads(s.to_json()) for s in spans]}))
"""


def test_each_python_api_call_sends_one_span_without_names(tmp_path):
    database_path = tmp_path / "telemetry.duckdb"
    connection = duckdb.connect(str(database_path))
    connection.execute("CREATE TABLE main.secret_orders (id INTEGER)")
    connection.execute("INSERT INTO main.secret_orders VALUES (1), (2)")
    connection.close()
    paths: dict[str, str] = {
        "contract": str(tmp_path / "contract.yml"),
        "data_source": str(tmp_path / "data_source.yml"),
        "missing_data_source": str(tmp_path / "secret_folder" / "data_source.yml"),
    }
    (tmp_path / "contract.yml").write_text(
        "dataset: secret_ds/telemetry/main/secret_orders\ncolumns:\n  - name: id\nchecks:\n  - row_count:\n"
    )
    (tmp_path / "data_source.yml").write_text(
        f"type: duckdb\nname: secret_ds\nconnection:\n  database: {json.dumps(str(database_path))}\n"
    )

    environment: dict[str, str] = dict(os.environ)
    environment["SODA_CORE_TELEMETRY_ENABLED"] = "true"
    environment["SODA_CORE_TELEMETRY_TEST_MODE"] = "true"
    environment["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)
    completed = subprocess.run(
        [sys.executable, "-c", CHILD_SCRIPT % OUTPUT_PREFIX, json.dumps(paths)],
        env=environment,
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert completed.returncode == 0, completed.stderr
    [line] = [line for line in completed.stdout.splitlines() if line.startswith(OUTPUT_PREFIX)]
    output = json.loads(line[len(OUTPUT_PREFIX) :])
    first, second, failed, in_caller, caller, cli = output["spans"]

    api_span_name = "soda_core.contracts.api.verify_api.verify_contract_locally"
    assert [first["name"], second["name"], failed["name"], in_caller["name"]] == [api_span_name] * 4
    # Separate root spans: a call does not become the child of the call before it.
    assert first["parent_id"] is None and second["parent_id"] is None
    assert first["context"]["trace_id"] != second["context"]["trace_id"]
    assert first["attributes"]["result__checks_count"] == 1
    assert first["attributes"]["result__data_source_type"] == "duckdb"

    assert output["raised"] == "YamlParserException"
    assert failed["status"] == {"status_code": "ERROR", "description": "YamlParserException"}
    assert failed["events"] == []

    # A span the caller has open does not become the parent either.
    assert caller["name"] == "caller_span"
    assert in_caller["parent_id"] is None
    assert in_caller["context"]["trace_id"] != caller["context"]["trace_id"]

    # The CLI keeps one span with the results on it, though verify_contract is traced too.
    assert cli["name"] == "soda_core.cli.cli.execute"
    assert cli["attributes"]["result__checks_count"] == 1
    assert cli["attributes"]["result__data_source_type"] == "duckdb"

    sent: str = json.dumps([first, second, failed, in_caller])
    for name in ("secret_ds", "secret_orders", "secret_folder", str(tmp_path)):
        assert name not in sent
