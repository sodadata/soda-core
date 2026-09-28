"""Pin the ``sodaCoreInsertScanResults`` payload of an unscoped contract, byte for byte.

Recorded on origin/main ac8c7474, before any scope code existed, so the scope work can prove that an
unscoped contract still uploads exactly what it uploaded before: check paths, identities, definitions,
attributes, diagnostics, key order and the ordered log lines.

The fixture contract covers every core check type that runs on DuckDB without a warehouse, a top-level
filter, a check-level filter, check attributes at both levels and an empty qualifier. It runs on a private
in-memory DuckDB against the mock Soda Cloud, whatever ``TEST_DATASOURCE`` says, so the recording does not
depend on the suite's data source or schema name. The contract comes from a string, which keeps
``contract.metadata.source.filePath`` stable, and the data timestamp is pinned, which keeps the freshness
values stable. Only values that change between runs are masked: the scan id, the scan start and end
timestamps, and each log's timestamp and file path.

To re-record after an intended change, run with ``SODA_TEST_RECORD_FIXTURES=1`` and review the fixture
diff before committing it.
"""

from __future__ import annotations

import copy
import difflib
import json
import logging
import os
from pathlib import Path

import duckdb
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.test_functions import dedent_and_strip
from soda_core.common import logging_configuration
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

RECORD_FIXTURES_ENV_VAR = "SODA_TEST_RECORD_FIXTURES"
FIXTURE_PATH = Path(__file__).parent / "fixtures" / "scan_results_payload_unscoped.json"
MASK = "<masked>"

DATA_TIMESTAMP = "2026-09-28T12:00:00+00:00"

CONTRACT_YAML = """
    dataset: fixture_ds/main/orders
    filter: |
      status <> 'cancelled'
    check_attributes:
      team: data-eng
      priority: 2
    columns:
      - name: id
        data_type: integer
        checks:
          - missing:
          - duplicate:
      - name: customer_id
        checks:
          - missing:
              qualifier: ""
      - name: amount
        checks:
          - invalid:
              valid_min: 0
          - invalid:
              qualifier: strict
              valid_min: 1
              valid_max: 200
              filter: country = 'BE'
          - aggregate:
              function: avg
              threshold:
                must_be_between:
                  greater_than: 0
                  less_than: 1000
      - name: country
        valid_reference_data:
          dataset: fixture_ds/main/countries
          column: code
        checks:
          - invalid:
              attributes:
                owner: geo
      - name: status
      - name: updated_at
    checks:
      - schema:
      - row_count:
      - row_count:
          qualifier: 2
          threshold:
            must_be_greater_than: 1
      - freshness:
          column: updated_at
          threshold:
            unit: hour
            must_be_less_than: 24
      - duplicate:
          columns: [customer_id, country]
      - metric:
          expression: sum(amount) / count(*)
          threshold:
            must_be_greater_than: 0
      - failed_rows:
          expression: amount > 150
"""


def _fixture_data_source() -> DuckDBDataSourceImpl:
    connection = duckdb.connect(":memory:")
    # Naive timestamps are read in the session time zone, which defaults to the machine's.
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute(
        "CREATE TABLE orders (id INTEGER, customer_id VARCHAR, amount INTEGER, country VARCHAR, status VARCHAR, "
        "updated_at TIMESTAMP)"
    )
    connection.execute(
        """
        INSERT INTO orders VALUES
            (1, 'c1', 10, 'BE', 'open', TIMESTAMP '2026-09-28 10:00:00'),
            (2, 'c2', 250, 'NL', 'open', TIMESTAMP '2026-09-28 09:00:00'),
            (3, NULL, -5, 'BE', 'shipped', TIMESTAMP '2026-09-27 12:00:00'),
            (4, 'c2', 40, 'XX', 'shipped', TIMESTAMP '2026-09-28 11:00:00'),
            (4, 'c3', 300, 'BE', 'open', TIMESTAMP '2026-09-26 08:00:00'),
            (5, 'c4', 20, 'DE', 'cancelled', TIMESTAMP '2026-09-20 08:00:00')
        """
    )
    connection.execute("CREATE TABLE countries (code VARCHAR)")
    connection.execute("INSERT INTO countries VALUES ('BE'), ('NL'), ('DE')")
    return DuckDBDataSourceImpl.from_existing_cursor(connection, name="fixture_ds")


def _verify_fixture_contract(monkeypatch, caplog) -> dict:
    # Runner env vars add or change payload fields.
    for env_var in ("SODA_SCAN_ID", "SODA_INSTRUCTION_ID", "SODA_SCAN_DATA_TIMESTAMP"):
        monkeypatch.delenv(env_var, raising=False)
    # The log lines depend on verbose mode and on the root log level, which other tests may change.
    monkeypatch.setattr(logging_configuration, "verbose_mode", True)
    caplog.set_level(logging.DEBUG)
    # The first use of this singleton logs a line; keep it out of the recorded logs.
    EnvConfigHelper()

    soda_cloud = MockSodaCloud([MockResponse(status_code=200, json_object={"fileId": "fixture-file-id"})])
    ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(dedent_and_strip(CONTRACT_YAML))],
        data_source_impls=[_fixture_data_source()],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
        data_timestamp=DATA_TIMESTAMP,
    )
    payloads: list[dict] = [
        request.json
        for request in soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]
    assert len(payloads) == 1
    return payloads[0]


def _mask_run_varying_values(payload: dict) -> dict:
    masked: dict = copy.deepcopy(payload)
    for key in ("scanId", "scanStartTimestamp", "scanEndTimestamp"):
        if key in masked:
            masked[key] = MASK
    for log in masked.get("logs") or []:
        log["timestamp"] = MASK
        location = log.get("location")
        if isinstance(location, dict) and "file_path" in location:
            location["file_path"] = MASK
    return masked


def _to_json_text(payload: dict) -> str:
    # No sort_keys: the key order is part of what the engine emits.
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"


def _load_recording(masked_payload: dict) -> str:
    if os.environ.get(RECORD_FIXTURES_ENV_VAR) == "1":
        FIXTURE_PATH.parent.mkdir(parents=True, exist_ok=True)
        FIXTURE_PATH.write_text(_to_json_text(masked_payload), encoding="utf-8")
        pytest.skip(f"Re-recorded {FIXTURE_PATH.name}; review the diff and rerun without {RECORD_FIXTURES_ENV_VAR}")
    if not FIXTURE_PATH.exists():
        pytest.fail(f"No recording at {FIXTURE_PATH}. Record it with {RECORD_FIXTURES_ENV_VAR}=1.", pytrace=False)
    return FIXTURE_PATH.read_text(encoding="utf-8")


def _assert_lines_match_recording(recorded: list[str], actual: list[str], what: str) -> None:
    if actual != recorded:
        diff: str = "\n".join(
            difflib.unified_diff(recorded, actual, fromfile=f"recorded {what}", tofile=f"actual {what}", lineterm="")
        )
        pytest.fail(
            f"The unscoped {what} changed. If that is intended, re-record with {RECORD_FIXTURES_ENV_VAR}=1.\n{diff}",
            pytrace=False,
        )


def _log_lines(payload: dict) -> list[str]:
    return [f"{log['level']}: {log['message']}" for log in payload.get("logs") or []]


def test_unscoped_log_lines_match_recording(monkeypatch, caplog):
    masked_payload: dict = _mask_run_varying_values(_verify_fixture_contract(monkeypatch, caplog))
    recorded_payload: dict = json.loads(_load_recording(masked_payload))

    assert _log_lines(recorded_payload), "the recording must carry the scan logs"
    _assert_lines_match_recording(_log_lines(recorded_payload), _log_lines(masked_payload), "log lines")


def test_unscoped_payload_matches_recording(monkeypatch, caplog):
    masked_payload: dict = _mask_run_varying_values(_verify_fixture_contract(monkeypatch, caplog))
    recorded_payload_text: str = _load_recording(masked_payload)

    assert masked_payload["defaultDataSourceProperties"] == {"type": "duckdb"}
    _assert_lines_match_recording(
        recorded_payload_text.splitlines(), _to_json_text(masked_payload).splitlines(), "payload"
    )
