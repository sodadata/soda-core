"""Pin what Soda Cloud receives for an unscoped contract.

Each snapshot was taken before any scope code existed, so the scope work can prove that an unscoped contract
still sends what it sent before. A snapshot holds two things: the requests sent to Soda Cloud, in order, and the
``sodaCoreInsertScanResults`` payload without its logs. The payload carries each check's path, identities,
definition, attributes, outcome and diagnostics. The logs stay out because they hold debug output and query
text, which change for reasons that have nothing to do with what Soda Cloud stores per check.

The test contract covers every core check type that runs on DuckDB without a warehouse, the ``query:``
form of the metric and failed_rows checks, a check that warns, a top-level filter, a check-level filter,
check attributes at both levels with one key set at both, and an empty qualifier. It runs on a private
in-memory DuckDB against the mock Soda Cloud, whatever ``TEST_DATASOURCE`` says, so the snapshot does
not depend on the suite's data source or schema name. The contract comes from a string, which keeps
``contract.metadata.source.filePath`` stable, and the data timestamp is pinned, which keeps the
freshness values stable. Only values that change between runs are masked: the scan id and the scan start
and end timestamps.

A run filtered by check path and one filtered by a check selector are pinned the same way, each in a
snapshot of its own, so the payload of a partial run, EXCLUDED checks included, stays the same too.

To update the snapshots after an intended change, run with ``SODA_TEST_UPDATE_SNAPSHOTS=1`` and review the
diff before committing it.
"""

from __future__ import annotations

import copy
import difflib
import json
from pathlib import Path
from urllib.parse import urlparse

import duckdb
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.snapshot_updates import UPDATE_SNAPSHOTS_ENV_VAR, updating_snapshots
from helpers.test_functions import dedent_and_strip
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "scan_results_payload_unscoped.json"
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
                priority: 1
      - name: status
      - name: updated_at
    checks:
      - schema:
      - row_count:
      - row_count:
          qualifier: 2
          threshold:
            must_be_greater_than: 1
      - row_count:
          qualifier: warn
          threshold:
            level: warn
            must_be_greater_than: 10
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
      - metric:
          qualifier: query
          query: |
            SELECT AVG(amount) FROM orders WHERE status <> 'cancelled'
          threshold:
            must_be_greater_than: 0
      - failed_rows:
          qualifier: query
          query: |
            SELECT * FROM orders WHERE amount < 0
      - failed_rows:
          expression: amount > 150
"""


def _orders_data_source() -> DuckDBDataSourceImpl:
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


def _verify_unscoped_contract(
    monkeypatch, check_paths: list[str] | None = None, check_selectors: list[CheckSelector] | None = None
) -> dict:
    """The requests sent to Soda Cloud, in order, and the uploaded results payload."""
    # The first use of this singleton loads a .env, which may set runner env vars; clear them after it.
    EnvConfigHelper()
    # Runner env vars add or change payload fields.
    for env_var in ("SODA_SCAN_ID", "SODA_INSTRUCTION_ID", "SODA_SCAN_DATA_TIMESTAMP", "SODA_SCAN_DEFINITION"):
        monkeypatch.delenv(env_var, raising=False)

    soda_cloud = MockSodaCloud([MockResponse(status_code=200, json_object={"fileId": "fixture-file-id"})])
    ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(dedent_and_strip(CONTRACT_YAML))],
        data_source_impls=[_orders_data_source()],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
        data_timestamp=DATA_TIMESTAMP,
        check_paths=check_paths,
        check_selectors=check_selectors,
    )
    payloads: list[dict] = [
        request.json
        for request in soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]
    assert len(payloads) == 1
    return {"requests": _request_sequence(soda_cloud), "payload": _mask_run_varying_values(payloads[0])}


def _request_sequence(soda_cloud: MockSodaCloud) -> list[str]:
    # The path tells the file upload from a command; a command's type tells the commands apart.
    return [
        " ".join(
            part
            for part in (urlparse(request.url).path, (request.json or {}).get("type"))
            if isinstance(part, str) and part
        )
        for request in soda_cloud.requests
    ]


def _mask_run_varying_values(payload: dict) -> dict:
    masked: dict = copy.deepcopy(payload)
    for key in ("scanId", "scanStartTimestamp", "scanEndTimestamp"):
        if key in masked:
            masked[key] = MASK
    masked.pop("logs", None)
    return masked


def _to_json_text(sent: dict) -> str:
    # No sort_keys: the key order is part of what the engine emits.
    return json.dumps(sent, indent=2, ensure_ascii=False) + "\n"


def _load_or_update_snapshot(snapshot_path: Path, sent: dict) -> str:
    if updating_snapshots():
        snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        snapshot_path.write_text(_to_json_text(sent), encoding="utf-8")
        pytest.skip(f"Updated {snapshot_path.name}; review the diff and rerun without {UPDATE_SNAPSHOTS_ENV_VAR}")
    if not snapshot_path.exists():
        pytest.fail(f"No snapshot at {snapshot_path}. Take it with {UPDATE_SNAPSHOTS_ENV_VAR}=1.", pytrace=False)
    return snapshot_path.read_text(encoding="utf-8")


def _assert_matches_snapshot(sent: dict, snapshot_path: Path = SNAPSHOT_PATH) -> None:
    expected: list[str] = _load_or_update_snapshot(snapshot_path, sent).splitlines()
    actual: list[str] = _to_json_text(sent).splitlines()
    if actual != expected:
        diff: str = "\n".join(difflib.unified_diff(expected, actual, fromfile="snapshot", tofile="actual", lineterm=""))
        pytest.fail(
            f"What the unscoped contract sends to Soda Cloud changed. If that is intended, update the snapshot with "
            f"{UPDATE_SNAPSHOTS_ENV_VAR}=1.\n{diff}",
            pytrace=False,
        )


def test_unscoped_contract_sends_what_it_sent_before(monkeypatch):
    sent: dict = _verify_unscoped_contract(monkeypatch)

    assert sent["payload"]["defaultDataSourceProperties"] == {"type": "duckdb"}
    _assert_matches_snapshot(sent)


FILTERED_RUNS = pytest.mark.parametrize(
    "snapshot_name, check_paths, check_selectors",
    [
        (
            "scan_results_payload_unscoped_check_paths.json",
            ["columns.amount.checks.invalid.strict", "checks.metric.query"],
            None,
        ),
        ("scan_results_payload_unscoped_check_selector.json", None, ["type=row_count"]),
    ],
    ids=["check-paths", "check-selector"],
)


@FILTERED_RUNS
def test_filtered_unscoped_contract_sends_what_it_sent_before(monkeypatch, snapshot_name, check_paths, check_selectors):
    sent: dict = _verify_unscoped_contract(
        monkeypatch, check_paths=check_paths, check_selectors=CheckSelector.parse_all(check_selectors)
    )

    assert any(check["outcome"] == "excluded" for check in sent["payload"]["checks"])
    _assert_matches_snapshot(sent, Path(__file__).parent / "snapshots" / snapshot_name)
