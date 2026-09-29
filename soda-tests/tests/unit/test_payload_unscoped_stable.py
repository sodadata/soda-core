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

A scoped variant of the contract declares two scopes and adds scoped copies of three checks. Its unscoped checks must
upload exactly what the snapshot holds, but for their line numbers, and its scoped checks carry the scope in their
path, identity, attributes and definition.

A file of a kind without scope support declares scopes too and never applies them. Its checks upload the top-level
check attributes and filter whatever their scope, and no scope input fails the file.

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
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.snapshot_updates import UPDATE_SNAPSHOTS_ENV_VAR, updating_snapshots
from helpers.test_functions import dedent_and_strip
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession, ContractVerificationSessionResult
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
    _, soda_cloud = _verify_session(
        monkeypatch, [CONTRACT_YAML], check_paths=check_paths, check_selectors=check_selectors
    )
    payloads: list[dict] = _uploaded_payloads(soda_cloud)
    assert len(payloads) == 1
    return {"requests": _request_sequence(soda_cloud), "payload": _mask_run_varying_values(payloads[0])}


def _verify_contract(
    monkeypatch,
    contract_yaml: str,
    check_paths: list[str] | None = None,
    check_selectors: list[CheckSelector] | None = None,
) -> tuple[ContractVerificationSessionResult, dict]:
    session_result, soda_cloud = _verify_session(
        monkeypatch, [contract_yaml], check_paths=check_paths, check_selectors=check_selectors
    )
    payloads: list[dict] = _uploaded_payloads(soda_cloud)
    assert len(payloads) == 1
    return session_result, payloads[0]


def _verify_session(
    monkeypatch,
    contract_yamls: list[str],
    check_paths: list[str] | None = None,
    check_selectors: list[CheckSelector] | None = None,
) -> tuple[ContractVerificationSessionResult, MockSodaCloud]:
    # The first use of this singleton loads a .env, which may set runner env vars; clear them after it.
    EnvConfigHelper()
    # Runner env vars add or change payload fields.
    for env_var in ("SODA_SCAN_ID", "SODA_INSTRUCTION_ID", "SODA_SCAN_DATA_TIMESTAMP", "SODA_SCAN_DEFINITION"):
        monkeypatch.delenv(env_var, raising=False)

    # The mock answers in request order. Each contract posts its file, which must get a file id back, and then its
    # results, which get the mock's default empty 200.
    soda_cloud = MockSodaCloud(
        [
            response
            for _ in contract_yamls
            for response in (MockResponse(status_code=200, json_object={"fileId": "fixture-file-id"}), None)
        ]
    )
    session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
        contract_yaml_sources=[
            ContractYamlSource.from_str(dedent_and_strip(contract_yaml)) for contract_yaml in contract_yamls
        ],
        data_source_impls=[_orders_data_source()],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
        data_timestamp=DATA_TIMESTAMP,
        check_paths=check_paths,
        check_selectors=check_selectors,
    )
    return session_result, soda_cloud


def _uploaded_payloads(soda_cloud: MockSodaCloud) -> list[dict]:
    return [
        request.json
        for request in soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]


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


# The test contract with two declared scopes and scoped copies of three of its checks, indented like it. The copy on
# 'amount' goes after the column's own checks and the others after the last check, so the unscoped checks keep their
# order. Core alone never activates a declared scope, so the scoped checks go up as EXCLUDED, with their path,
# identity, attributes and definition.
SCOPED_AMOUNT_CHECK = """\
          - invalid:
              scope: eu
              valid_min: 0
              attributes:
                owner: finance
                region: eu-west
"""
SCOPED_CHECKS_AND_SCOPES = """\
      - row_count:
          scope: eu
          qualifier: 2
          threshold:
            must_be_greater_than: 1
      - row_count:
          scope: us
    scopes:
      eu:
        name: EU
        filter: country IN ('BE', 'NL', 'DE')
        check_attributes:
          team: data-eng-eu
          region: eu
      us:
        name: US
"""


def _scoped_contract_yaml() -> str:
    country_column: str = "      - name: country\n"
    assert CONTRACT_YAML.count(country_column) == 1 and CONTRACT_YAML.endswith("amount > 150\n")
    return CONTRACT_YAML.replace(country_column, SCOPED_AMOUNT_CHECK + country_column) + SCOPED_CHECKS_AND_SCOPES


def _without_location(check: dict) -> dict:
    return {key: value for key, value in check.items() if key != "location"}


def _verify_scoped_contract(monkeypatch) -> tuple[ContractVerificationSessionResult, list[dict], list[dict]]:
    """The session result, the uploaded checks with an identity in the snapshot and the other uploaded checks."""
    if updating_snapshots():
        pytest.skip(f"Updates nothing; rerun without {UPDATE_SNAPSHOTS_ENV_VAR}")
    session_result, payload = _verify_contract(monkeypatch, _scoped_contract_yaml())
    snapshot_identities: set[str] = {check["identities"]["vc1"] for check in _snapshot_checks()}
    return (
        session_result,
        [check for check in payload["checks"] if check["identities"]["vc1"] in snapshot_identities],
        [check for check in payload["checks"] if check["identities"]["vc1"] not in snapshot_identities],
    )


def _snapshot_checks() -> list[dict]:
    return json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))["payload"]["checks"]


# Pins the EXCLUDED outcome of core alone, so it drops the soda-scopes extension wherever that is installed.
@pytest.mark.usefixtures("without_scopes_extension")
def test_declared_scopes_leave_the_unscoped_checks_as_in_the_snapshot(monkeypatch):
    session_result, unscoped_checks, scoped_checks = _verify_scoped_contract(monkeypatch)

    # Every check in the snapshot, unchanged but for its line number.
    assert [_without_location(check) for check in unscoped_checks] == [
        _without_location(check) for check in _snapshot_checks()
    ]
    # The scoped copies get identities of their own, distinct from each other too.
    assert len({check["identities"]["vc1"] for check in scoped_checks}) == 3
    assert [(check["outcome"], check["source"]) for check in scoped_checks] == [("excluded", "soda-contract")] * 3
    assert session_result.number_of_checks_excluded == 3


def test_scoped_checks_carry_the_scope_prefix(monkeypatch):
    _, _, scoped_checks = _verify_scoped_contract(monkeypatch)

    assert [check["checkPath"] for check in scoped_checks] == [
        "scope.eu:columns.amount.checks.invalid",
        "scope.eu:checks.row_count.2",
        "scope.us:checks.row_count",
    ]


def test_scoped_checks_carry_the_scope_check_attributes(monkeypatch):
    _, _, scoped_checks = _verify_scoped_contract(monkeypatch)

    # The scope's check attributes under the check's own, never the top-level ones.
    assert [check["resourceAttributes"] for check in scoped_checks] == [
        [
            {"name": "team", "value": "data-eng-eu"},
            {"name": "region", "value": "eu-west"},
            {"name": "owner", "value": "finance"},
        ],
        [{"name": "team", "value": "data-eng-eu"}, {"name": "region", "value": "eu"}],
        [],
    ]


def test_scoped_checks_carry_the_scope_filter_in_their_definition(monkeypatch):
    _, _, scoped_checks = _verify_scoped_contract(monkeypatch)

    # The scope filter in place of the top-level one; a scope without a filter shows none.
    assert [check["definition"] for check in scoped_checks] == [
        "filter: country IN ('BE', 'NL', 'DE')\n"
        "columns:\n"
        "- name: amount\n"
        "  checks:\n"
        "  - scope: eu\n"
        "    valid_min: 0\n"
        "    attributes:\n"
        "      owner: finance\n"
        "      region: eu-west\n",
        "filter: country IN ('BE', 'NL', 'DE')\n"
        "checks:\n"
        "- scope: eu\n"
        "  qualifier: 2\n"
        "  threshold:\n"
        "    must_be_greater_than: 1\n",
        "checks:\n- scope: us\n",
    ]


# A file of a kind without scope support, which reads scope input as written and never applies it. Its checks carry
# the top-level filter and check attributes whatever their scope, as before scopes existed, and its scope input never
# fails the file.
UNSUPPORTED_KIND_YAML = f"""
    kind: {SCOPE_UNSUPPORTED_KIND}
    dataset: fixture_ds/main/orders
    filter: status <> 'cancelled'
    check_attributes:
      team: data-eng
    columns: []
    checks:
      - row_count:
      - row_count:
          scope: eu
          qualifier: declared
      - row_count:
          scope: undeclared
          qualifier: undeclared
    scopes:
      eu:
        name: EU
        filter: country = 'BE'
        check_attributes:
          team: data-eng-eu
          region: eu
"""


def test_kind_without_scope_support_keeps_the_top_level_attributes_and_filter(monkeypatch):
    session_result, payload = _verify_contract(monkeypatch, UNSUPPORTED_KIND_YAML)

    assert session_result.get_errors() == []
    assert [(check["checkPath"], check["outcome"]) for check in payload["checks"]] == [
        ("checks.row_count", "pass"),
        ("checks.row_count.declared", "excluded"),
        ("checks.row_count.undeclared", "excluded"),
    ]
    assert [check["resourceAttributes"] for check in payload["checks"]] == [[{"name": "team", "value": "data-eng"}]] * 3
    assert [check["definition"].split("checks:")[0] for check in payload["checks"]] == [
        "filter: status <> 'cancelled'\n"
    ] * 3


def _unsupported_kind_yaml_with_scope_check_attribute(attribute_line: str) -> str:
    return f"""
        kind: {SCOPE_UNSUPPORTED_KIND}
        dataset: fixture_ds/main/orders
        columns: []
        checks:
          - row_count:
          - row_count:
              scope: eu
              qualifier: scoped
        scopes:
          eu:
            name: EU
            check_attributes:
              {attribute_line}
    """


# Scope check attributes the payload cannot carry: a tagged value, a key that is not a string and a set.
UNSENDABLE_SCOPE_CHECK_ATTRIBUTES = pytest.mark.parametrize(
    "attribute_line",
    ["owner: !custom x", "1: numeric-key", "labels: !!set {a, b}"],
    ids=["tagged-value", "non-string-key", "set-value"],
)


@UNSENDABLE_SCOPE_CHECK_ATTRIBUTES
def test_kind_without_scope_support_never_fails_on_scope_check_attributes(monkeypatch, attribute_line):
    session_result, payload = _verify_contract(
        monkeypatch, _unsupported_kind_yaml_with_scope_check_attribute(attribute_line)
    )

    assert session_result.get_errors() == []
    assert [check["outcome"] for check in payload["checks"]] == ["pass", "excluded"]


@UNSENDABLE_SCOPE_CHECK_ATTRIBUTES
def test_kind_without_scope_support_uploads_next_to_a_contract_despite_scope_check_attributes(
    monkeypatch, attribute_line
):
    contract_yaml: str = """
        dataset: fixture_ds/main/orders
        columns: []
        checks:
          - row_count:
    """
    session_result, soda_cloud = _verify_session(
        monkeypatch, [_unsupported_kind_yaml_with_scope_check_attribute(attribute_line), contract_yaml]
    )
    payloads: list[dict] = _uploaded_payloads(soda_cloud)

    assert session_result.get_errors() == []
    assert [[check["outcome"] for check in payload["checks"]] for payload in payloads] == [
        ["pass", "excluded"],
        ["pass"],
    ]
