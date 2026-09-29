"""Pin the ``sodaCoreInsertScanResults`` payload of an unscoped contract, byte for byte.

Recorded before any scope code existed, so the scope work can prove that an unscoped contract still
uploads exactly what it uploaded before: check paths, identities, definitions, attributes, diagnostics,
key order and the ordered log lines, which carry the full SQL of each query.

The fixture contract covers every core check type that runs on DuckDB without a warehouse, the ``query:``
form of the metric and failed_rows checks, a check that warns, a top-level filter, a check-level filter,
check attributes at both levels with one key set at both, and an empty qualifier. It runs on a private
in-memory DuckDB against the mock Soda Cloud, whatever ``TEST_DATASOURCE`` says, so the recording does
not depend on the suite's data source or schema name. The contract comes from a string, which keeps
``contract.metadata.source.filePath`` stable, and the data timestamp is pinned, which keeps the
freshness values stable. Only values that change between runs are masked: the scan id, the scan start
and end timestamps, and each log's timestamp.

The payload carries no metric ids, so the ids of the contract's resolved metrics are pinned next to it in
``fixtures/metric_ids_unscoped.json``, in resolution order.

A run filtered by check path and one filtered by a check selector are pinned the same way, each in a
recording of its own, so the payload of a partial run, EXCLUDED checks included, stays the same too.

A scoped variant of the contract declares two scopes and adds scoped copies of three checks. Its unscoped checks must
upload exactly what the recording holds, but for their line numbers, and its scoped checks carry the scope in their
path, identity, attributes and definition.

A file of a kind without scope support declares scopes too and never applies them. Its checks upload the top-level
check attributes and filter whatever their scope, and no scope input fails the file.

To re-record after an intended change, run with ``SODA_TEST_RECORD_FIXTURES=1`` and review the fixture
diff before committing it.
"""

from __future__ import annotations

import copy
import difflib
import json
import logging
from pathlib import Path

import duckdb
import pytest
from helpers.fixture_recording import RECORD_FIXTURES_ENV_VAR, recording_fixtures
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND
from helpers.test_functions import dedent_and_strip
from soda_core.check_collections.base import CheckCollectionImpl, CheckCollectionResult
from soda_core.common import env_config_helper, logging_configuration
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession, ContractVerificationSessionResult
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

FIXTURE_PATH = Path(__file__).parent / "fixtures" / "scan_results_payload_unscoped.json"
METRIC_IDS_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "metric_ids_unscoped.json"
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


def _fixture_data_source() -> DuckDBDataSourceImpl:
    connection = duckdb.connect(":memory:")
    # Naive timestamps are read in the session time zone, which defaults to the machine's.
    connection.execute("SET TimeZone = 'UTC'")
    connection.execute(
        "CREATE TABLE orders (id INTEGER, customer_id VARCHAR, amount INTEGER, country VARCHAR, status VARCHAR, "
        "updated_at TIMESTAMP)"
    )
    connection.execute("""
        INSERT INTO orders VALUES
            (1, 'c1', 10, 'BE', 'open', TIMESTAMP '2026-09-28 10:00:00'),
            (2, 'c2', 250, 'NL', 'open', TIMESTAMP '2026-09-28 09:00:00'),
            (3, NULL, -5, 'BE', 'shipped', TIMESTAMP '2026-09-27 12:00:00'),
            (4, 'c2', 40, 'XX', 'shipped', TIMESTAMP '2026-09-28 11:00:00'),
            (4, 'c3', 300, 'BE', 'open', TIMESTAMP '2026-09-26 08:00:00'),
            (5, 'c4', 20, 'DE', 'cancelled', TIMESTAMP '2026-09-20 08:00:00')
        """)
    connection.execute("CREATE TABLE countries (code VARCHAR)")
    connection.execute("INSERT INTO countries VALUES ('BE'), ('NL'), ('DE')")
    return DuckDBDataSourceImpl.from_existing_cursor(connection, name="fixture_ds")


def _verify_fixture_contract(
    monkeypatch, caplog, check_paths: list[str] | None = None, check_selectors: list[CheckSelector] | None = None
) -> dict:
    _, payload = _verify_contract(
        monkeypatch, caplog, CONTRACT_YAML, check_paths=check_paths, check_selectors=check_selectors
    )
    return payload


def _verify_contract(
    monkeypatch,
    caplog,
    contract_yaml: str,
    check_paths: list[str] | None = None,
    check_selectors: list[CheckSelector] | None = None,
) -> tuple[ContractVerificationSessionResult, dict]:
    session_result, payloads = _verify_session(
        monkeypatch, caplog, [contract_yaml], check_paths=check_paths, check_selectors=check_selectors
    )
    assert len(payloads) == 1
    return session_result, payloads[0]


def _verify_session(
    monkeypatch,
    caplog,
    contract_yamls: list[str],
    check_paths: list[str] | None = None,
    check_selectors: list[CheckSelector] | None = None,
) -> tuple[ContractVerificationSessionResult, list[dict]]:
    # The first use of this singleton logs a line and loads a .env, which may set runner env vars; keep the
    # line out of the recorded logs and clear the env vars after it.
    EnvConfigHelper()
    # Runner env vars add or change payload fields.
    for env_var in ("SODA_SCAN_ID", "SODA_INSTRUCTION_ID", "SODA_SCAN_DATA_TIMESTAMP", "SODA_SCAN_DEFINITION"):
        monkeypatch.delenv(env_var, raising=False)
    # The log lines depend on verbose mode and on the root log level, which other tests may change.
    monkeypatch.setattr(logging_configuration, "verbose_mode", True)
    # The debug print limits come from SODA_DEBUG_PRINT_* env vars at import; pin the defaults, except that by
    # default the SQL debug line stops at 1024 chars, which would leave the end of the metrics query unpinned.
    monkeypatch.setattr(DataSourceConnection, "MAX_CHARS_PER_STRING", 256)
    monkeypatch.setattr(DataSourceConnection, "MAX_ROWS", 20)
    monkeypatch.setattr(DataSourceConnection, "MAX_CHARS_PER_SQL", 100_000)
    caplog.set_level(logging.DEBUG)

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
        data_source_impls=[_fixture_data_source()],
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
    return session_result, payloads


def _mask_run_varying_values(payload: dict) -> dict:
    masked: dict = copy.deepcopy(payload)
    for key in ("scanId", "scanStartTimestamp", "scanEndTimestamp"):
        if key in masked:
            masked[key] = MASK
    for log in masked.get("logs") or []:
        log["timestamp"] = MASK
    return masked


def _to_json_text(payload: dict) -> str:
    # No sort_keys: the key order is part of what the engine emits.
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"


def _load_recording(masked_payload: dict, fixture_path: Path = FIXTURE_PATH) -> str:
    return _load_or_record(fixture_path, _to_json_text(masked_payload))


def _load_or_record(fixture_path: Path, actual_text: str) -> str:
    if recording_fixtures():
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_path.write_text(actual_text, encoding="utf-8")
        pytest.skip(f"Re-recorded {fixture_path.name}; review the diff and rerun without {RECORD_FIXTURES_ENV_VAR}")
    if not fixture_path.exists():
        pytest.fail(f"No recording at {fixture_path}. Record it with {RECORD_FIXTURES_ENV_VAR}=1.", pytrace=False)
    return fixture_path.read_text(encoding="utf-8")


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


def _verified_check_collection_impl(monkeypatch, caplog) -> CheckCollectionImpl:
    verified: list[CheckCollectionImpl] = []
    original_verify = CheckCollectionImpl.verify

    def recording_verify(self) -> CheckCollectionResult:
        verified.append(self)
        return original_verify(self)

    monkeypatch.setattr(CheckCollectionImpl, "verify", recording_verify)
    _verify_fixture_contract(monkeypatch, caplog)
    assert len(verified) == 1
    return verified[0]


def _metric_ids(check_collection_impl: CheckCollectionImpl) -> dict:
    # Everything but the id only labels the row, so a changed id shows up in the diff next to its metric.
    return {
        "metrics": [
            {
                "metric": type(metric_impl).__name__,
                "type": metric_impl.type,
                "column": metric_impl.column_impl.column_yaml.name if metric_impl.column_impl else None,
                "filter": metric_impl.check_filter,
                "id": metric_impl.id,
            }
            for metric_impl in check_collection_impl.metrics_resolver.get_resolved_metrics()
        ]
    }


def test_unscoped_metric_ids_match_recording(monkeypatch, caplog):
    check_collection_impl: CheckCollectionImpl = _verified_check_collection_impl(monkeypatch, caplog)
    metric_ids_text: str = _to_json_text(_metric_ids(check_collection_impl))
    recorded_metric_ids_text: str = _load_or_record(METRIC_IDS_FIXTURE_PATH, metric_ids_text)

    _assert_lines_match_recording(recorded_metric_ids_text.splitlines(), metric_ids_text.splitlines(), "metric ids")


def _assert_payload_matches_recording(masked_payload: dict) -> None:
    if recording_fixtures():
        pytest.skip(f"Records nothing; rerun without {RECORD_FIXTURES_ENV_VAR}")
    _assert_lines_match_recording(
        FIXTURE_PATH.read_text(encoding="utf-8").splitlines(), _to_json_text(masked_payload).splitlines(), "payload"
    )


@pytest.mark.parametrize(
    "limit, value",
    [("MAX_CHARS_PER_STRING", 5), ("MAX_ROWS", 1), ("MAX_CHARS_PER_SQL", 50)],
)
def test_debug_print_limits_from_the_environment_do_not_change_the_payload(monkeypatch, caplog, limit, value):
    # DataSourceConnection reads each limit from a SODA_DEBUG_PRINT_* env var when it is imported, so a limit
    # set in the environment has the same effect as this patch.
    monkeypatch.setattr(DataSourceConnection, limit, value)
    _assert_payload_matches_recording(_mask_run_varying_values(_verify_fixture_contract(monkeypatch, caplog)))


def test_runner_env_vars_in_a_dotenv_file_do_not_change_the_payload(monkeypatch, caplog):
    # Stands in for a .env that sets a runner env var; through monkeypatch, so the test leaves it unset.
    monkeypatch.setattr(
        env_config_helper, "load_dotenv", lambda override=False: monkeypatch.setenv("SODA_SCAN_ID", "scan-from-dotenv")
    )
    # A fresh singleton loads the .env on first use, as in a new process; the old one comes back afterwards.
    monkeypatch.setattr(EnvConfigHelper, "_EnvConfigHelper__instance", None)
    _assert_payload_matches_recording(_mask_run_varying_values(_verify_fixture_contract(monkeypatch, caplog)))


FILTERED_RUNS = pytest.mark.parametrize(
    "fixture_name, check_paths, check_selectors",
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
def test_unscoped_filtered_payload_matches_recording(monkeypatch, caplog, fixture_name, check_paths, check_selectors):
    masked_payload: dict = _mask_run_varying_values(
        _verify_fixture_contract(
            monkeypatch, caplog, check_paths=check_paths, check_selectors=CheckSelector.parse_all(check_selectors)
        )
    )
    recorded_payload_text: str = _load_recording(masked_payload, Path(__file__).parent / "fixtures" / fixture_name)

    assert any(check["outcome"] == "excluded" for check in masked_payload["checks"])
    _assert_lines_match_recording(
        recorded_payload_text.splitlines(), _to_json_text(masked_payload).splitlines(), "filtered payload"
    )


# The fixture contract with two declared scopes and scoped copies of three of its checks, indented like it. The copy on
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


def _verify_scoped_contract(monkeypatch, caplog) -> tuple[ContractVerificationSessionResult, list[dict], list[dict]]:
    """The session result, the uploaded checks with a recorded identity and the other uploaded checks."""
    if recording_fixtures():
        pytest.skip(f"Records nothing; rerun without {RECORD_FIXTURES_ENV_VAR}")
    session_result, payload = _verify_contract(monkeypatch, caplog, _scoped_contract_yaml())
    recorded_identities: set[str] = {check["identities"]["vc1"] for check in _recorded_checks()}
    return (
        session_result,
        [check for check in payload["checks"] if check["identities"]["vc1"] in recorded_identities],
        [check for check in payload["checks"] if check["identities"]["vc1"] not in recorded_identities],
    )


def _recorded_checks() -> list[dict]:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))["checks"]


def test_declared_scopes_leave_the_unscoped_checks_as_recorded(monkeypatch, caplog):
    session_result, unscoped_checks, scoped_checks = _verify_scoped_contract(monkeypatch, caplog)

    # Every recorded check, unchanged but for its line number.
    assert [_without_location(check) for check in unscoped_checks] == [
        _without_location(check) for check in _recorded_checks()
    ]
    # The scoped copies get identities of their own, distinct from each other too.
    assert len({check["identities"]["vc1"] for check in scoped_checks}) == 3
    assert [(check["outcome"], check["source"]) for check in scoped_checks] == [("excluded", "soda-contract")] * 3
    assert session_result.number_of_checks_excluded == 3


def test_scoped_checks_carry_the_scope_prefix(monkeypatch, caplog):
    _, _, scoped_checks = _verify_scoped_contract(monkeypatch, caplog)

    assert [check["checkPath"] for check in scoped_checks] == [
        "scope.eu:columns.amount.checks.invalid",
        "scope.eu:checks.row_count.2",
        "scope.us:checks.row_count",
    ]


def test_scoped_checks_carry_the_scope_check_attributes(monkeypatch, caplog):
    _, _, scoped_checks = _verify_scoped_contract(monkeypatch, caplog)

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


def test_scoped_checks_carry_the_scope_filter_in_their_definition(monkeypatch, caplog):
    _, _, scoped_checks = _verify_scoped_contract(monkeypatch, caplog)

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


def test_kind_without_scope_support_keeps_the_top_level_attributes_and_filter(monkeypatch, caplog):
    session_result, payload = _verify_contract(monkeypatch, caplog, UNSUPPORTED_KIND_YAML)

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
def test_kind_without_scope_support_never_fails_on_scope_check_attributes(monkeypatch, caplog, attribute_line):
    session_result, payload = _verify_contract(
        monkeypatch, caplog, _unsupported_kind_yaml_with_scope_check_attribute(attribute_line)
    )

    assert session_result.get_errors() == []
    assert [check["outcome"] for check in payload["checks"]] == ["pass", "excluded"]


@UNSENDABLE_SCOPE_CHECK_ATTRIBUTES
def test_kind_without_scope_support_uploads_next_to_a_contract_despite_scope_check_attributes(
    monkeypatch, caplog, attribute_line
):
    contract_yaml: str = """
        dataset: fixture_ds/main/orders
        columns: []
        checks:
          - row_count:
    """
    session_result, payloads = _verify_session(
        monkeypatch, caplog, [_unsupported_kind_yaml_with_scope_check_attribute(attribute_line), contract_yaml]
    )

    assert session_result.get_errors() == []
    assert [[check["outcome"] for check in payload["checks"]] for payload in payloads] == [
        ["pass", "excluded"],
        ["pass"],
    ]
