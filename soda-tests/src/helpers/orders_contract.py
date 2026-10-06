"""A fixture contract on an in-memory orders table, verified against the mock Soda Cloud.

The contract covers every core check type that runs on DuckDB without a warehouse, the ``query:`` form of the
metric and failed_rows checks, a check that warns, a top-level filter, a check-level filter, check attributes at
both levels with one key set at both, and an empty qualifier. It runs on a private in-memory DuckDB, whatever
``TEST_DATASOURCE`` says, so what it uploads does not depend on the suite's data source or schema name. The
contract comes from a string, which keeps ``contract.metadata.source.filePath`` stable, and the data timestamp is
pinned, which keeps the freshness values stable.

``scoped_contract_yaml()`` is the same contract with two declared scopes and scoped copies of three of its checks.
"""

from __future__ import annotations

import duckdb
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.test_functions import dedent_and_strip
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession, ContractVerificationSessionResult
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

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

# Scoped copies of three checks, indented like the contract. The copy on 'amount' goes after the column's own
# checks and the others after the last check, so the unscoped checks keep their order. Core alone never activates a
# declared scope, so the scoped checks go up as EXCLUDED, with their path, identity, attributes and definition.
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


def scoped_contract_yaml() -> str:
    country_column: str = "      - name: country\n"
    assert CONTRACT_YAML.count(country_column) == 1 and CONTRACT_YAML.endswith("amount > 150\n")
    return CONTRACT_YAML.replace(country_column, SCOPED_AMOUNT_CHECK + country_column) + SCOPED_CHECKS_AND_SCOPES


def orders_data_source() -> DuckDBDataSourceImpl:
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


def verify_session(
    monkeypatch,
    contract_yamls: list[str],
    check_paths: list[str] | None = None,
    check_selectors: list[CheckSelector] | None = None,
) -> tuple[ContractVerificationSessionResult, MockSodaCloud]:
    """Verifies the contracts in one session on the orders data source and publishes to the mock Soda Cloud."""
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
        data_source_impls=[orders_data_source()],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
        data_timestamp=DATA_TIMESTAMP,
        check_paths=check_paths,
        check_selectors=check_selectors,
    )
    return session_result, soda_cloud


def verify_contract(
    monkeypatch,
    contract_yaml: str,
    check_paths: list[str] | None = None,
    check_selectors: list[CheckSelector] | None = None,
) -> tuple[ContractVerificationSessionResult, dict]:
    """The session result and the one uploaded results payload of a single contract."""
    session_result, soda_cloud = verify_session(
        monkeypatch, [contract_yaml], check_paths=check_paths, check_selectors=check_selectors
    )
    payloads: list[dict] = uploaded_payloads(soda_cloud)
    assert len(payloads) == 1
    return session_result, payloads[0]


def uploaded_payloads(soda_cloud: MockSodaCloud) -> list[dict]:
    return [
        request.json
        for request in soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]
