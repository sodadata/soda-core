"""The ``scope`` check filter on a real data source.

``scope=<key>`` selects the checks of a declared scope, ``scope=base`` the checks without a scope, several values OR
together and ``scope!=<key>`` excludes. Core never activates a declared scope, so a selected scoped check still goes
up as EXCLUDED; the selection itself is read from the check selectors. A key that no collection in the session
declares fails the run with exit code 3 before any query against the dataset.

Every test here drops the soda-scopes extension, so the file pins core alone even where soda-scopes is installed.
"""

from __future__ import annotations

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.test_functions import dedent_and_strip
from helpers.test_table import TestTable, TestTableSpecification
from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import interpret_contract_verification_result
from soda_core.cli.handlers.scan import run_scan
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import CheckOutcome, ContractVerificationSession
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import CheckCollectionImplExtension

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("scope_selectors")
    .column_integer("id")
    .column_varchar("region")
    .rows(rows=[(1, "eu"), (2, "eu"), (3, "us")])
    .build()
)

# Two declared scopes and base checks. Column checks come first, as in the YAML.
CONTRACT_YAML: str = """
    scopes:
      eu:
        name: EU
        filter: region = 'eu'
      us:
        name: US
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
CHECKS: list[tuple[str, str]] = [
    ("missing", "base"),
    ("missing", "eu"),
    ("row_count", "base"),
    ("row_count", "eu"),
    ("row_count", "us"),
]

_RECORDER_EXTENSION: str = "scope_selectors_recorder"


@pytest.fixture
def built_collections() -> list[CheckCollectionImpl]:
    """Every collection the test builds, of any kind, recorded by a global extension removed at teardown."""
    built: list[CheckCollectionImpl] = []

    class _Recorder(CheckCollectionImplExtension):
        def __init__(self, contract_impl: CheckCollectionImpl):
            built.append(contract_impl)

    CheckCollectionImpl.register_extension(_RECORDER_EXTENSION, _Recorder)
    try:
        yield built
    finally:
        CheckCollectionImpl.impl_extensions.pop(_RECORDER_EXTENSION, None)


def _checks(collection: CheckCollectionImpl, selected_only: bool = False) -> list[tuple[str, str]]:
    return [
        (check_impl.type, check_impl.scope.key)
        for check_impl in collection.all_check_impls
        if not selected_only or CheckSelector.all_match(collection.check_selectors, check_impl)
    ]


def _yaml_source(
    data_source_test_helper: DataSourceTestHelper, test_table: TestTable, yaml_str: str, file_path: str
) -> ContractYamlSource:
    yaml_str = f"dataset: {data_source_test_helper.build_dqn(test_table)}\n{dedent_and_strip(yaml_str)}"
    return ContractYamlSource.from_str(yaml_str=yaml_str, file_path=file_path)


def _capture_sql(data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The SQL of every statement the data source connection runs from here on, until the test ends."""
    captured: list[str] = []
    connection = data_source_test_helper.data_source_impl.data_source_connection
    for method_name in (
        "execute_query",
        "execute_query_one_by_one",
        "execute_query_one_by_one_prefer_streaming",
        "execute_query_iterate",
        "execute_update",
    ):
        original = getattr(connection, method_name)

        def _recording(*args, _original=original, **kwargs):
            captured.append(kwargs["sql"] if "sql" in kwargs else args[0])
            return _original(*args, **kwargs)

        monkeypatch.setattr(connection, method_name, _recording)
    return captured


@pytest.mark.parametrize(
    "check_filters, selected",
    [
        pytest.param([], CHECKS, id="bare"),
        pytest.param(["scope=eu"], [("missing", "eu"), ("row_count", "eu")], id="eu"),
        pytest.param(
            ["scope=eu", "scope=us"], [("missing", "eu"), ("row_count", "eu"), ("row_count", "us")], id="eu-us"
        ),
        pytest.param(["scope=base"], [("missing", "base"), ("row_count", "base")], id="base"),
        pytest.param(["scope!=eu"], [("missing", "base"), ("row_count", "base"), ("row_count", "us")], id="not-eu"),
        pytest.param(["scope=eu", "type=missing"], [("missing", "eu")], id="eu-missing"),
    ],
)
def test_scope_selector_matrix(
    data_source_test_helper: DataSourceTestHelper,
    built_collections: list[CheckCollectionImpl],
    check_filters: list[str],
    selected: list[tuple[str, str]],
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    session_result = data_source_test_helper.verify_contract(
        contract_yaml_str=CONTRACT_YAML,
        test_table=test_table,
        check_selectors=CheckSelector.parse_all(check_filters),
    )

    assert not session_result.has_errors
    [contract_impl] = built_collections
    assert _checks(contract_impl) == CHECKS
    assert _checks(contract_impl, selected_only=True) == selected

    # Only the selected base checks run. Every check is in the result.
    [result] = session_result.contract_verification_results
    expected_outcomes = [
        CheckOutcome.PASSED if check in selected and check[1] == "base" else CheckOutcome.EXCLUDED for check in CHECKS
    ]
    assert [(check_result.check.type, check_result.check.scope or "base") for check_result in result.check_results] == (
        CHECKS
    )
    assert [check_result.outcome for check_result in result.check_results] == expected_outcomes
    assert result.number_of_checks_excluded == expected_outcomes.count(CheckOutcome.EXCLUDED)


@pytest.mark.parametrize("check_filter", ["scope=apac", "scope!=apac"])
def test_an_unknown_scope_key_exits_3_before_any_query(
    data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch, check_filter: str
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    executed_sql: list[str] = _capture_sql(data_source_test_helper, monkeypatch)
    run_logs: list[Logs] = []

    def verify(check_filters: list[str]):
        def command(logs: Logs) -> ExitCode:
            run_logs.append(logs)
            session_result = ContractVerificationSession.execute(
                contract_yaml_sources=[
                    _yaml_source(data_source_test_helper, test_table, CONTRACT_YAML, "scope_selectors.yml")
                ],
                data_source_impls=[data_source_test_helper.data_source_impl],
                check_selectors=CheckSelector.parse_all(check_filters),
                logs=logs,
            )
            return interpret_contract_verification_result(session_result)

        return run_scan(soda_cloud=None, command=command)

    def queries_on_the_dataset() -> list[str]:
        table_name: str = test_table.unique_name.lower()
        return [sql for sql in executed_sql if table_name in sql.lower() or "_soda_filtered_" in sql.lower()]

    # A known key queries the dataset, so the capture sees those queries.
    assert verify(["scope=eu"]) == ExitCode.OK
    assert queries_on_the_dataset()
    executed_sql.clear()

    assert verify([check_filter]) == ExitCode.LOG_ERRORS
    assert "'apac'" in run_logs[-1].get_errors_str()
    assert queries_on_the_dataset() == []
