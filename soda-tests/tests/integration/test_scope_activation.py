"""Declared scopes activated by an extension run over their own filtered CTE.

``helpers.scope_activation_extension`` stands in for the module that runs declared scopes. Without it a declared
scope stays inactive and core builds no SQL for it. Scoped and unscoped checks on the same column carry different
qualifiers, so their check paths stay distinct.

Every test here drops the soda-scopes extension, so only the stand-in activates scopes even with soda-scopes installed.
"""

from __future__ import annotations

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.scope_activation_extension import scope_activation
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import CheckOutcome, ContractVerificationResult

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("scope_activation")
    .column_integer("id")
    .column_varchar("region")
    .column_varchar("email")
    .rows(
        rows=[
            (1, "eu", None),
            (2, "eu", "a"),
            (3, "us", None),
            (4, "us", None),
            (5, "us", "b"),
        ]
    )
    .build()
)

reference_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("scope_activation_reference")
    .column_varchar("email")
    .rows(rows=[("a",)])
    .build()
)


def _capture_executed_sql(data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    captured: list[str] = []
    connection = data_source_test_helper.data_source_impl.data_source_connection
    original = connection.execute_query

    def _wrapped(sql: str, log_query: bool = True):
        captured.append(sql)
        return original(sql, log_query)

    monkeypatch.setattr(connection, "execute_query", _wrapped)
    return captured


def _scopes_yaml(data_source_test_helper: DataSourceTestHelper) -> str:
    region = data_source_test_helper.quote_column("region")
    return f"""
            scopes:
              eu:
                name: EU
                filter: |
                  {region} = 'eu'
              us:
                name: US
                filter: |
                  {region} = 'us'
    """


def _verify(
    data_source_test_helper: DataSourceTestHelper, test_table, contract_yaml_str: str
) -> ContractVerificationResult:
    session_result = data_source_test_helper.verify_contract(test_table=test_table, contract_yaml_str=contract_yaml_str)
    assert not session_result.has_errors, session_result.get_errors_str()
    [result] = session_result.contract_verification_results
    return result


def _results_by_qualifier(result: ContractVerificationResult) -> dict:
    return {check_result.check.qualifier: check_result for check_result in result.check_results}


def test_a_scoped_check_aggregates_over_its_scope(
    data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    captured_sql = _capture_executed_sql(data_source_test_helper, monkeypatch)

    with scope_activation("eu"):
        result = _verify(
            data_source_test_helper,
            test_table,
            _scopes_yaml(data_source_test_helper)
            + """
            columns:
              - name: email
                checks:
                  - missing:
                      qualifier: all
                      threshold:
                        must_be_less_than: 5
                  - missing:
                      qualifier: eu
                      scope: eu
                      threshold:
                        must_be_less_than: 5
                  - missing:
                      qualifier: us
                      scope: us
            """,
        )

    results = _results_by_qualifier(result)
    assert results["all"].outcome == CheckOutcome.PASSED
    assert results["all"].diagnostic_metric_values == {
        "missing_count": 3,
        "missing_percent": 60.0,
        "check_rows_tested": 5,
        "dataset_rows_tested": 5,
    }
    assert results["eu"].outcome == CheckOutcome.PASSED
    assert results["eu"].diagnostic_metric_values == {
        "missing_count": 1,
        "missing_percent": 50.0,
        "check_rows_tested": 2,
        "dataset_rows_tested": 5,
        "scope_rows_tested": 2,
    }
    assert results["us"].outcome == CheckOutcome.EXCLUDED
    assert result.number_of_checks_excluded == 1

    scope_queries = [sql for sql in captured_sql if "_soda_filtered_scope_eu" in sql]
    assert len(scope_queries) == 1
    assert "_soda_filtered_dataset" not in scope_queries[0]
    assert not any("_soda_filtered_scope_us" in sql for sql in captured_sql)
    base_queries = [sql for sql in captured_sql if "_soda_filtered_dataset" in sql]
    assert len(base_queries) == 1


def test_without_activation_a_declared_scope_builds_no_sql(
    data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    captured_sql = _capture_executed_sql(data_source_test_helper, monkeypatch)

    result = _verify(
        data_source_test_helper,
        test_table,
        _scopes_yaml(data_source_test_helper)
        + """
            columns:
              - name: email
                checks:
                  - missing:
                      qualifier: all
                      threshold:
                        must_be_less_than: 5
                  - missing:
                      qualifier: eu
                      scope: eu
            checks:
              - row_count:
                  qualifier: us
                  scope: us
            """,
    )

    results = _results_by_qualifier(result)
    assert results["all"].outcome == CheckOutcome.PASSED
    assert "scope_rows_tested" not in results["all"].diagnostic_metric_values
    assert [results["eu"].outcome, results["us"].outcome] == [CheckOutcome.EXCLUDED, CheckOutcome.EXCLUDED]
    assert result.number_of_checks_excluded == 2
    assert not any("_soda_filtered_scope_" in sql for sql in captured_sql)


def test_identical_metrics_in_two_scopes_stay_separate(
    data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    captured_sql = _capture_executed_sql(data_source_test_helper, monkeypatch)

    with scope_activation("eu", "us"):
        result = _verify(
            data_source_test_helper,
            test_table,
            _scopes_yaml(data_source_test_helper)
            + """
            columns:
              - name: email
                checks:
                  - missing:
                      qualifier: eu
                      scope: eu
                      threshold:
                        must_be_less_than: 5
                  - missing:
                      qualifier: us
                      scope: us
                      threshold:
                        must_be_less_than: 5
            """,
        )

    results = _results_by_qualifier(result)
    assert (
        results["eu"].diagnostic_metric_values["missing_count"],
        results["us"].diagnostic_metric_values["missing_count"],
    ) == (1, 2)
    assert (
        results["eu"].diagnostic_metric_values["scope_rows_tested"],
        results["us"].diagnostic_metric_values["scope_rows_tested"],
    ) == (2, 3)

    # One aggregation query per active scope.
    for key in ["eu", "us"]:
        assert sum(f"_soda_filtered_scope_{key}" in sql for sql in captured_sql) == 1


def test_a_scoped_query_check_carries_no_scope_rows_tested(
    data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    captured_sql = _capture_executed_sql(data_source_test_helper, monkeypatch)

    with scope_activation("eu", "us"):
        result = _verify(
            data_source_test_helper,
            test_table,
            _scopes_yaml(data_source_test_helper)
            + f"""
            columns:
              - name: email
                checks:
                  - missing:
                      qualifier: eu
                      scope: eu
                      threshold:
                        must_be_less_than: 5
            checks:
              - metric:
                  qualifier: us
                  scope: us
                  query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
                  threshold:
                    must_be_greater_than: 0
            """,
        )

    results = _results_by_qualifier(result)
    assert results["eu"].diagnostic_metric_values["scope_rows_tested"] == 2
    assert results["us"].outcome == CheckOutcome.PASSED
    assert results["us"].diagnostic_metric_values == {"dataset_rows_tested": 5}
    # No check aggregates in us, so its scope builds no SQL.
    assert not any("_soda_filtered_scope_us" in sql for sql in captured_sql)


def test_a_scoped_reference_check_selects_from_its_scope(
    data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    reference_table = data_source_test_helper.ensure_test_table(reference_table_specification)
    captured_sql = _capture_executed_sql(data_source_test_helper, monkeypatch)

    with scope_activation("eu"):
        result = _verify(
            data_source_test_helper,
            test_table,
            _scopes_yaml(data_source_test_helper)
            + f"""
            columns:
              - name: email
                checks:
                  - invalid:
                      qualifier: all
                      valid_reference_data:
                        dataset: {data_source_test_helper.build_dqn(reference_table)}
                        column: email
                      threshold:
                        must_be_less_than: 5
                  - invalid:
                      qualifier: eu
                      scope: eu
                      valid_reference_data:
                        dataset: {data_source_test_helper.build_dqn(reference_table)}
                        column: email
            """,
        )

    results = _results_by_qualifier(result)
    # 'b' is the only value missing from the reference data, and it is in us.
    assert results["all"].diagnostic_metric_values["invalid_count"] == 1
    assert results["eu"].outcome == CheckOutcome.PASSED
    assert results["eu"].diagnostic_metric_values["invalid_count"] == 0
    assert results["eu"].diagnostic_metric_values["scope_rows_tested"] == 2

    reference_queries = [sql for sql in captured_sql if "_soda_filtered_referenced_dataset" in sql]
    assert len(reference_queries) == 2
    assert sum("_soda_filtered_scope_eu" in sql for sql in reference_queries) == 1
    assert sum("_soda_filtered_dataset" in sql for sql in reference_queries) == 1
