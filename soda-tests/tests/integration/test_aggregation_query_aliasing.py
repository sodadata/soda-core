"""
Regression tests for two linked bugs surfaced on Databricks (DBR 18.2 / Spark 4.1):

1. The aggregation query emitted byte-identical SUM(CASE WHEN col IS NULL ...) expressions
   whenever a column had both `missing:` and `invalid:` checks (because InvalidCheckImpl
   started resolving its own MissingCountMetricImpl in PR #2588, and those instances do
   not dedup against the missing check's MissingCountMetricImpl). Spark 4.x rejects result
   schemas with duplicate auto-derived column names; older runtimes tolerated them.

2. When the aggregation query fails for any reason, the duplicate check's
   DuplicateCountMetricImpl.compute_derived_value crashes the whole scan with
   `unsupported operand type(s) for -: 'NoneType' and 'NoneType'` because it does not
   guard `valid_count - distinct_count` against None.
"""

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    ContractVerificationResult,
)

_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("aggregation_aliasing")
    .column_varchar("id")
    .column_integer("age")
    .rows(
        rows=[
            ("1", 1),
            ("2", 2),
            ("3", None),
            (None, 4),
        ]
    )
    .build()
)


def _capture_executed_sql(data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Patch the active data source connection to record every SQL it executes.

    Uses pytest's `monkeypatch` so the wrap is auto-undone at test teardown — important
    because the data source connection is session-scoped and the wrap would otherwise
    leak into every subsequent test.
    """
    captured: list[str] = []
    connection = data_source_test_helper.data_source_impl.data_source_connection
    original = connection.execute_query

    def _wrapped(sql: str, log_query: bool = True):
        captured.append(sql)
        return original(sql, log_query)

    monkeypatch.setattr(connection, "execute_query", _wrapped)
    return captured


def _find_aggregation_sql(captured_sql: list[str]) -> str:
    """Locate the aggregation query among captured SQL.

    AggregationQuery is the only query that wraps the dataset in the
    `_soda_filtered_dataset` CTE and then SELECTs aggregate expressions from it.
    """
    candidates = [sql for sql in captured_sql if "_soda_filtered_dataset" in sql and "SUM(" in sql.upper()]
    assert candidates, f"Expected an aggregation query to have been executed. Captured:\n{captured_sql}"
    # If there are multiple (large contracts may split), assert on the first one — it's
    # enough that one carries the duplicated metrics for the regression to manifest.
    return candidates[0]


def _extract_select_field_lines(aggregation_sql: str) -> list[str]:
    """Return the list of stripped SELECT field expressions for the OUTER SELECT.

    The aggregation SQL has a CTE which itself contains a `SELECT *` — we want the
    fields from the outer SELECT (the one whose FROM is `_soda_filtered_dataset`),
    not the CTE's inner SELECT.
    """
    lines = aggregation_sql.splitlines()
    # Locate the outer FROM line by looking for the dataset CTE alias.
    outer_from_idx = next(
        i for i, line in enumerate(lines) if line.lstrip().startswith("FROM ") and "_soda_filtered_dataset" in line
    )
    # The outer SELECT is the last `SELECT ` line before that FROM.
    outer_select_idx = max(i for i, line in enumerate(lines[:outer_from_idx]) if line.lstrip().startswith("SELECT "))
    fields: list[str] = []
    for i in range(outer_select_idx, outer_from_idx):
        line = lines[i]
        if i == outer_select_idx:
            line = line.lstrip()[len("SELECT ") :]
        fields.append(line.strip().rstrip(","))
    return fields


def test_missing_and_invalid_on_same_column_produces_unique_aggregation_aliases(
    data_source_test_helper: DataSourceTestHelper,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Issue #1: missing + invalid on the same column must not emit byte-identical
    aggregation expressions (which become duplicate output column names on Spark 4.x)."""
    test_table = data_source_test_helper.ensure_test_table(_test_table_specification)
    captured_sql = _capture_executed_sql(data_source_test_helper, monkeypatch)

    data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
                checks:
                  - missing:
                  - invalid:
                      valid_values: ['1', '2', '3']
            """,
    )

    aggregation_sql = _find_aggregation_sql(captured_sql)
    fields = _extract_select_field_lines(aggregation_sql)

    # The aggregation query must not contain two byte-identical field expressions —
    # that is the shape that triggers "Can't unify schema with duplicate field names"
    # on Databricks Spark 4.x.
    duplicates = {f for f in fields if fields.count(f) > 1}
    assert not duplicates, (
        f"Aggregation query has duplicate field expressions {duplicates!r}. "
        f"Full SELECT list:\n  " + "\n  ".join(fields) + f"\n\nFull SQL:\n{aggregation_sql}"
    )


def test_duplicate_check_survives_failed_aggregation_query(
    data_source_test_helper: DataSourceTestHelper,
) -> None:
    """Issue #2: when the aggregation query fails (here: poisoned by a failed_rows
    expression referencing a non-existent column), the duplicate check's derived
    metric must not crash the scan with TypeError. It should produce NOT_EVALUATED."""
    test_table = data_source_test_helper.ensure_test_table(_test_table_specification)

    # No assert_contract_* helper directly accepts "may raise or may return errors",
    # so go through verify_contract and assert on the resulting outcome instead of
    # letting an uncaught TypeError tear the test down.
    session_result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - failed_rows:
                  name: poison the aggregation query
                  expression: "definitely_not_a_column IS NULL"
            columns:
              - name: id
                checks:
                  - duplicate:
                      name: id should be unique
            """,
    )

    assert session_result is not None, "verify_contract returned no session result — likely a TypeError"
    assert session_result.contract_verification_results, "Expected at least one contract verification result"
    result: ContractVerificationResult = session_result.contract_verification_results[0]

    duplicate_results = [r for r in result.check_results if r.check.name == "id should be unique"]
    assert (
        duplicate_results
    ), f"Expected the duplicate check to produce a result. Got: {[r.check.name for r in result.check_results]}"
    duplicate_result: CheckResult = duplicate_results[0]

    assert duplicate_result.outcome == CheckOutcome.NOT_EVALUATED, (
        f"Expected duplicate check outcome NOT_EVALUATED when its aggregation query "
        f"failed upstream; got {duplicate_result.outcome}"
    )


def test_failed_rows_percent_check_does_not_falsely_pass_when_aggregation_fails(
    data_source_test_helper: DataSourceTestHelper,
) -> None:
    """Regression: `failed_rows: expression` with `metric: percent` and a threshold of
    `must_be: 0` previously defaulted failed_rows_percent to 0 when its aggregation
    query failed — producing a falsely PASSED outcome against the 0 threshold.

    Use a syntactically valid expression that the aggregation rejects at execute
    time. Most data sources will fail it (unresolved column). The check must be
    NOT_EVALUATED, not PASSED.
    """
    test_table = data_source_test_helper.ensure_test_table(_test_table_specification)

    session_result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - failed_rows:
                  name: failed_rows percent on poisoned expression
                  expression: "definitely_not_a_column IS NULL"
                  threshold:
                    metric: percent
                    must_be: 0
            """,
    )

    assert session_result is not None
    assert session_result.contract_verification_results
    result = session_result.contract_verification_results[0]
    failed_rows_results = [
        r for r in result.check_results if r.check.name == "failed_rows percent on poisoned expression"
    ]
    assert failed_rows_results, "Expected the failed_rows check to produce a result"
    failed_rows_result: CheckResult = failed_rows_results[0]

    assert failed_rows_result.outcome == CheckOutcome.NOT_EVALUATED, (
        f"Expected NOT_EVALUATED when the failed_rows aggregation query failed; "
        f"got {failed_rows_result.outcome} (this is the falsely-PASSED regression)"
    )


def test_missing_percent_does_not_falsely_pass_when_aggregation_fails(
    data_source_test_helper: DataSourceTestHelper,
) -> None:
    """Regression: a `missing:` check with `metric: percent` and `must_be: 0`
    threshold must not falsely PASS when its underlying missing_count aggregation
    query fails. The bug was in `DerivedPercentageMetricImpl.compute_derived_value`
    defaulting to 0 when `fraction is None` — same shape as the failed_rows bug,
    powering missing_percent / invalid_percent / duplicate_percent."""
    test_table = data_source_test_helper.ensure_test_table(_test_table_specification)

    session_result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - failed_rows:
                  name: poison the aggregation query
                  expression: "definitely_not_a_column IS NULL"
            columns:
              - name: id
                checks:
                  - missing:
                      name: id missing percent
                      threshold:
                        metric: percent
                        must_be_less_than_or_equal: 0
            """,
    )

    assert session_result is not None
    assert session_result.contract_verification_results
    result = session_result.contract_verification_results[0]
    missing_results = [r for r in result.check_results if r.check.name == "id missing percent"]
    assert missing_results, "Expected the missing check to produce a result"
    missing_result: CheckResult = missing_results[0]

    assert missing_result.outcome == CheckOutcome.NOT_EVALUATED, (
        f"Expected NOT_EVALUATED when the missing_count aggregation query failed; "
        f"got {missing_result.outcome} (this is the DerivedPercentageMetricImpl falsely-PASSED regression)"
    )
