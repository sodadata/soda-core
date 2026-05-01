import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    ContractVerificationResult,
)
from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult

# These tests deliberately reference a missing column to exercise the SQL-error
# degradation path. The snapshot recorder only captures successful queries (see
# snapshot_connection.py execute_query record branch), so the failing aggregation
# never enters the snapshot — replay then surfaces a SnapshotMismatchError instead
# of the real DB error the assertions look for. Run against real DB only.
pytestmark = pytest.mark.no_snapshot

# Table has only `id`. The contracts below reference `ghost_column` which does not exist —
# without the fix this triggers `column "ghost_column" does not exist` from the database
# and the entire scan aborts before the schema check can evaluate.
test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("schema_survives_missing")
    .column_varchar("id")
    .rows(rows=[("1",), ("2",)])
    .build()
)

multi_column_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("schema_survives_partial_missing")
    .column_varchar("id")
    .column_integer("age")
    .rows(rows=[("1", 1), (None, 2), ("3", None)])
    .build()
)


def test_schema_check_runs_when_column_check_targets_missing_column(
    data_source_test_helper: DataSourceTestHelper,
):
    """Headline scenario from the bug report: schema + missing_count on a dropped column.

    With the fix, the schema check FAILs with `ghost_column` reported as missing and the
    `missing_count` check degrades to NOT_EVALUATED. Without the fix, the SQL exception
    propagates out of `verify()` and this test errors out before reaching the assertion.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
              - name: ghost_column
                checks:
                  - missing:
        """,
    )

    schema_results = [r for r in result.check_results if isinstance(r, SchemaCheckResult)]
    assert len(schema_results) == 1, "Schema check did not run — scan was aborted by aggregation failure"
    assert "ghost_column" in schema_results[0].expected_column_names_not_actual

    other_results = [r for r in result.check_results if not isinstance(r, SchemaCheckResult)]
    assert len(other_results) == 1
    assert other_results[0].outcome == CheckOutcome.NOT_EVALUATED


def test_column_check_on_missing_column_without_schema_check(
    data_source_test_helper: DataSourceTestHelper,
):
    """Without a schema check defined, the column-level check still degrades cleanly
    rather than crashing the scan."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: ghost_column
                checks:
                  - missing:
        """,
    )

    assert len(result.check_results) == 1
    assert result.check_results[0].outcome == CheckOutcome.NOT_EVALUATED

    errors_str = "\n".join(str(r.msg) for r in (result.log_records or []) if r.levelno >= 40)
    assert "ghost_column" in errors_str, (
        f"Expected the SQL error referencing 'ghost_column' to appear in contract errors. " f"Got: {errors_str!r}"
    )


def test_scan_completes_when_one_column_in_multi_column_contract_is_missing(
    data_source_test_helper: DataSourceTestHelper,
):
    """A contract that mixes checks on existing and dropped columns must complete the scan
    so the schema check still reports the dropped column. Note: missing/invalid metrics
    across columns are batched into a single aggregation query; if any column reference in
    that batch fails, every metric in the batch degrades to NOT_EVALUATED. That batching is
    pre-existing soda-core behavior outside the scope of this fix."""
    test_table = data_source_test_helper.ensure_test_table(multi_column_table_specification)

    result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - schema:
            columns:
              - name: id
                checks:
                  - missing:
              - name: age
              - name: ghost_column
                checks:
                  - missing:
        """,
    )

    schema_results = [r for r in result.check_results if isinstance(r, SchemaCheckResult)]
    assert len(schema_results) == 1, "Schema check did not run — scan was aborted"
    assert "ghost_column" in schema_results[0].expected_column_names_not_actual

    non_schema_results = [r for r in result.check_results if not isinstance(r, SchemaCheckResult)]
    assert len(non_schema_results) == 2
    assert all(r.outcome == CheckOutcome.NOT_EVALUATED for r in non_schema_results)


def test_non_sodacore_exception_still_aborts(
    data_source_test_helper: DataSourceTestHelper,
    monkeypatch: pytest.MonkeyPatch,
):
    """The wrap is intentionally narrow: only SodaCoreException is swallowed. Anything
    else (programming bugs, infrastructure failures) must still propagate so it gets
    noticed instead of silently masked."""
    from soda_core.contracts.impl.contract_verification_impl import AggregationQuery

    test_table = data_source_test_helper.ensure_test_table(multi_column_table_specification)

    def boom(self):
        raise ValueError("simulated programming bug")

    monkeypatch.setattr(AggregationQuery, "execute", boom)

    with pytest.raises(ValueError, match="simulated programming bug"):
        data_source_test_helper.assert_contract_fail(
            test_table=test_table,
            contract_yaml_str=f"""
                columns:
                  - name: id
                    checks:
                      - missing:
            """,
        )
