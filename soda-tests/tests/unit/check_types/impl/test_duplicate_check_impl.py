"""
Unit tests for duplicate check implementations — unique logic only.

Column duplicate checks have domain-specific behaviour:
- Derived computation: duplicate_count = row_count - missing_count - distinct_count
- Missing values are subtracted before computing duplicates
- duplicate_percent is computed from non-missing total

Multi-column duplicate checks:
- duplicate_count = row_count - distinct_count (no missing subtraction)
- Operates on a list of columns, not a single column

Generic threshold pass/fail/warn/NOT_EVALUATED patterns are covered in
test_threshold_checks.py.
"""

from helpers.impl_test_helpers import build_contract_impl, build_measurement_values
from soda_core.contracts.contract_verification import CheckOutcome
from soda_core.contracts.impl.check_types.duplicate_check import (
    ColumnDuplicateCheckImpl,
    DuplicateCountMetricImpl,
    MultiColumnDuplicateCheckImpl,
)


def test_duplicate_count_derived_from_components():
    """duplicate_count = row_count - missing_count - distinct_count.
    100 rows, 0 missing, 90 distinct → 10 duplicates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: order_id
        data_type: integer
        checks:
          - duplicate:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, ColumnDuplicateCheckImpl)

    mv = build_measurement_values(
        [
            (check.distinct_count_metric_impl, 90),
            (check.check_rows_tested_metric_impl, 100),
            (check.missing_count_metric_impl, 0),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.threshold_value == 10
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
        "missing_count": 0,
        "duplicate_count": 10,
        "duplicate_percent": 10.0,
    }


def test_duplicate_accounts_for_missing_values():
    """Missing values are subtracted before computing duplicates.
    100 rows, 5 missing, 95 distinct → 0 duplicates → PASSED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: order_id
        data_type: integer
        checks:
          - duplicate:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    mv = build_measurement_values(
        [
            (check.distinct_count_metric_impl, 95),
            (check.check_rows_tested_metric_impl, 100),
            (check.missing_count_metric_impl, 5),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.PASSED
    assert result.threshold_value == 0
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
        "missing_count": 5,
        "duplicate_count": 0,
        "duplicate_percent": 0.0,
    }


def test_duplicate_percent_computed_from_non_missing():
    """duplicate_percent = duplicate_count / (row_count - missing_count) * 100.
    100 rows, 0 missing, 80 distinct → 20 duplicates → 20%."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: order_id
        data_type: integer
        checks:
          - duplicate:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    mv = build_measurement_values(
        [
            (check.distinct_count_metric_impl, 80),
            (check.check_rows_tested_metric_impl, 100),
            (check.missing_count_metric_impl, 0),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
        "missing_count": 0,
        "duplicate_count": 20,
        "duplicate_percent": 20.0,
    }


def test_duplicate_percent_with_missing():
    """With missing values, percent is based on non-missing total.
    100 rows, 10 missing, 80 distinct → 10 duplicates.
    non_missing = 90, so percent = 10/90 * 100 ≈ 11.11."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: order_id
        data_type: integer
        checks:
          - duplicate:
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]

    mv = build_measurement_values(
        [
            (check.distinct_count_metric_impl, 80),
            (check.check_rows_tested_metric_impl, 100),
            (check.missing_count_metric_impl, 10),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.diagnostic_metric_values["duplicate_count"] == 10
    assert result.diagnostic_metric_values["missing_count"] == 10
    assert result.diagnostic_metric_values["check_rows_tested"] == 100
    assert result.diagnostic_metric_values["dataset_rows_tested"] == 100
    # 10 / 90 * 100 ≈ 11.11
    assert abs(result.diagnostic_metric_values["duplicate_percent"] - 11.11) < 0.1


def test_duplicate_setup_metrics_has_derived_duplicate_count():
    """setup_metrics() should register a DuplicateCountMetricImpl as a derived metric."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: order_id
        data_type: integer
        checks:
          - duplicate:
    """
    contract_impl = build_contract_impl(contract_yaml)
    derived_dup = [m for m in contract_impl.metrics if isinstance(m, DuplicateCountMetricImpl)]
    assert len(derived_dup) == 1


def test_multi_column_duplicate_passes_with_zero_duplicates():
    """Multi-column duplicate: 100 rows, 100 distinct combinations → 0 duplicates → PASSED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: first_name
        data_type: character varying
      - name: last_name
        data_type: character varying
    checks:
      - duplicate:
          columns:
            - first_name
            - last_name
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, MultiColumnDuplicateCheckImpl)

    mv = build_measurement_values(
        [
            (check.multi_column_distinct_count_metric_impl, 100),
            (check.row_count_metric_impl, 100),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.PASSED
    assert result.diagnostic_metric_values == {
        "duplicate_count": 0,
        "duplicate_percent": 0.0,
        "check_rows_tested": 100,
        "dataset_rows_tested": 100,
    }


def test_multi_column_duplicate_fails_with_duplicates():
    """Multi-column duplicate: 100 rows, 90 distinct → 10 duplicates → FAILED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: first_name
        data_type: character varying
      - name: last_name
        data_type: character varying
    checks:
      - duplicate:
          columns:
            - first_name
            - last_name
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, MultiColumnDuplicateCheckImpl)

    mv = build_measurement_values(
        [
            (check.multi_column_distinct_count_metric_impl, 90),
            (check.row_count_metric_impl, 100),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.outcome == CheckOutcome.FAILED
    assert result.diagnostic_metric_values == {
        "duplicate_count": 10,
        "duplicate_percent": 10.0,
        "check_rows_tested": 100,
        "dataset_rows_tested": 100,
    }


def test_multi_column_duplicate_no_missing_subtraction():
    """Multi-column duplicate computes duplicate_count = row_count - distinct_count
    (no missing subtraction, unlike column duplicate)."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: first_name
        data_type: character varying
      - name: last_name
        data_type: character varying
    checks:
      - duplicate:
          columns:
            - first_name
            - last_name
    """
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, MultiColumnDuplicateCheckImpl)

    # 100 rows, 95 distinct → 5 duplicates (missing is not factored in)
    mv = build_measurement_values(
        [
            (check.multi_column_distinct_count_metric_impl, 95),
            (check.row_count_metric_impl, 100),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.diagnostic_metric_values == {
        "duplicate_count": 5,
        "duplicate_percent": 5.0,
        "check_rows_tested": 100,
        "dataset_rows_tested": 100,
    }
