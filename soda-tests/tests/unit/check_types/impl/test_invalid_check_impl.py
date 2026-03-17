"""
Unit tests for InvalidCheckImpl — unique logic only.

Invalid checks have domain-specific behaviour:
- Require valid_values (or valid_format) on the column to function
- Track missing_count alongside invalid_count in diagnostics
- Support both count and percent metrics

Generic threshold pass/fail/warn/NOT_EVALUATED patterns are covered in
test_threshold_checks.py.  Percent threshold is covered there too.
"""

from helpers.impl_test_helpers import (
    build_contract_impl,
    build_measurement_values,
    get_check_impl,
)
from soda_core.contracts.impl.check_types.invalidity_check import (
    InvalidCheckImpl,
    InvalidCountMetricImpl,
)
from soda_core.contracts.impl.check_types.missing_check import MissingCountMetricImpl
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl

INVALID_YAML = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        valid_values: ['active', 'inactive']
        checks:
          - invalid:
"""


def test_invalid_setup_creates_invalid_missing_and_row_count_metrics():
    """InvalidCheckImpl needs InvalidCount, MissingCount, and RowCount metrics."""
    check = get_check_impl(INVALID_YAML)
    assert isinstance(check, InvalidCheckImpl)
    invalid_count_metrics = [m for m in check.metrics if isinstance(m, InvalidCountMetricImpl)]
    missing_count_metrics = [m for m in check.metrics if isinstance(m, MissingCountMetricImpl)]
    row_count_metrics = [m for m in check.metrics if isinstance(m, RowCountMetricImpl)]
    assert len(invalid_count_metrics) == 1
    assert len(missing_count_metrics) == 1
    assert len(row_count_metrics) >= 1


def test_invalid_diagnostics_include_missing_count():
    """CheckResult diagnostics should include missing_count alongside invalid_count."""
    contract_impl = build_contract_impl(INVALID_YAML)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, InvalidCheckImpl)

    mv = build_measurement_values(
        [
            (check.invalid_count_metric_impl, 10),
            (check.missing_count_metric_impl, 5),
            (check.row_count_metric, 200),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 200
    result = check.evaluate(mv)
    assert result.diagnostic_metric_values == {
        "invalid_count": 10,
        "invalid_percent": 5.0,
        "missing_count": 5,
        "check_rows_tested": 200,
        "dataset_rows_tested": 200,
    }


def test_invalid_percent_accounts_for_missing():
    """invalid_percent is based on total rows (not just non-missing rows).
    8 invalid out of 100 rows → 8.0% regardless of missing count."""
    contract_impl = build_contract_impl(INVALID_YAML)
    check = contract_impl.all_check_impls[0]

    mv = build_measurement_values(
        [
            (check.invalid_count_metric_impl, 8),
            (check.missing_count_metric_impl, 12),
            (check.row_count_metric, 100),
        ],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = 100
    result = check.evaluate(mv)
    assert result.diagnostic_metric_values == {
        "invalid_count": 8,
        "invalid_percent": 8.0,
        "missing_count": 12,
        "check_rows_tested": 100,
        "dataset_rows_tested": 100,
    }
