"""
Parametrized tests for all threshold-based check types.

These tests verify the generic evaluate_threshold() machinery that is shared
across check types: PASSED / FAILED / NOT_EVALUATED / WARN outcomes,
threshold_value propagation, and the presence of standard diagnostic metrics.

Check-type-specific logic (e.g. duplicate derived computation, freshness
timedelta, schema column matching) is tested in dedicated files.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional

import pytest
from helpers.impl_test_helpers import (
    build_contract_impl,
    build_measurement_values,
    get_check_impl,
)
from soda_core.contracts.contract_verification import CheckOutcome
from soda_core.contracts.impl.check_types.aggregate_check import (
    AggregateCheckImpl,
    AggregateFunctionMetricImpl,
)
from soda_core.contracts.impl.check_types.failed_rows_check import (
    FailedRowsCheckImpl,
    FailedRowsExpressionMetricImpl,
)
from soda_core.contracts.impl.check_types.invalidity_check import InvalidCheckImpl
from soda_core.contracts.impl.check_types.metric_check import (
    MetricCheckImpl,
    MetricExpressionMetricImpl,
)
from soda_core.contracts.impl.check_types.missing_check import (
    MissingCheckImpl,
    MissingCountMetricImpl,
)
from soda_core.contracts.impl.check_types.row_count_check import (
    RowCountCheckImpl,
    RowCountMetricImpl,
)
from soda_core.contracts.impl.contract_verification_impl import (
    CheckImpl,
    ContractImpl,
    DerivedPercentageMetricImpl,
)

# ---------------------------------------------------------------------------
# Configuration dataclass: one entry per check type
# ---------------------------------------------------------------------------


@dataclass
class CheckConfig:
    """Describes how to build and evaluate a single threshold-based check type."""

    id: str
    # YAML snippet that defines the contract with a single check
    yaml: str
    # Expected CheckImpl subclass
    check_class: type
    # Expected primary metric class (the main metric created by setup_metrics)
    primary_metric_class: type
    # Build (metric, value) tuples for a passing scenario
    passing_metrics: Callable[[CheckImpl, ContractImpl], list[tuple]]
    # Build (metric, value) tuples for a failing scenario
    failing_metrics: Callable[[CheckImpl, ContractImpl], list[tuple]]
    # The expected threshold_value for the passing scenario
    passing_threshold_value: object
    # The expected threshold_value for the failing scenario
    failing_threshold_value: object
    # Diagnostic metric keys expected in the result (subset check)
    expected_diagnostic_keys: list[str]
    # Full expected diagnostic_metric_values dict for the passing scenario
    expected_diagnostic_values: Optional[Callable[[CheckImpl, ContractImpl], dict]] = None
    # Build (metric, value) tuples for a None (not-evaluated) scenario
    none_metrics: Optional[Callable[[CheckImpl, ContractImpl], list[tuple]]] = None
    # YAML for warn-threshold variant (if supported)
    warn_yaml: Optional[str] = None
    # Build (metric, value) tuples for the warn scenario
    warn_metrics: Optional[Callable[[CheckImpl, ContractImpl], list[tuple]]] = None


# ---------------------------------------------------------------------------
# Check type configurations
# ---------------------------------------------------------------------------


ROW_COUNT_CONFIG = CheckConfig(
    id="row_count",
    yaml="""
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - row_count:
    """,
    check_class=RowCountCheckImpl,
    primary_metric_class=RowCountMetricImpl,
    passing_metrics=lambda check, ci: [
        (check.row_count_metric, 100),
        (ci.row_count_metric_impl, 100),
    ],
    failing_metrics=lambda check, ci: [
        (check.row_count_metric, 0),
        (ci.row_count_metric_impl, 0),
    ],
    passing_threshold_value=100,
    failing_threshold_value=0,
    expected_diagnostic_keys=["check_rows_tested", "dataset_rows_tested"],
    expected_diagnostic_values=lambda check, ci: {
        "check_rows_tested": 100,
        "dataset_rows_tested": 100,
    },
    none_metrics=lambda check, ci: [
        (check.row_count_metric, None),
        (ci.row_count_metric_impl, None),
    ],
    warn_yaml="""
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - row_count:
          threshold:
            must_be_greater_than: 0
            level: warn
    """,
    warn_metrics=lambda check, ci: [
        (check.row_count_metric, 0),
        (ci.row_count_metric_impl, 0),
    ],
)

MISSING_CONFIG = CheckConfig(
    id="missing",
    yaml="""
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
        checks:
          - missing:
    """,
    check_class=MissingCheckImpl,
    primary_metric_class=MissingCountMetricImpl,
    passing_metrics=lambda check, ci: [
        (check.missing_count_metric_impl, 0),
        (check.row_count_metric_impl, 100),
    ],
    failing_metrics=lambda check, ci: [
        (check.missing_count_metric_impl, 5),
        (check.row_count_metric_impl, 100),
    ],
    passing_threshold_value=0,
    failing_threshold_value=5,
    expected_diagnostic_keys=["missing_count", "missing_percent", "check_rows_tested", "dataset_rows_tested"],
    expected_diagnostic_values=lambda check, ci: {
        "missing_count": 0,
        "missing_percent": 0.0,
        "check_rows_tested": 100,
        "dataset_rows_tested": 100,
    },
    none_metrics=lambda check, ci: [
        (check.missing_count_metric_impl, None),
        (check.row_count_metric_impl, None),
    ],
    warn_yaml="""
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
        checks:
          - missing:
              threshold:
                must_be: 0
                level: warn
    """,
    warn_metrics=lambda check, ci: [
        (check.missing_count_metric_impl, 3),
        (check.row_count_metric_impl, 100),
    ],
)

AGGREGATE_CONFIG = CheckConfig(
    id="aggregate",
    yaml="""
    dataset: my_data_source/my_dataset
    columns:
      - name: amount
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              threshold:
                must_be_greater_than: 1000
    """,
    check_class=AggregateCheckImpl,
    primary_metric_class=AggregateFunctionMetricImpl,
    passing_metrics=lambda check, ci: [
        (check.aggregate_metric, 1500),
        (check.check_rows_tested_metric, 100),
    ],
    failing_metrics=lambda check, ci: [
        (check.aggregate_metric, 500),
        (check.check_rows_tested_metric, 100),
    ],
    passing_threshold_value=1500,
    failing_threshold_value=500,
    expected_diagnostic_keys=["sum", "check_rows_tested", "dataset_rows_tested"],
    expected_diagnostic_values=lambda check, ci: {
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
        "sum": 1500,
    },
    none_metrics=lambda check, ci: [
        (check.aggregate_metric, None),
        (check.check_rows_tested_metric, None),
    ],
    warn_yaml="""
    dataset: my_data_source/my_dataset
    columns:
      - name: amount
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              threshold:
                must_be_greater_than: 1000
                level: warn
    """,
    warn_metrics=lambda check, ci: [
        (check.aggregate_metric, 500),
        (check.check_rows_tested_metric, 100),
    ],
)

METRIC_EXPRESSION_CONFIG = CheckConfig(
    id="metric_expression",
    yaml="""
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          expression: sum(amount)
          threshold:
            must_be_greater_than: 100
    """,
    check_class=MetricCheckImpl,
    primary_metric_class=MetricExpressionMetricImpl,
    passing_metrics=lambda check, ci: [
        (check.numeric_metric_impl, 150),
        (check.check_rows_tested_metric_impl, 50),
    ],
    failing_metrics=lambda check, ci: [
        (check.numeric_metric_impl, 50),
        (check.check_rows_tested_metric_impl, 50),
    ],
    passing_threshold_value=150,
    failing_threshold_value=50,
    expected_diagnostic_keys=["dataset_rows_tested"],
    expected_diagnostic_values=lambda check, ci: {
        "dataset_rows_tested": 100,
    },
    none_metrics=lambda check, ci: [
        (check.numeric_metric_impl, None),
        (check.check_rows_tested_metric_impl, None),
    ],
    warn_yaml="""
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          expression: sum(amount)
          threshold:
            must_be_greater_than: 100
            level: warn
    """,
    warn_metrics=lambda check, ci: [
        (check.numeric_metric_impl, 50),
        (check.check_rows_tested_metric_impl, 50),
    ],
)

FAILED_ROWS_CONFIG = CheckConfig(
    id="failed_rows",
    yaml="""
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - failed_rows:
          expression: "status NOT IN ('active', 'inactive')"
    """,
    check_class=FailedRowsCheckImpl,
    primary_metric_class=FailedRowsExpressionMetricImpl,
    passing_metrics=lambda check, ci: [
        (check.failed_rows_count_metric_impl, 0),
        (check.check_rows_tested_metric_impl, 100),
    ],
    failing_metrics=lambda check, ci: [
        (check.failed_rows_count_metric_impl, 5),
        (check.check_rows_tested_metric_impl, 100),
    ],
    passing_threshold_value=0,
    failing_threshold_value=5,
    expected_diagnostic_keys=[
        "failed_rows_count",
        "failed_rows_percent",
        "check_rows_tested",
        "dataset_rows_tested",
    ],
    expected_diagnostic_values=lambda check, ci: {
        "failed_rows_count": 0,
        "failed_rows_percent": 0.0,
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
    },
    none_metrics=lambda check, ci: [
        (check.failed_rows_count_metric_impl, None),
        (check.check_rows_tested_metric_impl, None),
    ],
    warn_yaml="""
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - failed_rows:
          expression: "status NOT IN ('active', 'inactive')"
          threshold:
            must_be: 0
            level: warn
    """,
    warn_metrics=lambda check, ci: [
        (check.failed_rows_count_metric_impl, 3),
        (check.check_rows_tested_metric_impl, 100),
    ],
)


ALL_CONFIGS = [
    ROW_COUNT_CONFIG,
    MISSING_CONFIG,
    AGGREGATE_CONFIG,
    METRIC_EXPRESSION_CONFIG,
    FAILED_ROWS_CONFIG,
]


def _config_ids(configs: list[CheckConfig]) -> list[str]:
    return [c.id for c in configs]


# ---------------------------------------------------------------------------
# Helper: build + evaluate
# ---------------------------------------------------------------------------


def _build_and_get_check(yaml_str: str) -> tuple[ContractImpl, CheckImpl]:
    contract_impl = build_contract_impl(yaml_str)
    check = contract_impl.all_check_impls[0]
    return contract_impl, check


def _evaluate_check(
    contract_impl: ContractImpl,
    check: CheckImpl,
    metric_values: list[tuple],
    dataset_rows_tested: object = 100,
):
    mv = build_measurement_values(metric_values, contract_impl=contract_impl)
    contract_impl.dataset_rows_tested = dataset_rows_tested
    return check.evaluate(mv)


# ---------------------------------------------------------------------------
# setup_metrics: check class and primary metric
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_setup_creates_correct_check_class(config: CheckConfig):
    """Parsing the YAML should produce the expected CheckImpl subclass."""
    check = get_check_impl(config.yaml)
    assert isinstance(check, config.check_class)


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_setup_creates_primary_metric(config: CheckConfig):
    """setup_metrics() should register the primary metric for this check type."""
    check = get_check_impl(config.yaml)
    primary_metrics = [m for m in check.metrics if isinstance(m, config.primary_metric_class)]
    assert len(primary_metrics) >= 1


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_setup_creates_row_count_metric(config: CheckConfig):
    """Every threshold check should register a RowCountMetricImpl."""
    check = get_check_impl(config.yaml)
    row_count_metrics = [m for m in check.metrics if isinstance(m, RowCountMetricImpl)]
    assert len(row_count_metrics) >= 1


# ---------------------------------------------------------------------------
# evaluate: standard outcomes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_evaluate_passes_when_threshold_met(config: CheckConfig):
    """Metric value meeting default/explicit threshold → PASSED."""
    contract_impl, check = _build_and_get_check(config.yaml)
    assert isinstance(check, config.check_class)
    result = _evaluate_check(contract_impl, check, config.passing_metrics(check, contract_impl))
    assert result.outcome == CheckOutcome.PASSED


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_evaluate_fails_when_threshold_not_met(config: CheckConfig):
    """Metric value violating default/explicit threshold → FAILED."""
    contract_impl, check = _build_and_get_check(config.yaml)
    result = _evaluate_check(contract_impl, check, config.failing_metrics(check, contract_impl))
    assert result.outcome == CheckOutcome.FAILED


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_evaluate_not_evaluated_when_none(config: CheckConfig):
    """Primary metric value = None → NOT_EVALUATED."""
    contract_impl, check = _build_and_get_check(config.yaml)
    metrics_fn = config.none_metrics or config.failing_metrics
    result = _evaluate_check(contract_impl, check, metrics_fn(check, contract_impl), dataset_rows_tested=None)
    assert result.outcome == CheckOutcome.NOT_EVALUATED


# ---------------------------------------------------------------------------
# evaluate: threshold_value propagation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_evaluate_threshold_value_on_pass(config: CheckConfig):
    """CheckResult.threshold_value should equal the measured metric value when passing."""
    contract_impl, check = _build_and_get_check(config.yaml)
    result = _evaluate_check(contract_impl, check, config.passing_metrics(check, contract_impl))
    assert result.threshold_value == config.passing_threshold_value


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_evaluate_threshold_value_on_fail(config: CheckConfig):
    """CheckResult.threshold_value should equal the measured metric value when failing."""
    contract_impl, check = _build_and_get_check(config.yaml)
    result = _evaluate_check(contract_impl, check, config.failing_metrics(check, contract_impl))
    assert result.threshold_value == config.failing_threshold_value


# ---------------------------------------------------------------------------
# evaluate: diagnostic metrics
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("config", ALL_CONFIGS, ids=_config_ids(ALL_CONFIGS))
def test_evaluate_diagnostic_metric_keys(config: CheckConfig):
    """CheckResult.diagnostic_metric_values should include all expected keys."""
    contract_impl, check = _build_and_get_check(config.yaml)
    result = _evaluate_check(contract_impl, check, config.passing_metrics(check, contract_impl))
    for key in config.expected_diagnostic_keys:
        assert key in result.diagnostic_metric_values, f"Missing diagnostic key: {key}"


CONFIGS_WITH_DIAGNOSTIC_VALUES = [c for c in ALL_CONFIGS if c.expected_diagnostic_values is not None]


@pytest.mark.parametrize("config", CONFIGS_WITH_DIAGNOSTIC_VALUES, ids=_config_ids(CONFIGS_WITH_DIAGNOSTIC_VALUES))
def test_evaluate_diagnostic_metric_values(config: CheckConfig):
    """CheckResult.diagnostic_metric_values should exactly match expected dict."""
    contract_impl, check = _build_and_get_check(config.yaml)
    result = _evaluate_check(contract_impl, check, config.passing_metrics(check, contract_impl))
    expected = config.expected_diagnostic_values(check, contract_impl)
    assert result.diagnostic_metric_values == expected


# ---------------------------------------------------------------------------
# evaluate: WARN outcome (step 4 — previously uncovered)
# ---------------------------------------------------------------------------


WARN_CONFIGS = [c for c in ALL_CONFIGS if c.warn_yaml is not None]


@pytest.mark.parametrize("config", WARN_CONFIGS, ids=_config_ids(WARN_CONFIGS))
def test_evaluate_warn_when_threshold_not_met_with_warn_level(config: CheckConfig):
    """Threshold with level: warn, metric violating threshold → WARN (not FAILED)."""
    contract_impl, check = _build_and_get_check(config.warn_yaml)
    assert isinstance(check, config.check_class)
    result = _evaluate_check(contract_impl, check, config.warn_metrics(check, contract_impl))
    assert result.outcome == CheckOutcome.WARN


# ---------------------------------------------------------------------------
# evaluate: percent-based thresholds (step 4 — previously uncovered)
# ---------------------------------------------------------------------------


def test_missing_percent_threshold():
    """missing check with metric: percent uses missing_percent as threshold_value."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
        checks:
          - missing:
              threshold:
                metric: percent
                must_be_less_than: 10
    """
    contract_impl, check = _build_and_get_check(contract_yaml)
    assert isinstance(check, MissingCheckImpl)
    # 5 missing out of 100 → 5%
    result = _evaluate_check(
        contract_impl,
        check,
        [(check.missing_count_metric_impl, 5), (check.row_count_metric_impl, 100)],
    )
    assert result.outcome == CheckOutcome.PASSED
    assert result.threshold_value == 5.0


def test_missing_percent_threshold_fails():
    """missing check with metric: percent, 15% missing where threshold < 10 → FAILED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
        checks:
          - missing:
              threshold:
                metric: percent
                must_be_less_than: 10
    """
    contract_impl, check = _build_and_get_check(contract_yaml)
    # 15 missing out of 100 → 15%
    result = _evaluate_check(
        contract_impl,
        check,
        [(check.missing_count_metric_impl, 15), (check.row_count_metric_impl, 100)],
    )
    assert result.outcome == CheckOutcome.FAILED
    assert result.threshold_value == 15.0


def test_invalid_percent_threshold():
    """invalid check with metric: percent uses invalid_percent as threshold_value."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        valid_values: ['active', 'inactive']
        checks:
          - invalid:
              threshold:
                metric: percent
                must_be_less_than: 10
    """
    contract_impl, check = _build_and_get_check(contract_yaml)
    assert isinstance(check, InvalidCheckImpl)
    # 3 invalid out of 100 total, 2 missing → 3%
    result = _evaluate_check(
        contract_impl,
        check,
        [
            (check.invalid_count_metric_impl, 3),
            (check.missing_count_metric_impl, 2),
            (check.row_count_metric, 100),
        ],
    )
    assert result.outcome == CheckOutcome.PASSED
    assert isinstance(result.threshold_value, float)


def test_failed_rows_percent_threshold():
    """failed_rows check with metric: percent uses failed_rows_percent as threshold_value."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - failed_rows:
          expression: "status NOT IN ('active', 'inactive')"
          threshold:
            metric: percent
            must_be_less_than: 10
    """
    contract_impl, check = _build_and_get_check(contract_yaml)
    assert isinstance(check, FailedRowsCheckImpl)
    # 5 failed out of 100 → 5%
    result = _evaluate_check(
        contract_impl,
        check,
        [(check.failed_rows_count_metric_impl, 5), (check.check_rows_tested_metric_impl, 100)],
    )
    assert result.outcome == CheckOutcome.PASSED
    assert result.threshold_value == 5.0


# ---------------------------------------------------------------------------
# setup_metrics: derived percent metric registration (missing / invalid)
# ---------------------------------------------------------------------------


def test_missing_setup_registers_derived_percent():
    """MissingCheckImpl should register a DerivedPercentageMetricImpl for missing_percent."""
    contract_impl = build_contract_impl(MISSING_CONFIG.yaml)
    derived_metrics = [m for m in contract_impl.metrics if isinstance(m, DerivedPercentageMetricImpl)]
    assert any(m.type == "missing_percent" for m in derived_metrics)


def test_invalid_setup_registers_derived_percent():
    """InvalidCheckImpl should register a DerivedPercentageMetricImpl for invalid_percent."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        valid_values: ['active', 'inactive']
        checks:
          - invalid:
    """
    contract_impl = build_contract_impl(contract_yaml)
    derived_metrics = [m for m in contract_impl.metrics if isinstance(m, DerivedPercentageMetricImpl)]
    assert any(m.type == "invalid_percent" for m in derived_metrics)


# ---------------------------------------------------------------------------
# setup_metrics: row_count metric resolved through metrics_resolver
# ---------------------------------------------------------------------------


def test_row_count_metric_resolved_in_contract_metrics():
    """RowCountMetricImpl should be present in the contract's resolved metrics list."""
    contract_impl = build_contract_impl(ROW_COUNT_CONFIG.yaml)
    resolved_types = [type(m).__name__ for m in contract_impl.metrics]
    assert "RowCountMetricImpl" in resolved_types


# ---------------------------------------------------------------------------
# evaluate: explicit threshold variants
# ---------------------------------------------------------------------------


def test_row_count_explicit_threshold_fails():
    """row_count with must_be_greater_than: 50, row_count=30 → FAILED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - row_count:
          threshold:
            must_be_greater_than: 50
    """
    contract_impl, check = _build_and_get_check(contract_yaml)
    result = _evaluate_check(
        contract_impl,
        check,
        [(check.row_count_metric, 30), (contract_impl.row_count_metric_impl, 30)],
        dataset_rows_tested=30,
    )
    assert result.outcome == CheckOutcome.FAILED


def test_missing_explicit_threshold_passes():
    """missing with must_be_less_than: 10, missing_count=5 → PASSED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
        checks:
          - missing:
              threshold:
                must_be_less_than: 10
    """
    contract_impl, check = _build_and_get_check(contract_yaml)
    result = _evaluate_check(
        contract_impl,
        check,
        [(check.missing_count_metric_impl, 5), (check.row_count_metric_impl, 100)],
    )
    assert result.outcome == CheckOutcome.PASSED


def test_failed_rows_explicit_threshold_passes():
    """failed_rows with must_be_less_than_or_equal: 10, count=8 → PASSED."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - failed_rows:
          expression: "status NOT IN ('active', 'inactive')"
          threshold:
            must_be_less_than_or_equal: 10
    """
    contract_impl, check = _build_and_get_check(contract_yaml)
    result = _evaluate_check(
        contract_impl,
        check,
        [(check.failed_rows_count_metric_impl, 8), (check.check_rows_tested_metric_impl, 100)],
    )
    assert result.outcome == CheckOutcome.PASSED
