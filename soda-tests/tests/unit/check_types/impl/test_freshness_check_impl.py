"""
Unit tests for FreshnessCheckImpl — unique logic only.

Freshness has domain-specific behaviour that differs from generic threshold
checks:
- Computes timedelta between max_timestamp and data_timestamp
- Converts to a configurable unit (second, minute, hour, day)
- Returns FAILED (not NOT_EVALUATED) when max_timestamp is None
- Returns a FreshnessCheckResult with freshness-specific fields

Generic threshold pass/fail/warn/NOT_EVALUATED patterns are covered in
test_threshold_checks.py.
"""

from datetime import datetime, timedelta, timezone

from helpers.impl_test_helpers import build_contract_impl, build_measurement_values
from soda_core.contracts.contract_verification import CheckOutcome
from soda_core.contracts.impl.check_types.freshness_check import (
    FreshnessCheckImpl,
    FreshnessCheckResult,
)


def _freshness_yaml(unit: str = "hour", threshold: int = 24) -> str:
    return f"""
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - freshness:
          column: created_at
          threshold:
            unit: {unit}
            must_be_less_than: {threshold}
    """


def _build_freshness_check(yaml_str: str):
    contract_impl = build_contract_impl(yaml_str)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, FreshnessCheckImpl)
    return contract_impl, check


def _evaluate_freshness(contract_impl, check, max_ts, rows=100):
    mv = build_measurement_values(
        [(check.max_timestamp_metric, max_ts), (check.check_rows_tested_metric_impl, rows)],
        contract_impl=contract_impl,
    )
    contract_impl.dataset_rows_tested = rows
    return check.evaluate(mv)


def _data_timestamp(check: FreshnessCheckImpl) -> datetime:
    return check.contract_impl.contract_yaml.data_timestamp or datetime.now(tz=timezone.utc)


def test_freshness_passes_when_within_threshold():
    """max_timestamp 1 hour before data_timestamp, threshold < 24 hours → PASSED."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="hour", threshold=24))
    max_ts = _data_timestamp(check) - timedelta(hours=1)
    result = _evaluate_freshness(contract_impl, check, max_ts)
    assert result.outcome == CheckOutcome.PASSED


def test_freshness_fails_when_beyond_threshold():
    """max_timestamp 48 hours before data_timestamp, threshold < 24 hours → FAILED."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="hour", threshold=24))
    max_ts = _data_timestamp(check) - timedelta(hours=48)
    result = _evaluate_freshness(contract_impl, check, max_ts)
    assert result.outcome == CheckOutcome.FAILED


def test_freshness_fails_when_no_timestamp():
    """max_timestamp=None → FAILED (no data means freshness can't be established)."""
    contract_impl, check = _build_freshness_check(_freshness_yaml())
    result = _evaluate_freshness(contract_impl, check, max_ts=None, rows=0)
    assert result.outcome == CheckOutcome.FAILED
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 0,
        "check_rows_tested": 0,
    }


def test_freshness_result_type_and_fields():
    """evaluate() should return FreshnessCheckResult with freshness details."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="day", threshold=7))
    max_ts = _data_timestamp(check) - timedelta(days=2)
    result = _evaluate_freshness(contract_impl, check, max_ts, rows=50)

    assert isinstance(result, FreshnessCheckResult)
    assert result.unit == "day"
    assert result.freshness is not None
    assert result.freshness_in_seconds is not None
    assert result.outcome == CheckOutcome.PASSED
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 50,
        "check_rows_tested": 50,
        "freshness_in_days": result.threshold_value,
    }


def test_freshness_unit_second():
    """Freshness with unit=second, 30 seconds old, threshold < 60 → PASSED.
    Note: 'second' is not a supported unit; the parser falls back to 'hour',
    so the diagnostic key is freshness_in_hours."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="second", threshold=60))
    max_ts = _data_timestamp(check) - timedelta(seconds=30)
    result = _evaluate_freshness(contract_impl, check, max_ts)
    assert result.outcome == CheckOutcome.PASSED
    assert result.threshold_value is not None
    assert result.threshold_value < 60
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
        "freshness_in_hours": result.threshold_value,
    }


def test_freshness_unit_minute():
    """Freshness with unit=minute, 5 minutes old, threshold < 10 → PASSED."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="minute", threshold=10))
    max_ts = _data_timestamp(check) - timedelta(minutes=5)
    result = _evaluate_freshness(contract_impl, check, max_ts)
    assert result.outcome == CheckOutcome.PASSED
    assert result.threshold_value is not None
    assert result.threshold_value < 10
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
        "freshness_in_minutes": result.threshold_value,
    }


def test_freshness_unit_minute_fails():
    """Freshness with unit=minute, 15 minutes old, threshold < 10 → FAILED."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="minute", threshold=10))
    max_ts = _data_timestamp(check) - timedelta(minutes=15)
    result = _evaluate_freshness(contract_impl, check, max_ts)
    assert result.outcome == CheckOutcome.FAILED


def test_freshness_unit_day():
    """Freshness with unit=day, 3 days old, threshold < 7 → PASSED."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="day", threshold=7))
    max_ts = _data_timestamp(check) - timedelta(days=3)
    result = _evaluate_freshness(contract_impl, check, max_ts)
    assert result.outcome == CheckOutcome.PASSED
    assert result.threshold_value is not None
    assert result.threshold_value < 7
    assert result.diagnostic_metric_values == {
        "dataset_rows_tested": 100,
        "check_rows_tested": 100,
        "freshness_in_days": result.threshold_value,
    }


def test_freshness_unit_day_fails():
    """Freshness with unit=day, 10 days old, threshold < 7 → FAILED."""
    contract_impl, check = _build_freshness_check(_freshness_yaml(unit="day", threshold=7))
    max_ts = _data_timestamp(check) - timedelta(days=10)
    result = _evaluate_freshness(contract_impl, check, max_ts)
    assert result.outcome == CheckOutcome.FAILED
