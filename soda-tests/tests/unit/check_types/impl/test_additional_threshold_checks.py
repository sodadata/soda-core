"""
Tests for checks whose threshold carries a nested `additional:` threshold.

Uses the same harness as test_threshold_checks.py: build_contract_impl() gives a
real ContractImpl (no database), and build_measurement_values() feeds evaluate().
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from helpers.impl_test_helpers import build_contract_impl, build_measurement_values
from soda_core.contracts.contract_verification import CheckOutcome
from soda_core.contracts.impl.check_types.freshness_check import FreshnessCheckImpl
from soda_core.contracts.impl.check_types.row_count_check import RowCountCheckImpl
from soda_core.contracts.impl.contract_verification_impl import ContractImpl

# fail when row_count <= 10, warn when row_count <= 100
FAIL_OUTER_CONTRACT_YAML = """
dataset: my_data_source/my_dataset
columns:
  - name: id
    data_type: integer
checks:
  - row_count:
      threshold:
        must_be_greater_than: 10
        additional:
          must_be_greater_than: 100
          level: warn
"""

# Same zones, levels the other way around: the outer threshold is the warn one
# and the `additional` one is the fail one.
WARN_OUTER_CONTRACT_YAML = """
dataset: my_data_source/my_dataset
columns:
  - name: id
    data_type: integer
checks:
  - row_count:
      threshold:
        must_be_greater_than: 100
        level: warn
        additional:
          must_be_greater_than: 10
"""


def build_row_count_check(contract_yaml: str) -> tuple[ContractImpl, RowCountCheckImpl]:
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, RowCountCheckImpl)
    return contract_impl, check


def evaluate_row_count(row_count: int, contract_yaml: str = FAIL_OUTER_CONTRACT_YAML) -> CheckOutcome:
    contract_impl, check = build_row_count_check(contract_yaml)
    measurement_values = build_measurement_values(
        [(check.row_count_metric, row_count), (contract_impl.row_count_metric_impl, row_count)],
        contract_impl=contract_impl,
    )
    return check.evaluate(measurement_values).outcome


def test_both_pass():
    assert evaluate_row_count(150) == CheckOutcome.PASSED


def test_warn_zone():
    assert evaluate_row_count(50) == CheckOutcome.WARN


def test_fail_wins():
    assert evaluate_row_count(5) == CheckOutcome.FAILED


def test_warn_outer_both_pass():
    assert evaluate_row_count(150, WARN_OUTER_CONTRACT_YAML) == CheckOutcome.PASSED


def test_warn_outer_warn_zone():
    assert evaluate_row_count(50, WARN_OUTER_CONTRACT_YAML) == CheckOutcome.WARN


def test_warn_outer_fail_wins():
    assert evaluate_row_count(5, WARN_OUTER_CONTRACT_YAML) == CheckOutcome.FAILED


def test_warn_threshold_attribute_set():
    _, check = build_row_count_check(FAIL_OUTER_CONTRACT_YAML)
    assert check.threshold.level.value == "fail"
    assert check.threshold.must_be_greater_than == 10
    assert check.warn_threshold is not None
    assert check.warn_threshold.level.value == "warn"
    assert check.warn_threshold.must_be_greater_than == 100


def test_warn_outer_threshold_attributes_assigned_by_level():
    _, check = build_row_count_check(WARN_OUTER_CONTRACT_YAML)
    assert check.threshold.level.value == "fail"
    assert check.threshold.must_be_greater_than == 10
    assert check.warn_threshold is not None
    assert check.warn_threshold.level.value == "warn"
    assert check.warn_threshold.must_be_greater_than == 100


def test_warn_only_threshold_keeps_primary_slot_and_no_warn_threshold():
    _, check = build_row_count_check("""
        dataset: my_data_source/my_dataset
        columns:
          - name: id
            data_type: integer
        checks:
          - row_count:
              threshold:
                must_be_greater_than: 10
                level: warn
        """)
    assert check.threshold.level.value == "warn"
    assert check.threshold.must_be_greater_than == 10
    assert check.warn_threshold is None


def test_two_outer_comparisons_with_additional_is_rejected(caplog):
    """The outer arity error is what keeps this check from ever running.

    Its impl has no fail threshold (and evaluates NOT_EVALUATED forever) while a warn
    threshold is still uploaded — so the error must be an error, not a warning.
    """
    contract_impl = build_contract_impl("""
        dataset: my_data_source/my_dataset
        columns:
          - name: id
            data_type: integer
        checks:
          - row_count:
              threshold:
                must_be_greater_than: 10
                must_be_less_than: 1000
                additional:
                  must_be_greater_than: 100
                  level: warn
        """)
    assert "must specify exactly one comparison itself" in caplog.text
    check = contract_impl.all_check_impls[0]
    assert check.threshold is None
    measurement_values = build_measurement_values(
        [(check.row_count_metric, 50), (contract_impl.row_count_metric_impl, 50)],
        contract_impl=contract_impl,
    )
    assert check.evaluate(measurement_values).outcome == CheckOutcome.NOT_EVALUATED


def test_no_threshold_key_keeps_default_and_no_warn():
    _, check = build_row_count_check("""
        dataset: my_data_source/my_dataset
        columns:
          - name: id
            data_type: integer
        checks:
          - row_count:
        """)
    assert check.threshold.must_be_greater_than == 0
    assert check.warn_threshold is None


# ---------------------------------------------------------------------------
# Freshness: the one check type with its own threshold shape (unit) and its own
# schema def for `additional`. Both thresholds compare in the outer unit.
# ---------------------------------------------------------------------------

# fail when older than 3 days, warn when older than 1 day
FRESHNESS_CONTRACT_YAML = """
dataset: my_data_source/my_dataset
columns: []
checks:
  - freshness:
      column: created_at
      threshold:
        unit: day
        must_be_less_than: 3
        additional:
          must_be_less_than: 1
          level: warn
"""


def build_freshness_check(contract_yaml: str = FRESHNESS_CONTRACT_YAML) -> tuple[ContractImpl, FreshnessCheckImpl]:
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, FreshnessCheckImpl)
    return contract_impl, check


def evaluate_freshness(age: timedelta, contract_yaml: str = FRESHNESS_CONTRACT_YAML) -> CheckOutcome:
    contract_impl, check = build_freshness_check(contract_yaml)
    data_timestamp = check.contract_impl.yaml.data_timestamp or datetime.now(tz=timezone.utc)
    contract_impl.dataset_rows_tested = 100
    measurement_values = build_measurement_values(
        [
            (check.max_timestamp_metric, data_timestamp - age),
            (check.check_rows_tested_metric_impl, 100),
        ],
        contract_impl=contract_impl,
    )
    return check.evaluate(measurement_values).outcome


def test_freshness_additional_parsed():
    _, check = build_freshness_check()
    assert check.check_yaml.unit == "day"
    assert check.check_yaml.threshold.must_be_less_than == 3
    assert check.check_yaml.threshold.additional.must_be_less_than == 1
    assert check.check_yaml.threshold.additional.level == "warn"


def test_freshness_additional_fills_the_warn_slot():
    _, check = build_freshness_check()
    assert check.threshold.level.value == "fail"
    assert check.threshold.must_be_less_than == 3
    assert check.warn_threshold is not None
    assert check.warn_threshold.level.value == "warn"
    assert check.warn_threshold.must_be_less_than == 1


def test_freshness_additional_both_pass():
    assert evaluate_freshness(timedelta(hours=12)) == CheckOutcome.PASSED


def test_freshness_additional_warn_zone():
    # Older than the warn threshold (1 day), still within the fail threshold (3 days).
    assert evaluate_freshness(timedelta(days=2)) == CheckOutcome.WARN


def test_freshness_additional_fail_wins():
    assert evaluate_freshness(timedelta(days=5)) == CheckOutcome.FAILED
