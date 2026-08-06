"""
Tests for checks whose threshold is expressed with fail:/warn: severity blocks.

Uses the same harness as test_threshold_checks.py: build_contract_impl() gives a
real ContractImpl (no database), and build_measurement_values() feeds evaluate().
"""

from __future__ import annotations

from helpers.impl_test_helpers import build_contract_impl, build_measurement_values
from soda_core.contracts.contract_verification import CheckOutcome
from soda_core.contracts.impl.check_types.row_count_check import RowCountCheckImpl
from soda_core.contracts.impl.contract_verification_impl import ContractImpl

CONTRACT_YAML = """
dataset: my_data_source/my_dataset
columns:
  - name: id
    data_type: integer
checks:
  - row_count:
      threshold:
        fail:
          must_be_greater_than: 10
        warn:
          must_be_greater_than: 100
"""


def build_row_count_check(contract_yaml: str) -> tuple[ContractImpl, RowCountCheckImpl]:
    contract_impl = build_contract_impl(contract_yaml)
    check = contract_impl.all_check_impls[0]
    assert isinstance(check, RowCountCheckImpl)
    return contract_impl, check


def evaluate_row_count(row_count: int) -> CheckOutcome:
    contract_impl, check = build_row_count_check(CONTRACT_YAML)
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


def test_warn_threshold_attribute_set():
    _, check = build_row_count_check(CONTRACT_YAML)
    assert check.threshold.level.value == "fail"
    assert check.warn_threshold is not None
    assert check.warn_threshold.level.value == "warn"


def test_no_threshold_key_keeps_default_and_no_warn():
    _, check = build_row_count_check(
        """
        dataset: my_data_source/my_dataset
        columns:
          - name: id
            data_type: integer
        checks:
          - row_count:
        """
    )
    assert check.threshold.must_be_greater_than == 0
    assert check.warn_threshold is None
