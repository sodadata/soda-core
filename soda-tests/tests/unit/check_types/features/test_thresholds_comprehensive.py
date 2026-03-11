"""
Unit tests for threshold behavior complementary to test_thresholds.py.

This module tests aspects of thresholds NOT covered in test_thresholds.py,
including float thresholds, None value handling, default thresholds,
metric name computation, and outcome levels.
"""

import pytest
from helpers.yaml_parsing_helpers import parse_check_from_contract, parse_column_check
from soda_core.contracts.impl.contract_verification_impl import (
    ThresholdImpl,
    ThresholdLevel,
    ThresholdType,
)
from soda_core.contracts.impl.contract_yaml import ThresholdYaml


def test_threshold_with_float_values():
    """Test that thresholds work correctly with float values."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: score
        data_type: numeric
        checks:
          - aggregate:
              function: avg
              threshold:
                must_be_between:
                  greater_than: 75.5
                  less_than: 99.99
    """
    check = parse_column_check(contract_yaml)

    # Should parse float values in threshold YAML
    assert check.threshold is not None
    assert isinstance(check.threshold, ThresholdYaml)
    assert check.threshold.must_be_between is not None
    assert check.threshold.must_be_between.greater_than == 75.5
    assert check.threshold.must_be_between.less_than == 99.99


def test_threshold_passes_method_with_none():
    """Test that threshold.passes() raises TypeError with None values (expected behavior)."""
    threshold = ThresholdImpl(
        type=ThresholdType.SINGLE_COMPARATOR,
        must_be_greater_than=0,
    )

    # None values should raise TypeError when compared
    # This is the current behavior - passes() expects numeric values
    with pytest.raises(TypeError):
        threshold.passes(None)


def test_threshold_with_zero_boundary():
    """Test that thresholds correctly handle zero as a boundary value."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: balance
        data_type: numeric
        checks:
          - aggregate:
              function: min
              threshold:
                must_be_greater_than_or_equal: 0.0
    """
    check = parse_column_check(contract_yaml)

    assert check.threshold is not None
    assert check.threshold.must_be_greater_than_or_equal == 0.0


def test_threshold_get_metric_name_without_column():
    """Test that ThresholdImpl.get_metric_name works without column context."""
    # get_metric_name should work with None column_impl
    metric_name = ThresholdImpl.get_metric_name("missing_count", column_impl=None)
    assert metric_name == "missing_count"


def test_threshold_warn_level():
    """Test that thresholds can have warn level instead of fail."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        checks:
          - missing:
              threshold:
                must_be_less_than: 10
                level: warn
    """
    check = parse_column_check(contract_yaml)

    assert check.threshold is not None
    # Threshold YAML should be marked as warn level
    assert check.threshold.level == "warn"


def test_threshold_default_level_is_fail():
    """Test that default threshold level is 'fail'."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: col1
        data_type: integer
    checks:
      - row_count:
          threshold:
            must_be_greater_than: 0
    """
    check = parse_check_from_contract(contract_yaml)

    # Default should be fail
    assert check.threshold is not None
    assert check.threshold.level == "fail"


def test_threshold_boundary_values_inclusive():
    """Test that boundary comparisons are inclusive/exclusive as expected."""
    # Test must_be_greater_than_or_equal (inclusive)
    threshold_gte = ThresholdImpl(
        type=ThresholdType.SINGLE_COMPARATOR,
        must_be_greater_than_or_equal=10,
    )
    assert threshold_gte.passes(10)  # Inclusive
    assert threshold_gte.passes(11)
    assert not threshold_gte.passes(9)

    # Test must_be_greater_than (exclusive)
    threshold_gt = ThresholdImpl(
        type=ThresholdType.SINGLE_COMPARATOR,
        must_be_greater_than=10,
    )
    assert not threshold_gt.passes(10)  # Exclusive
    assert threshold_gt.passes(11)
    assert not threshold_gt.passes(9)


def test_threshold_to_threshold_info():
    """Test that ThresholdImpl converts to Threshold info correctly."""
    threshold = ThresholdImpl(
        type=ThresholdType.SINGLE_COMPARATOR,
        level=ThresholdLevel.WARN,
        must_be_greater_than=100,
    )

    threshold_info = threshold.to_threshold_info()
    assert threshold_info.must_be_greater_than == 100
    assert threshold_info.level == "warn"


def test_threshold_with_negative_values():
    """Test that thresholds work correctly with negative values."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: temperature
        data_type: numeric
        checks:
          - aggregate:
              function: min
              threshold:
                must_be_greater_than: -273.15
    """
    check = parse_column_check(contract_yaml)

    assert check.threshold is not None
    assert check.threshold.must_be_greater_than == -273.15


def test_threshold_range_with_negative_boundaries():
    """Test range thresholds with negative boundaries."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: deviation
        data_type: numeric
        checks:
          - aggregate:
              function: avg
              threshold:
                must_be_between:
                  greater_than: -10
                  less_than: 10
    """
    check = parse_column_check(contract_yaml)

    assert check.threshold is not None
    # ThresholdYaml has must_be_between attribute (RangeYaml)
    assert check.threshold.must_be_between is not None
    assert check.threshold.must_be_between.greater_than == -10
    assert check.threshold.must_be_between.less_than == 10


def test_threshold_string_representation():
    """Test that threshold assertion summary formats correctly."""
    threshold = ThresholdImpl(
        type=ThresholdType.SINGLE_COMPARATOR,
        must_be=42,
    )
    summary = threshold.get_assertion_summary("answer")
    assert "answer" in summary
    assert "42" in summary
