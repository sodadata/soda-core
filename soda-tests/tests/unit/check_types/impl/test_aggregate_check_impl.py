"""
Unit tests for AggregateCheckImpl validation-without-execute.

Tests the aggregate check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_aggregate_check_with_sum_validates():
    """Test that aggregate check with sum function validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - aggregate:
          function: sum
          column: amount
          threshold:
            must_be_greater_than: 1000
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_aggregate_check_with_avg_validates():
    """Test that aggregate check with avg function validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - aggregate:
          function: avg
          column: price
          threshold:
            must_be_greater_than_or_equal: 50.0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_aggregate_check_with_min_validates():
    """Test that aggregate check with min function validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - aggregate:
          function: min
          column: rating
          threshold:
            must_be_greater_than_or_equal: 0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_aggregate_check_with_max_validates():
    """Test that aggregate check with max function validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - aggregate:
          function: max
          column: score
          threshold:
            must_be_less_than_or_equal: 100
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_aggregate_check_with_filter_validates():
    """Test that aggregate check with filter clause validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - aggregate:
          function: sum
          column: quantity
          filter: "status = 'completed'"
          threshold:
            must_be_greater_than: 500
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"
