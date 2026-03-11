"""
Unit tests for FreshnessCheckImpl validation-without-execute.

Tests the freshness check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_freshness_check_validates_without_execute():
    """Test that a contract with a freshness check validates successfully in dry-run mode."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - freshness:
          column: created_at
          threshold:
            unit: hour
            must_be_less_than: 24
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_freshness_check_with_hour_unit_validates():
    """Test that freshness check with hour unit validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - freshness:
          column: updated_at
          threshold:
            unit: hour
            must_be_less_than: 6
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_freshness_check_with_day_unit_validates():
    """Test that freshness check with day unit validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - freshness:
          column: last_update
          threshold:
            unit: day
            must_be_less_than_or_equal: 7
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_freshness_check_with_filter_validates():
    """Test that freshness check with filter clause validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - freshness:
          column: sync_timestamp
          filter: "status = 'completed'"
          threshold:
            unit: minute
            must_be_less_than: 60
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"
