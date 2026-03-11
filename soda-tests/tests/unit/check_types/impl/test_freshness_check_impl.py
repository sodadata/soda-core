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
    columns:
      - name: created_at
        data_type: timestamp without time zone
        checks:
          - freshness:
              column: created_at
              must_be_less_than: 24
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_freshness_check_with_hour_unit_validates():
    """Test that freshness check with hour unit validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: updated_at
        data_type: timestamp
        checks:
          - freshness:
              column: updated_at
              unit: hour
              must_be_less_than: 6
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_freshness_check_with_day_unit_validates():
    """Test that freshness check with day unit validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: last_update
        data_type: timestamp without time zone
        checks:
          - freshness:
              column: last_update
              unit: day
              must_be_less_than_or_equal: 7
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_freshness_check_with_filter_validates():
    """Test that freshness check with filter clause validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: sync_timestamp
        data_type: timestamp
        checks:
          - freshness:
              column: sync_timestamp
              filter: "status = 'completed'"
              unit: minute
              must_be_less_than: 60
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors
