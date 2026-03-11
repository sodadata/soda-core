"""
Unit tests for MissingCheckImpl validation-without-execute.

Tests the missing check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_missing_check_validates_without_execute():
    """Test that a contract with a missing check validates successfully in dry-run mode."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
        checks:
          - missing:
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_missing_check_with_count_metric_validates():
    """Test that missing check with count metric validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: integer
        checks:
          - missing:
              metric: count
              must_be: 0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_missing_check_with_percent_metric_validates():
    """Test that missing check with percent metric validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: email
        data_type: character varying
        checks:
          - missing:
              metric: percent
              must_be_less_than: 5.0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_missing_check_with_filter_validates():
    """Test that missing check with filter clause validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: address
        data_type: character varying
        checks:
          - missing:
              filter: country = 'USA'
              must_be_less_than_or_equal: 10
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors
