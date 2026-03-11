"""
Unit tests for RowCountCheckImpl validation-without-execute.

Tests the row count check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_row_count_check_validates_without_execute():
    """Test that a contract with a row_count check validates successfully in dry-run mode."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - row_count:
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_row_count_check_with_explicit_threshold_validates():
    """Test that row_count with explicit threshold validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - row_count:
          threshold:
            must_be_greater_than: 100
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_row_count_check_with_filter_validates():
    """Test that row_count with filter clause validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
    checks:
      - row_count:
          filter: status = 'active'
          threshold:
            must_be_greater_than_or_equal: 50
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"
