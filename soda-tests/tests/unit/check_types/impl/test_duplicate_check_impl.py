"""
Unit tests for DuplicateCheckImpl validation-without-execute.

Tests the duplicate check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_duplicate_check_validates_without_execute():
    """Test that a contract with a duplicate check validates successfully in dry-run mode."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: order_id
        data_type: integer
        checks:
          - duplicate:
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_duplicate_check_with_count_metric_validates():
    """Test that duplicate check with count metric validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: email
        data_type: character varying
        checks:
          - duplicate:
              metric: count
              threshold:
                must_be: 0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_duplicate_check_with_percent_metric_validates():
    """Test that duplicate check with percent metric validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: product_id
        data_type: integer
        checks:
          - duplicate:
              metric: percent
              threshold:
                must_be_less_than: 2.0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_multi_column_duplicate_check_validates():
    """Test that multi-column duplicate check validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: integer
      - name: date
        data_type: date
    checks:
      - duplicate:
          columns:
            - user_id
            - date
          threshold:
            must_be: 0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"
