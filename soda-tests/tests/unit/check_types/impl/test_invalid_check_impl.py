"""
Unit tests for InvalidCheckImpl validation-without-execute.

Tests the invalid check in isolation during contract validation,
without needing a real database connection.

Regression test for the invalid reference check bug that was fixed
in test_validate_without_execute.py.
"""

from helpers.impl_test_helpers import validate_contract


def test_invalid_check_with_valid_values_validates():
    """Test that invalid check with valid_values validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        checks:
          - invalid:
              valid_values:
                - active
                - inactive
                - pending
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_invalid_check_with_valid_reference_data_validates():
    """Test that invalid check with valid_reference_data validates (regression test)."""
    contract_yaml = """
    dataset: datasource/db/dataset/ecommerce_orders
    columns:
      - name: email
        data_type: character varying
        checks:
          - invalid:
              name: email exists in customer_info
              qualifier: x7p2k
              valid_reference_data:
                column: email
                dataset: datasource/customers/production/customer_info
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    # This was the original bug - should not crash with sql_dialect error
    assert errors is None or "sql_dialect" not in errors


def test_invalid_check_with_filter_validates():
    """Test that invalid check with filter clause validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: country_code
        data_type: character varying
        checks:
          - invalid:
              filter: country_code IS NOT NULL
              valid_values:
                - US
                - CA
                - MX
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors
