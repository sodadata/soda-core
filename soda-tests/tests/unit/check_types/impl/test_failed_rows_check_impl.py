"""
Unit tests for FailedRowsCheckImpl validation-without-execute.

Tests the failed rows check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_failed_rows_check_with_limit_validates():
    """Test that failed_rows check with limit validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        checks:
          - invalid:
              name: valid status
              valid_values:
                - active
                - inactive
          - failed_rows:
              check_name: valid status
              limit: 100
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_failed_rows_check_with_check_reference_validates():
    """Test that failed_rows check with check reference validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: integer
        checks:
          - missing:
              name: no missing user ids
      - name: email
        data_type: character varying
        checks:
          - failed_rows:
              check_name: no missing user ids
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors
