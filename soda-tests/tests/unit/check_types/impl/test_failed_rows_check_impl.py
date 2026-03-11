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
    columns: []
    checks:
      - failed_rows:
          expression: "status NOT IN ('active', 'inactive')"
          threshold:
            must_be_less_than_or_equal: 100
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_failed_rows_check_with_check_reference_validates():
    """Test that failed_rows check with query validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - failed_rows:
          query: |
            SELECT * FROM {dataset}
            WHERE user_id IS NULL
          threshold:
            must_be_less_than_or_equal: 0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"
