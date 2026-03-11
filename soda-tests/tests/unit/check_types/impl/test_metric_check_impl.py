"""
Unit tests for MetricCheckImpl validation-without-execute.

Tests the metric check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_metric_check_with_expression_validates():
    """Test that metric check with expression validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          expression: sum(amount)
          threshold:
            must_be_greater_than: 10000
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_metric_check_with_query_validates():
    """Test that metric check with query validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          query: |
            SELECT COUNT(DISTINCT customer_id) FROM {dataset}
          threshold:
            must_be_greater_than: 100
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_metric_check_with_threshold_between_validates():
    """Test that metric check with between threshold validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          expression: avg(order_amount)
          threshold:
            must_be_between:
              greater_than: 50
              less_than: 200
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"


def test_metric_check_with_column_expression_validates():
    """Test that metric check works with expressions."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          expression: sum(price)
          threshold:
            must_be_greater_than: 0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    assert not result.has_errors, f"Unexpected errors: {result.get_errors_str()}"
