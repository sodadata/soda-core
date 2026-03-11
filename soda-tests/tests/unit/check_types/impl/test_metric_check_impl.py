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
    columns:
      - name: id
        data_type: integer
    checks:
      - metric:
          name: total_revenue
          expression: sum(amount)
          must_be_greater_than: 10000
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_metric_check_with_query_validates():
    """Test that metric check with query validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - metric:
          name: unique_customers
          query: |
            SELECT COUNT(DISTINCT customer_id) FROM {dataset}
          must_be_greater_than: 100
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_metric_check_with_threshold_between_validates():
    """Test that metric check with between threshold validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
    checks:
      - metric:
          name: average_order_value
          expression: avg(order_amount)
          must_be_between:
            greater_than: 50
            less_than: 200
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_metric_check_with_column_expression_validates():
    """Test that metric check works with column expressions."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: price
        data_type: numeric
        column_expression: CAST(price AS DECIMAL(10,2))
        checks:
          - metric:
              name: price_sum
              expression: sum(price)
              must_be_greater_than: 0
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors
