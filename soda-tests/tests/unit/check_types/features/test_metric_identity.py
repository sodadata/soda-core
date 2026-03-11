"""
Unit tests for metric identity in check configurations.

Tests that different metric configurations result in distinct parsed
check objects with different characteristics.
"""

from helpers.yaml_parsing_helpers import parse_contract


def test_different_metric_types_have_different_configs():
    """Test that missing with count vs percent metric have different configurations."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: integer
        checks:
          - missing:
              threshold:
                metric: count
                must_be: 0
          - missing:
              threshold:
                metric: percent
                must_be: 0
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert len(col.check_yamls) == 2

    check_count = col.check_yamls[0]
    check_percent = col.check_yamls[1]

    # Both are missing checks but with different metrics
    assert check_count.type_name == "missing"
    assert check_percent.type_name == "missing"
    assert check_count.metric == "count"
    assert check_percent.metric == "percent"


def test_duplicate_with_different_metrics():
    """Test that duplicate checks with different metrics have different identities."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: email
        data_type: character varying
        checks:
          - duplicate:
              threshold:
                metric: count
          - duplicate:
              threshold:
                metric: percent
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert len(col.check_yamls) == 2

    check_count = col.check_yamls[0]
    check_percent = col.check_yamls[1]

    assert check_count.metric == "count"
    assert check_percent.metric == "percent"


def test_aggregate_with_different_functions():
    """Test that aggregate checks with different functions have different identities."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: amount
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              must_be_greater_than: 1000
          - aggregate:
              function: avg
              must_be_greater_than: 100
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert len(col.check_yamls) == 2

    check_sum = col.check_yamls[0]
    check_avg = col.check_yamls[1]

    assert check_sum.function == "sum"
    assert check_avg.function == "avg"


def test_metric_check_with_different_expressions():
    """Test that metric checks with different expressions are distinct."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: amount
        data_type: numeric
    checks:
      - metric:
          name: revenue_total
          expression: sum(amount)
          threshold:
            must_be_greater_than: 10000
      - metric:
          name: revenue_average
          expression: avg(amount)
          threshold:
            must_be_greater_than: 100
    """
    contract = parse_contract(contract_yaml)

    assert len(contract.checks) == 2

    metric1 = contract.checks[0]
    metric2 = contract.checks[1]

    assert metric1.expression == "sum(amount)"
    assert metric2.expression == "avg(amount)"
    assert metric1.name == "revenue_total"
    assert metric2.name == "revenue_average"


def test_metric_check_expression_vs_query():
    """Test that metric checks distinguish between expression and query."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: test_col
        data_type: integer
    checks:
      - metric:
          name: using_expression
          expression: count(*)
          threshold:
            must_be_greater_than: 0
      - metric:
          name: using_query
          query: SELECT COUNT(*) FROM {dataset}
          threshold:
            must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)

    assert len(contract.checks) == 2

    metric_expr = contract.checks[0]
    metric_query = contract.checks[1]

    # First uses expression
    assert metric_expr.expression is not None
    # Second uses query (expression may be None)
    assert metric_query.query is not None


def test_freshness_with_different_units():
    """Test that freshness checks with different units have different identities."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: created_at
        data_type: timestamp
    checks:
      - freshness:
          column: created_at
          threshold:
            unit: hour
            must_be_less_than: 24
      - freshness:
          column: created_at
          threshold:
            unit: day
            must_be_less_than: 7
    """
    contract = parse_contract(contract_yaml)

    assert len(contract.checks) == 2

    check_hour = contract.checks[0]
    check_day = contract.checks[1]

    assert check_hour.unit == "hour"
    assert check_day.unit == "day"


def test_invalid_with_different_validation_types():
    """Test that invalid checks with different validation types are distinct."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        checks:
          - invalid:
              name: valid status values
              valid_values:
                - active
                - inactive
          - invalid:
              name: valid status format
              valid_format:
                regex: '^[a-z]+$'
                name: status_regex
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert len(col.check_yamls) == 2

    check_values = col.check_yamls[0]
    check_format = col.check_yamls[1]

    # Both are invalid but with different validation methods
    assert check_values.type_name == "invalid"
    assert check_format.type_name == "invalid"
    assert check_values.name == "valid status values"
    assert check_format.name == "valid status format"


def test_check_with_column_expression_differences():
    """Test that checks with different column expressions are distinct."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: price
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              column_expression: CAST(price AS INTEGER)
              must_be_greater_than: 0
          - aggregate:
              function: sum
              column_expression: CAST(price AS FLOAT)
              must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert len(col.check_yamls) == 2

    check_int = col.check_yamls[0]
    check_float = col.check_yamls[1]

    assert check_int.column_expression == "CAST(price AS INTEGER)"
    assert check_float.column_expression == "CAST(price AS FLOAT)"
