"""
Unit tests for metric identity — distinct configs produce distinct check objects.

These tests verify that the parser keeps checks with different configurations
as separate objects. Field-value parsing (e.g. function == "sum") is tested
in yaml_parsing/; here we only assert distinctness between paired checks.
"""

from helpers.yaml_parsing_helpers import parse_contract


def test_missing_count_vs_percent_are_distinct():
    """Missing checks with count vs percent metric are distinct objects."""
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
              qualifier: percent
              threshold:
                metric: percent
                must_be: 0
    """
    contract = parse_contract(contract_yaml)
    col = contract.columns[0]
    assert len(col.check_yamls) == 2
    assert col.check_yamls[0].metric != col.check_yamls[1].metric


def test_duplicate_count_vs_percent_are_distinct():
    """Duplicate checks with count vs percent metric are distinct objects."""
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
              qualifier: percent
              threshold:
                metric: percent
    """
    contract = parse_contract(contract_yaml)
    col = contract.columns[0]
    assert len(col.check_yamls) == 2
    assert col.check_yamls[0].metric != col.check_yamls[1].metric


def test_aggregate_different_functions_are_distinct():
    """Aggregate checks with different functions are distinct objects."""
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
              qualifier: avg
              function: avg
              must_be_greater_than: 100
    """
    contract = parse_contract(contract_yaml)
    col = contract.columns[0]
    assert len(col.check_yamls) == 2
    assert col.check_yamls[0].function != col.check_yamls[1].function


def test_metric_different_expressions_are_distinct():
    """Metric checks with different expressions are distinct objects."""
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
          qualifier: average
          expression: avg(amount)
          threshold:
            must_be_greater_than: 100
    """
    contract = parse_contract(contract_yaml)
    assert len(contract.checks) == 2
    assert contract.checks[0].expression != contract.checks[1].expression
    assert contract.checks[0].name != contract.checks[1].name


def test_metric_expression_vs_query_are_distinct():
    """Metric checks using expression vs query are structurally different."""
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
          qualifier: query
          query: SELECT COUNT(*) FROM {dataset}
          threshold:
            must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)
    assert len(contract.checks) == 2
    metric_expr = contract.checks[0]
    metric_query = contract.checks[1]
    assert metric_expr.expression is not None
    assert metric_query.query is not None


def test_freshness_different_units_are_distinct():
    """Freshness checks with different units are distinct objects."""
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
          qualifier: daily
          column: created_at
          threshold:
            unit: day
            must_be_less_than: 7
    """
    contract = parse_contract(contract_yaml)
    assert len(contract.checks) == 2
    assert contract.checks[0].unit != contract.checks[1].unit


def test_invalid_values_vs_format_are_distinct():
    """Invalid checks with values vs format validation are distinct objects."""
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
              qualifier: format
              valid_format:
                regex: '^[a-z]+$'
                name: status_regex
    """
    contract = parse_contract(contract_yaml)
    col = contract.columns[0]
    assert len(col.check_yamls) == 2
    assert col.check_yamls[0].name != col.check_yamls[1].name


def test_column_expression_differences_are_distinct():
    """Checks with different column expressions are distinct objects."""
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
              qualifier: float
              function: sum
              column_expression: CAST(price AS FLOAT)
              must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)
    col = contract.columns[0]
    assert len(col.check_yamls) == 2
    assert col.check_yamls[0].column_expression != col.check_yamls[1].column_expression
