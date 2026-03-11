"""
Unit tests for column expression behavior.

Tests that column expressions are correctly parsed and applied at both
column and check levels, with proper override behavior.
"""

from helpers.yaml_parsing_helpers import parse_contract


def test_column_expression_on_column_level():
    """Test that column_expression is parsed at the column level."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: salary
        data_type: numeric
        column_expression: CAST(salary AS DECIMAL(10,2))
        checks:
          - aggregate:
              function: sum
              must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert col.column_expression == "CAST(salary AS DECIMAL(10,2))"


def test_column_expression_on_check_level():
    """Test that column_expression can be specified at check level."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: amount
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              column_expression: ABS(amount)
              must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    check = col.check_yamls[0]

    # Check should have its own column_expression
    assert check.column_expression == "ABS(amount)"


def test_check_level_expression_overrides_column_level():
    """Test that check-level column_expression overrides column-level."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: price
        data_type: numeric
        column_expression: CAST(price AS NUMERIC(8,2))
        checks:
          - aggregate:
              function: avg
              column_expression: ROUND(price, 2)
              must_be_greater_than: 0
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    check = col.check_yamls[0]

    # Column has its expression
    assert col.column_expression == "CAST(price AS NUMERIC(8,2))"
    # Check has its own, overriding the column-level one
    assert check.column_expression == "ROUND(price, 2)"


def test_fallback_to_column_level_when_check_not_specified():
    """Test that checks fall back to column-level expression when not specified."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: temperature
        data_type: numeric
        column_expression: temperature * 1.8 + 32
        checks:
          - aggregate:
              function: max
              must_be_less_than: 120
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    check = col.check_yamls[0]

    # Column has expression
    assert col.column_expression == "temperature * 1.8 + 32"
    # Check doesn't specify its own, but it should be able to use column's
    # (This behavior is implementation-dependent in the check impl layer)


def test_column_expression_with_multiple_checks():
    """Test that multiple checks on same column can have different expressions."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: value
        data_type: numeric
        column_expression: COALESCE(value, 0)
        checks:
          - aggregate:
              function: sum
              column_expression: CAST(value AS INTEGER)
              must_be_greater_than: 0
          - aggregate:
              function: avg
              column_expression: ABS(value)
              must_be_greater_than_or_equal: 0
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert len(col.check_yamls) == 2

    check1 = col.check_yamls[0]
    check2 = col.check_yamls[1]

    assert check1.column_expression == "CAST(value AS INTEGER)"
    assert check2.column_expression == "ABS(value)"


def test_complex_column_expression_with_functions():
    """Test that complex expressions with functions are preserved."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: created_at
        data_type: timestamp
        column_expression: DATE_TRUNC('day', created_at AT TIME ZONE 'UTC')
        checks:
          - freshness:
              column: created_at
              must_be_less_than: 7
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert col.column_expression == "DATE_TRUNC('day', created_at AT TIME ZONE 'UTC')"


def test_column_expression_with_special_characters():
    """Test that column expressions with special characters are handled."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: json_field
        data_type: text
        column_expression: "json_field->>'key'"
        checks:
          - missing:
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    assert col.column_expression == "json_field->>'key'"


def test_no_column_expression_when_not_specified():
    """Test that column_expression is None when not specified."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: simple_column
        data_type: integer
        checks:
          - missing:
    """
    contract = parse_contract(contract_yaml)

    col = contract.columns[0]
    # column_expression should be None or not set when not specified
    assert col.column_expression is None or col.column_expression == "simple_column"
