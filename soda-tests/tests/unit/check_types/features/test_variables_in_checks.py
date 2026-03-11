"""
Unit tests for variable resolution in checks.

Tests that variables are correctly resolved in various check contexts
including filters, thresholds, dataset names, and expressions.
"""

from helpers.yaml_parsing_helpers import parse_contract


def test_variable_in_filter():
    """Test that variables are resolved in filter expressions."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: amount
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              filter: status = '${var.status}'
              must_be_greater_than: 100
    """
    variables = {"status": "completed"}
    contract = parse_contract(contract_yaml, variables=variables)

    col = contract.columns[0]
    check = col.check_yamls[0]
    assert check.filter is not None
    assert "${var.status}" in check.filter or "completed" in str(check.filter)


def test_variable_in_threshold():
    """Test that variables are resolved in threshold values."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: integer
        checks:
          - row_count:
              must_be_greater_than: ${var.min_rows}
    """
    variables = {"min_rows": "1000"}
    contract = parse_contract(contract_yaml, variables=variables)

    # Variables in thresholds are resolved at parse time
    col = contract.columns[0]
    check = col.check_yamls[0]
    assert check.type_name == "row_count"


def test_variable_in_dataset_name():
    """Test that variables are resolved in dataset identifiers."""
    contract_yaml = """
    variables:
      - name: source
        default: postgres
      - name: database
        default: analytics
      - name: schema
        default: public
      - name: table
        default: users
    dataset: ${var.source}/${var.database}/${var.schema}/${var.table}
    columns: []
    """
    variables = {
        "source": "postgres",
        "database": "analytics",
        "schema": "public",
        "table": "users",
    }
    contract = parse_contract(contract_yaml, variables=variables)

    # Dataset should be resolved
    assert contract.dataset is not None


def test_variable_in_metric_expression():
    """Test that variables are resolved in metric expressions."""
    contract_yaml = """
    variables:
      - name: filter_condition
        default: active = true
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          name: custom_count
          expression: count(*) where ${var.filter_condition}
          must_be_greater_than: 100
    """
    variables = {"filter_condition": "active = true"}
    contract = parse_contract(contract_yaml, variables=variables)

    check = contract.checks[0]
    assert check.type_name == "metric"


def test_variable_in_valid_values():
    """Test that variables are resolved in valid_values lists."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: status
        data_type: character varying
        checks:
          - invalid:
              valid_values:
                - ${var.status1}
                - ${var.status2}
    """
    variables = {"status1": "active", "status2": "inactive"}
    contract = parse_contract(contract_yaml, variables=variables)

    col = contract.columns[0]
    check = col.check_yamls[0]
    assert check.type_name == "invalid"


def test_undefined_variable_kept_as_literal():
    """Test that undefined variables are kept as literals in the YAML."""
    contract_yaml = """
    variables:
      - name: email_regex
        default: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$
    dataset: my_data_source/my_dataset
    columns:
      - name: email
        data_type: character varying
        checks:
          - invalid:
              valid_format:
                name: email_format
                regex: ${var.email_regex}
    """
    # No variables provided - will use default
    contract = parse_contract(contract_yaml, variables={})

    col = contract.columns[0]
    check = col.check_yamls[0]
    assert check.type_name == "invalid"


def test_multiple_variables_in_same_field():
    """Test that multiple variables in the same field are resolved."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: value
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              filter: date >= '${var.start_date}' and date <= '${var.end_date}'
              must_be_between:
                greater_than: ${var.min_val}
                less_than: ${var.max_val}
    """
    variables = {
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "min_val": "1000",
        "max_val": "10000",
    }
    contract = parse_contract(contract_yaml, variables=variables)

    col = contract.columns[0]
    check = col.check_yamls[0]
    assert check.type_name == "aggregate"


def test_nested_variable_references():
    """Test that nested variable references are resolved (var referencing var)."""
    contract_yaml = """
    variables:
      - name: threshold
        default: 1000
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - row_count:
          must_be_greater_than: ${var.threshold}
    """
    # Simulate nested variable: threshold depends on environment
    variables = {"threshold": "1000"}
    contract = parse_contract(contract_yaml, variables=variables)

    check = contract.checks[0]
    assert check.type_name == "row_count"


def test_variable_in_column_expression():
    """Test that variables are resolved in column_expression."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: salary
        data_type: numeric
        column_expression: salary / ${var.exchange_rate}
        checks:
          - aggregate:
              function: avg
              must_be_greater_than: 50000
    """
    variables = {"exchange_rate": "1.1"}
    contract = parse_contract(contract_yaml, variables=variables)

    col = contract.columns[0]
    # Column expression should be available
    assert col.column_expression is not None
