"""
Unit tests for variable resolution in checks.

Tests that variables are correctly resolved in various check contexts
including filters, thresholds, dataset names, and expressions.
Covers all four resolution scenarios:
  - no default, no runtime value → unresolved
  - no default, runtime value provided → runtime value used
  - default defined, no runtime value → default used
  - default defined, runtime override → override used
"""

from helpers.yaml_parsing_helpers import parse_contract

# ---------------------------------------------------------------------------
# Scenario matrix: how defaults and runtime values interact
# ---------------------------------------------------------------------------


def test_required_variable_without_value_stays_unresolved():
    """When a variable is declared without a default and no runtime value
    is provided, the ${var.name} reference stays as a literal string."""
    contract_yaml = """
    variables:
      status:
        required: true
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
    contract = parse_contract(contract_yaml, variables={})

    check = contract.columns[0].check_yamls[0]
    assert check.filter == "status = '${var.status}'"
    assert contract.resolved_variable_values["status"] is None


def test_required_variable_with_runtime_value():
    """When a variable is declared without a default but a runtime value
    is provided, the runtime value is used."""
    contract_yaml = """
    variables:
      status:
        required: true
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
    contract = parse_contract(contract_yaml, variables={"status": "active"})

    check = contract.columns[0].check_yamls[0]
    assert check.filter == "status = 'active'"
    assert contract.resolved_variable_values["status"] == "active"


def test_default_variable_without_runtime_override():
    """When a variable has a default and no runtime value is provided,
    the default value is used."""
    contract_yaml = """
    variables:
      status:
        default: pending
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
    contract = parse_contract(contract_yaml, variables={})

    check = contract.columns[0].check_yamls[0]
    assert check.filter == "status = 'pending'"
    assert contract.resolved_variable_values["status"] == "pending"


def test_default_variable_overridden_by_runtime():
    """When a variable has a default and a different runtime value is provided,
    the runtime value takes precedence."""
    contract_yaml = """
    variables:
      status:
        default: pending
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
    contract = parse_contract(contract_yaml, variables={"status": "active"})

    check = contract.columns[0].check_yamls[0]
    assert check.filter == "status = 'active'"
    assert contract.resolved_variable_values["status"] == "active"


# ---------------------------------------------------------------------------
# Resolution in different YAML fields
# ---------------------------------------------------------------------------


def test_variable_in_threshold():
    """Test that runtime variables override defaults in threshold values."""
    contract_yaml = """
    variables:
      min_rows:
        default: 500
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - row_count:
          must_be_greater_than: ${var.min_rows}
    """
    contract = parse_contract(contract_yaml, variables={"min_rows": "1000"})

    check = contract.checks[0]
    assert check.check_yaml_object.read_value("must_be_greater_than") == "1000"
    assert contract.resolved_variable_values["min_rows"] == "1000"


def test_variable_in_dataset_name():
    """Test that variables are resolved in dataset identifiers, using a mix
    of defaults and runtime overrides."""
    contract_yaml = """
    variables:
      source:
        default: mysql
      database:
        default: staging
      schema:
        default: public
      table:
        required: true
    dataset: ${var.source}/${var.database}/${var.schema}/${var.table}
    columns: []
    """
    contract = parse_contract(
        contract_yaml,
        variables={"source": "postgres", "database": "analytics", "table": "users"},
    )

    # source and database overridden, schema uses default, table from runtime
    assert contract.dataset == "postgres/analytics/public/users"


def test_variable_in_metric_expression():
    """Test that a runtime variable overrides the default in a metric expression."""
    contract_yaml = """
    variables:
      filter_condition:
        default: status = 'draft'
    dataset: my_data_source/my_dataset
    columns: []
    checks:
      - metric:
          name: custom_count
          expression: count(*) where ${var.filter_condition}
          must_be_greater_than: 100
    """
    contract = parse_contract(contract_yaml, variables={"filter_condition": "active = true"})

    check = contract.checks[0]
    assert check.expression == "count(*) where active = true"


def test_variable_in_valid_values_uses_defaults():
    """Test that defaults are used for valid_values when no runtime values given."""
    contract_yaml = """
    variables:
      status1:
        default: active
      status2:
        default: inactive
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
    contract = parse_contract(contract_yaml, variables={})

    check = contract.columns[0].check_yamls[0]
    assert check.valid_values == ["active", "inactive"]


def test_variable_in_valid_values_overridden():
    """Test that runtime values override defaults in valid_values."""
    contract_yaml = """
    variables:
      status1:
        default: active
      status2:
        default: inactive
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
    contract = parse_contract(contract_yaml, variables={"status1": "enabled", "status2": "disabled"})

    check = contract.columns[0].check_yamls[0]
    assert check.valid_values == ["enabled", "disabled"]


def test_undeclared_variable_kept_as_literal():
    """Test that a variable reference used but never declared stays as a literal."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: amount
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              filter: status = '${var.undefined_status}'
              must_be_greater_than: 100
    """
    contract = parse_contract(contract_yaml, variables={})

    check = contract.columns[0].check_yamls[0]
    assert check.filter == "status = '${var.undefined_status}'"


def test_multiple_variables_with_mixed_sources():
    """Test that multiple variables in the same field resolve from a mix
    of defaults and runtime overrides."""
    contract_yaml = """
    variables:
      start_date:
        default: "2020-01-01"
      end_date:
        default: "2020-12-31"
    dataset: my_data_source/my_dataset
    columns:
      - name: value
        data_type: numeric
        checks:
          - aggregate:
              function: sum
              filter: date >= '${var.start_date}' and date <= '${var.end_date}'
              must_be_greater_than: 100
    """
    # Override only start_date; end_date uses its default
    contract = parse_contract(contract_yaml, variables={"start_date": "2024-06-01"})

    check = contract.columns[0].check_yamls[0]
    assert check.filter == "date >= '2024-06-01' and date <= '2020-12-31'"


def test_variable_in_dataset_filter_uses_default():
    """Test that a dataset-level filter uses the variable default when
    no runtime value is provided."""
    contract_yaml = """
    variables:
      dataset_filter:
        default: "created_at > '2023-01-01'"
    dataset: my_data_source/my_dataset
    filter: ${var.dataset_filter}
    columns: []
    """
    contract = parse_contract(contract_yaml, variables={})

    assert contract.filter == "created_at > '2023-01-01'"


def test_variable_in_column_expression_overridden():
    """Test that a runtime variable overrides the default in column_expression."""
    contract_yaml = """
    variables:
      exchange_rate:
        default: "1.0"
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
    contract = parse_contract(contract_yaml, variables={"exchange_rate": "1.35"})

    col = contract.columns[0]
    assert col.column_expression == "salary / 1.35"
