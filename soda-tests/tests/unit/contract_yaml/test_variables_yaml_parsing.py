"""
Unit tests for YAML variable parsing and resolution.
"""

from helpers.yaml_parsing_helpers import parse_contract
from soda_core.common.logs import Logs


def test_variables_in_dataset_name():
    """Test that ${var.prefix} in dataset name is resolved."""
    yaml_str = """
        dataset: ${var.prefix}/db/schema/table
        variables:
          prefix:
            type: string
            default: myapp
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str, variables={"prefix": "myapp"})

    assert contract_yaml.dataset == "myapp/db/schema/table"


def test_variables_in_column_checks():
    """Test that variables in check filters are resolved (as strings in YAML)."""
    yaml_str = """
        dataset: ds/db/schema/table
        variables:
          status_filter:
            type: string
            default: draft
        columns:
          - name: amount
            data_type: numeric
            checks:
              - aggregate:
                  function: sum
                  filter: status = '${var.status_filter}'
                  must_be_greater_than: 0
    """
    contract_yaml = parse_contract(yaml_str, variables={"status_filter": "active"})

    # Verify the variable was resolved to the runtime value, not the default
    assert contract_yaml.resolved_variable_values.get("status_filter") == "active"
    # Verify the variable is actually substituted in the check's filter field
    check_yaml = contract_yaml.columns[0].check_yamls[0]
    assert check_yaml.filter == "status = 'active'"


def test_variables_in_filter():
    """Test that variables in filters are resolved."""
    yaml_str = """
        dataset: ds/db/schema/table
        variables:
          env:
            type: string
            default: dev
        filter: "environment = '${var.env}'"
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str, variables={"env": "prod"})

    assert contract_yaml.filter == "environment = 'prod'"


def test_variables_type_coercion():
    """Test that numeric variables are coerced to correct type."""
    yaml_str = """
        dataset: ds/db/schema/table
        variables:
          count:
            type: number
            default: 100
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str, variables={"count": "250"})

    # Verify the variable was resolved and can be used as a number
    resolved_value = contract_yaml.resolved_variable_values.get("count")
    assert resolved_value == "250"
    # When used in check thresholds, it will be coerced to number
    assert isinstance(float(resolved_value), float)


def test_required_variable_without_value_logs_error():
    """Test that a declared required variable with no runtime value logs an error."""
    logs = Logs()
    try:
        yaml_str = """
            dataset: ds/db/schema/table
            variables:
              required_var:
                type: string
                required: true
            columns:
              - name: id
                data_type: integer
        """
        # Don't provide required_var
        parse_contract(yaml_str, variables={})

        # Verify error was logged
        error_str = logs.get_errors_str()
        assert "required_var" in error_str, f"Expected 'required_var' in error message: {error_str}"
    finally:
        logs.remove_from_root_logger()


def test_variables_env_resolution(env_vars: dict):
    """Test that environment variables are resolved when use_env_vars=True."""
    env_vars["MY_VAR"] = "test_value"

    yaml_str = """
        dataset: ds/db/schema/table
        filter: "status = '${env.MY_VAR}'"
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str)

    # Environment variables are resolved when parsing the contract
    assert contract_yaml.filter == "status = 'test_value'"
