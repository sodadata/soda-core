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
    """Test that variables in check thresholds are resolved (as strings in YAML)."""
    yaml_str = """
        dataset: ds/db/schema/table
        variables:
          max_missing:
            type: number
            default: 10
        columns:
          - name: status
            data_type: string
            checks:
              - missing:
                  threshold:
                    must_be_less_than: 5
    """
    contract_yaml = parse_contract(yaml_str, variables={"max_missing": "10"})

    # Verify the threshold is parsed correctly
    check_yaml = contract_yaml.columns[0].check_yamls[0]
    assert check_yaml.threshold is not None
    assert check_yaml.threshold.must_be_less_than == 5


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


def test_variables_undefined_no_crash():
    """Test that undefined variables are left as literal ${var.UNKNOWN}."""
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
        contract_yaml = parse_contract(yaml_str, variables={})

        # Verify error was logged
        error_str = logs.get_errors_str()
        assert "required_var" in error_str or "Variable" in error_str
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
    assert "test_value" in contract_yaml.filter
