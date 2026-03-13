"""
Unit tests for ContractYaml parsing of basic dataset and column structures.
"""

from helpers.yaml_parsing_helpers import parse_column_check, parse_contract
from soda_core.common.exceptions import ContractParserException
from soda_core.common.logs import Logs


def test_parse_dataset_qualified_name():
    """Test that a 4-part dataset path is parsed correctly."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.dataset == "ds/db/schema/table"


def test_parse_columns_with_types():
    """Test that column names, data_types, and order are parsed correctly."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: user_id
            data_type: integer
          - name: user_name
            data_type: varchar(255)
          - name: created_at
            data_type: timestamp
    """
    contract_yaml = parse_contract(yaml_str)

    assert len(contract_yaml.columns) == 3
    assert contract_yaml.columns[0].name == "user_id"
    assert contract_yaml.columns[0].data_type == "integer"
    assert contract_yaml.columns[1].name == "user_name"
    assert contract_yaml.columns[1].data_type == "varchar(255)"
    assert contract_yaml.columns[2].name == "created_at"
    assert contract_yaml.columns[2].data_type == "timestamp"


def test_parse_dataset_level_checks():
    """Test that checks[] at contract level are recognized."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
        checks:
          - schema:
          - row_count:
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.checks is not None
    assert len(contract_yaml.checks) == 2
    assert contract_yaml.checks[0].type_name == "schema"
    assert contract_yaml.checks[1].type_name == "row_count"


def test_parse_column_level_checks():
    """Test that checks inside column definitions are recognized."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
            checks:
              - missing:
              - invalid:
    """
    contract_yaml = parse_contract(yaml_str)

    assert len(contract_yaml.columns) == 1
    column_yaml = contract_yaml.columns[0]
    assert column_yaml.check_yamls is not None
    assert len(column_yaml.check_yamls) == 2
    assert column_yaml.check_yamls[0].type_name == "missing"
    assert column_yaml.check_yamls[1].type_name == "invalid"


def test_parse_filter():
    """Test that contract-level filter string is parsed."""
    yaml_str = """
        dataset: ds/db/schema/table
        filter: "status = 'active'"
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.filter == "status = 'active'"


def test_parse_variables():
    """Test that variables section with name/type/default is parsed."""
    yaml_str = """
        dataset: ds/db/schema/table
        variables:
          env:
            type: string
            default: prod
          threshold:
            type: number
            default: 100
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str)

    assert len(contract_yaml.variables) == 2
    assert contract_yaml.variables[0].name == "env"
    assert contract_yaml.variables[0].type == "string"
    assert contract_yaml.variables[0].default == "prod"
    assert contract_yaml.variables[1].name == "threshold"
    assert contract_yaml.variables[1].type == "number"
    assert contract_yaml.variables[1].default == 100


def test_parse_empty_checks_raises_exception():
    """Test that empty checks list raises ContractParserException."""
    import pytest

    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
        checks: []
    """

    with pytest.raises(ContractParserException, match="must not be an empty list"):
        parse_contract(yaml_str)


def test_parse_invalid_dataset_logs_error(logs: Logs):
    """Test that bad dataset path (no slashes) logs an error."""
    yaml_str = """
        dataset: just_table_name
        columns:
          - name: id
            data_type: integer
    """
    parse_contract(yaml_str)

    assert "Invalid dataset qualified name" in logs.get_errors_str()


def test_parse_attributes_on_check():
    """Test that attributes dict on a check is parsed correctly."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
            checks:
              - missing:
                  attributes:
                    owner: data-team
                    priority: high
    """
    check_yaml = parse_column_check(yaml_str)

    assert check_yaml.attributes is not None
    assert isinstance(check_yaml.attributes, dict)
    assert check_yaml.attributes["owner"] == "data-team"
    assert check_yaml.attributes["priority"] == "high"


def test_parse_name_and_qualifier():
    """Test that check name and qualifier fields are parsed."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: status
            data_type: string
            checks:
              - missing:
                  name: "Status should not be missing"
                  qualifier: "important_column"
    """
    check_yaml = parse_column_check(yaml_str)

    assert check_yaml.name == "Status should not be missing"
    assert check_yaml.qualifier == "important_column"
