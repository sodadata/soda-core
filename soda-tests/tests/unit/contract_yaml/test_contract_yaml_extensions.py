"""
Unit tests for extended ContractYaml features like attributes, qualifiers, and mixed checks.
"""

from helpers.yaml_parsing_helpers import parse_column_check, parse_contract


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


def test_parse_multiple_checks_per_column():
    """Test that a column can have multiple checks."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: user_id
            data_type: integer
            checks:
              - missing:
              - invalid:
              - duplicate:
    """
    contract_yaml = parse_contract(yaml_str)

    column_yaml = contract_yaml.columns[0]
    assert len(column_yaml.check_yamls) == 3
    assert column_yaml.check_yamls[0].type_name == "missing"
    assert column_yaml.check_yamls[1].type_name == "invalid"
    assert column_yaml.check_yamls[2].type_name == "duplicate"


def test_parse_dataset_and_column_checks_mixed():
    """Test that both dataset and column-level checks can exist together."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
            checks:
              - missing:
          - name: name
            data_type: string
            checks:
              - invalid:
        checks:
          - schema:
          - row_count:
    """
    contract_yaml = parse_contract(yaml_str)

    # Check dataset-level checks
    assert contract_yaml.checks is not None
    assert len(contract_yaml.checks) == 2
    assert contract_yaml.checks[0].type_name == "schema"
    assert contract_yaml.checks[1].type_name == "row_count"

    # Check column-level checks
    assert len(contract_yaml.columns) == 2
    assert len(contract_yaml.columns[0].check_yamls) == 1
    assert contract_yaml.columns[0].check_yamls[0].type_name == "missing"
    assert len(contract_yaml.columns[1].check_yamls) == 1
    assert contract_yaml.columns[1].check_yamls[0].type_name == "invalid"
