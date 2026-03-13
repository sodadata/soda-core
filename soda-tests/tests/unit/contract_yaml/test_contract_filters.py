"""
Unit tests for filter parsing at contract and check levels.
"""

from helpers.yaml_parsing_helpers import parse_column_check, parse_contract


def test_filter_on_contract():
    """Test that filter string at contract level is parsed."""
    yaml_str = """
        dataset: ds/db/schema/table
        filter: "created_date >= '2024-01-01' AND status = 'active'"
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.filter == "created_date >= '2024-01-01' AND status = 'active'"


def test_filter_on_check():
    """Test that filter field on a per-check basis is parsed."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: status
            data_type: string
            checks:
              - missing:
                  filter: "region = 'US'"
    """
    check_yaml = parse_column_check(yaml_str)

    assert check_yaml.filter == "region = 'US'"


def test_filter_with_variables():
    """Test that filter containing ${var.x} is resolved."""
    yaml_str = """
        dataset: ds/db/schema/table
        variables:
          min_date:
            type: string
            default: "2024-01-01"
        filter: "created_date >= '${var.min_date}'"
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str, variables={"min_date": "2024-03-01"})

    assert contract_yaml.filter == "created_date >= '2024-03-01'"


def test_no_filter_is_none():
    """Test that absent filter is None."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.filter is None
