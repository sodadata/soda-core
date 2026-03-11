"""
Unit tests for ColumnYaml parsing of column properties and validations.
"""

from helpers.yaml_parsing_helpers import parse_column
from soda_core.contracts.impl.contract_yaml import ValidReferenceDataYaml


def test_column_name_and_data_type():
    """Test that basic column name and data_type fields are parsed."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: email
            data_type: varchar(255)
    """
    column_yaml = parse_column(yaml_str)

    assert column_yaml.name == "email"
    assert column_yaml.data_type == "varchar(255)"


def test_column_missing_values_list():
    """Test that missing_values list is parsed correctly."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: status
            data_type: string
            missing_values:
              - "N/A"
              - ""
              - null
    """
    column_yaml = parse_column(yaml_str)

    assert column_yaml.missing_values is not None
    assert len(column_yaml.missing_values) == 3
    assert "N/A" in column_yaml.missing_values
    assert "" in column_yaml.missing_values
    assert None in column_yaml.missing_values


def test_column_valid_values_and_format():
    """Test that valid_values list is parsed correctly."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: status
            data_type: string
            valid_values:
              - "active"
              - "inactive"
              - "pending"
    """
    column_yaml = parse_column(yaml_str)

    assert column_yaml.valid_values is not None
    assert len(column_yaml.valid_values) == 3
    assert "active" in column_yaml.valid_values
    assert "inactive" in column_yaml.valid_values
    assert "pending" in column_yaml.valid_values


def test_column_valid_reference_data():
    """Test that valid_reference_data with dataset+column is parsed."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: user_id
            data_type: integer
            valid_reference_data:
              dataset: ds/db/schema/users
              column: id
    """
    column_yaml = parse_column(yaml_str)

    assert column_yaml.valid_reference_data is not None
    assert isinstance(column_yaml.valid_reference_data, ValidReferenceDataYaml)
    assert column_yaml.valid_reference_data.dataset == "ds/db/schema/users"
    assert column_yaml.valid_reference_data.column == "id"


def test_column_expression():
    """Test that column_expression is parsed and stripped."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: full_name
            data_type: string
            column_expression: |
              COALESCE(first_name, '') || ' ' || COALESCE(last_name, '')
    """
    column_yaml = parse_column(yaml_str)

    assert column_yaml.column_expression is not None
    assert "COALESCE" in column_yaml.column_expression
    assert column_yaml.column_expression == column_yaml.column_expression.strip()


def test_column_optional_fields_absent():
    """Test that optional fields are None when not specified."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: integer
    """
    column_yaml = parse_column(yaml_str)

    assert column_yaml.missing_values is None
    assert column_yaml.missing_format is None
    assert column_yaml.invalid_values is None
    assert column_yaml.invalid_format is None
    assert column_yaml.valid_values is None
    assert column_yaml.valid_format is None
    assert column_yaml.valid_min is None
    assert column_yaml.valid_max is None
    assert column_yaml.valid_reference_data is None
    assert column_yaml.column_expression is None
