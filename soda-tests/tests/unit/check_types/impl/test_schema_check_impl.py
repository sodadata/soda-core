"""
Unit tests for SchemaCheckImpl validation-without-execute.

Tests the schema check in isolation during contract validation,
without needing a real database connection.
"""

from helpers.impl_test_helpers import validate_contract


def test_schema_check_validates_without_execute():
    """Test that a contract with a schema check validates successfully in dry-run mode."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: character varying
    checks:
      - schema:
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_schema_check_with_allow_extra_columns_validates():
    """Test that schema check with allow_extra_columns validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: created_at
        data_type: timestamp without time zone
    checks:
      - schema:
          allow_extra_columns: true
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_schema_check_with_allow_other_column_order_validates():
    """Test that schema check with allow_other_column_order validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: user_id
        data_type: bigint
      - name: email
        data_type: character varying
      - name: status
        data_type: character varying
    checks:
      - schema:
          allow_other_column_order: true
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_schema_check_with_both_allow_flags_validates():
    """Test that schema check with both allow flags validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: id
        data_type: integer
      - name: name
        data_type: text
    checks:
      - schema:
          allow_extra_columns: true
          allow_other_column_order: true
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors


def test_schema_check_with_character_length_validates():
    """Test that schema check with character_maximum_length validates."""
    contract_yaml = """
    dataset: my_data_source/my_dataset
    columns:
      - name: zip_code
        data_type: character varying
        character_maximum_length: 10
      - name: country_code
        data_type: character varying
        character_maximum_length: 2
    checks:
      - schema:
    """
    result = validate_contract(contract_yaml)
    assert result is not None
    errors = result.get_errors_str()
    assert errors is None or "sql_dialect" not in errors
