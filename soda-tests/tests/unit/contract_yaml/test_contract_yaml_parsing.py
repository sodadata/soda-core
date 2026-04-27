"""
Unit tests for ContractYaml parsing of basic dataset and column structures.
"""

from helpers.test_functions import dedent_and_strip
from helpers.yaml_parsing_helpers import parse_column_check, parse_contract
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.exceptions import ContractParserException
from soda_core.common.logs import Logs
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.impl.contract_yaml import ContractYaml


class _FakeSqlDialect:
    """Minimal dialect stub exposing only what ContractYaml parsing calls."""

    convert_str_to_datetime = staticmethod(convert_str_to_datetime)
    convert_datetime_to_str = staticmethod(convert_datetime_to_str)

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "string": SodaDataTypeName.TEXT,
            "clob": SodaDataTypeName.TEXT,
        }

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return data_type_name.lower() in ["varchar", "char", "string", "clob"]

    def data_type_has_parameter_numeric_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in ["numeric", "number", "decimal"]

    def data_type_has_parameter_numeric_scale(self, data_type_name) -> bool:
        return data_type_name.lower() in ["numeric", "number", "decimal"]

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in ["timestamp", "timestamp_tz", "time"]


class _FakeDataSourceImpl:
    sql_dialect = _FakeSqlDialect()


def _parse_with_dialect(yaml_str: str) -> ContractYaml:
    source = ContractYamlSource.from_str(yaml_str=dedent_and_strip(yaml_str))
    return ContractYaml.parse(
        contract_yaml_source=source,
        provided_variable_values={},
        primary_data_source_impl=_FakeDataSourceImpl(),
    )


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


def test_parse_character_maximum_length_valid_on_varchar():
    """Test that character_maximum_length is accepted on character types without errors."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: email
            data_type: VARCHAR
            character_maximum_length: 255
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.columns[0].character_maximum_length == 255


def test_parse_character_maximum_length_accepts_parenthesized_type():
    """Test that data_type with parenthesized suffix like 'varchar(255)' is normalized for validation."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: email
            data_type: varchar(255)
            character_maximum_length: 255
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.columns[0].character_maximum_length == 255


def test_parse_character_maximum_length_invalid_on_decimal(logs: Logs):
    """Test that character_maximum_length on DECIMAL logs an error."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: amount
            data_type: DECIMAL
            character_maximum_length: 255
    """
    parse_contract(yaml_str)

    assert "'character_maximum_length' is only valid for data types" in logs.get_errors_str()
    assert "amount" in logs.get_errors_str()


def test_parse_numeric_precision_valid_on_decimal():
    """Test that numeric_precision/scale are accepted on DECIMAL without errors."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: amount
            data_type: DECIMAL
            numeric_precision: 10
            numeric_scale: 2
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.columns[0].numeric_precision == 10
    assert contract_yaml.columns[0].numeric_scale == 2


def test_parse_numeric_precision_invalid_on_varchar(logs: Logs):
    """Test that numeric_precision on VARCHAR logs an error."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: name
            data_type: VARCHAR
            numeric_precision: 10
    """
    parse_contract(yaml_str)

    assert "'numeric_precision' is only valid for data types" in logs.get_errors_str()


def test_parse_numeric_scale_invalid_on_timestamp(logs: Logs):
    """Test that numeric_scale on TIMESTAMP logs an error."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: created_at
            data_type: TIMESTAMP
            numeric_scale: 2
    """
    parse_contract(yaml_str)

    assert "'numeric_scale' is only valid for data types" in logs.get_errors_str()


def test_parse_datetime_precision_valid_on_timestamp():
    """Test that datetime_precision is accepted on TIMESTAMP without errors."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: created_at
            data_type: TIMESTAMP
            datetime_precision: 6
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.columns[0].datetime_precision == 6


def test_parse_datetime_precision_invalid_on_integer(logs: Logs):
    """Test that datetime_precision on INTEGER logs an error."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: id
            data_type: INTEGER
            datetime_precision: 6
    """
    parse_contract(yaml_str)

    assert "'datetime_precision' is only valid for data types" in logs.get_errors_str()


def test_parse_type_params_without_data_type_skipped(logs: Logs):
    """Test that missing data_type skips type-param validation (other validators handle it)."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: mystery
            character_maximum_length: 255
    """
    parse_contract(yaml_str)

    assert "character_maximum_length" not in logs.get_errors_str()


def test_parse_numeric_family_switch_preserves_params():
    """Test that DECIMAL and NUMERIC are in the same family — params are valid for both."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: a
            data_type: DECIMAL
            numeric_precision: 10
            numeric_scale: 2
          - name: b
            data_type: NUMERIC
            numeric_precision: 10
            numeric_scale: 2
    """
    contract_yaml = parse_contract(yaml_str)

    assert contract_yaml.columns[0].numeric_precision == 10
    assert contract_yaml.columns[1].numeric_precision == 10


def test_parse_dialect_unknown_type_without_dialect_is_skipped(logs: Logs):
    """Without a dialect, native types like 'string' are unresolvable — validation skipped."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: s
            data_type: string
            numeric_precision: 10
    """
    parse_contract(yaml_str)

    assert "numeric_precision" not in logs.get_errors_str()


def test_parse_dialect_resolves_native_string_valid_char_param():
    """With a dialect, 'string' resolves to TEXT → character_maximum_length is valid."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: s
            data_type: string
            character_maximum_length: 255
    """
    contract_yaml = _parse_with_dialect(yaml_str)

    assert contract_yaml.columns[0].character_maximum_length == 255


def test_parse_dialect_resolves_native_string_invalid_numeric_param(logs: Logs):
    """With a dialect, 'string' resolves to TEXT → numeric_precision on it is flagged."""
    yaml_str = """
        dataset: ds/db/schema/table
        columns:
          - name: s
            data_type: string
            numeric_precision: 10
    """
    _parse_with_dialect(yaml_str)

    assert "'numeric_precision' is only valid for data types" in logs.get_errors_str()
