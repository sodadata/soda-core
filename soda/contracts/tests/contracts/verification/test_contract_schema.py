import logging

from contracts.helpers.contract_data_source_test_helper import ContractDataSourceTestHelper
from helpers.test_table import TestTable
from soda.contracts.impl.sql_dialect import SqlDialect
from soda.execution.data_type import DataType

from soda.contracts.check import SchemaCheck, SchemaCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult

logger = logging.getLogger(__name__)


contracts_schema_test_table = TestTable(
    name="contracts_schema",
    # fmt: off
    columns=[
        ("id", DataType.TEXT),
        ("size", DataType.DECIMAL),
        ("distance", DataType.INTEGER),
        ("created", DataType.DATE),
    ],
    values=[
    ]
    # fmt: on
)


def test_contract_schema_pass_with_data_types(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect
    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {sql_dialect.get_schema_check_sql_type_text()}
              - name: size
                data_type: {sql_dialect.get_schema_check_sql_type_decimal()}
              - name: distance
                data_type: {sql_dialect.get_schema_check_sql_type_integer()}
              - name: created
                data_type: {sql_dialect.get_schema_check_sql_type_date()}
         """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        "id": sql_dialect.get_schema_check_sql_type_text(),
        "size": sql_dialect.get_schema_check_sql_type_decimal(),
        "distance": sql_dialect.get_schema_check_sql_type_integer(),
        "created": sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check: SchemaCheck = schema_check_result.check
    assert check.columns == {
        "id": sql_dialect.get_schema_check_sql_type_text(),
        "size": sql_dialect.get_schema_check_sql_type_decimal(),
        "distance": sql_dialect.get_schema_check_sql_type_integer(),
        "created": sql_dialect.get_schema_check_sql_type_date(),
    }


def test_contract_schema_pass_without_data_types(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
              - name: size
              - name: distance
              - name: created
        """
    )

    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        "id": sql_dialect.get_schema_check_sql_type_text(),
        "size": sql_dialect.get_schema_check_sql_type_decimal(),
        "distance": sql_dialect.get_schema_check_sql_type_integer(),
        "created": sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check: SchemaCheck = schema_check_result.check
    assert check.columns == {
        "id": None,
        "size": None,
        "distance": None,
        "created": None,
    }


def test_contract_schema_missing_column(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {sql_dialect.get_schema_check_sql_type_text()}
              - name: size
                data_type: {sql_dialect.get_schema_check_sql_type_decimal()}
              - name: distance
                data_type: {sql_dialect.get_schema_check_sql_type_integer()}
              - name: themissingcolumn
                data_type: {sql_dialect.get_schema_check_sql_type_text()}
              - name: created
                data_type: {sql_dialect.get_schema_check_sql_type_date()}
        """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        "id": sql_dialect.get_schema_check_sql_type_text(),
        "size": sql_dialect.get_schema_check_sql_type_decimal(),
        "distance": sql_dialect.get_schema_check_sql_type_integer(),
        "created": sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == ["themissingcolumn"]
    assert schema_check_result.columns_having_wrong_type == []

    assert "Column 'themissingcolumn' was missing" in str(contract_result)


def test_contract_schema_missing_optional_column(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect

    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {sql_dialect.get_schema_check_sql_type_text()}
              - name: size
                data_type: {sql_dialect.get_schema_check_sql_type_decimal()}
              - name: distance
                data_type: {sql_dialect.get_schema_check_sql_type_integer()}
              - name: themissingcolumn
                data_type: {sql_dialect.get_schema_check_sql_type_text()}
                optional: true
              - name: created
                data_type: {sql_dialect.get_schema_check_sql_type_date()}
        """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        "id": sql_dialect.get_schema_check_sql_type_text(),
        "size": sql_dialect.get_schema_check_sql_type_decimal(),
        "distance": sql_dialect.get_schema_check_sql_type_integer(),
        "created": sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []


def test_contract_schema_extra_column(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {sql_dialect.get_schema_check_sql_type_text()}
              - name: size
                data_type: {sql_dialect.get_schema_check_sql_type_decimal()}
              - name: created
                data_type: {sql_dialect.get_schema_check_sql_type_date()}
        """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        "id": sql_dialect.get_schema_check_sql_type_text(),
        "size": sql_dialect.get_schema_check_sql_type_decimal(),
        "distance": sql_dialect.get_schema_check_sql_type_integer(),
        "created": sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == ["distance"]
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    assert "Column 'distance' was present and not allowed" in str(contract_result)


def test_contract_schema_data_type_mismatch(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: WRONG_VARCHAR
              - name: size
                data_type: {sql_dialect.get_schema_check_sql_type_decimal()}
              - name: distance
                data_type: {sql_dialect.get_schema_check_sql_type_integer()}
              - name: created
                data_type: {sql_dialect.get_schema_check_sql_type_date()}
        """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        "id": sql_dialect.get_schema_check_sql_type_text(),
        "size": sql_dialect.get_schema_check_sql_type_decimal(),
        "distance": sql_dialect.get_schema_check_sql_type_integer(),
        "created": sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []

    data_type_mismatch = schema_check_result.columns_having_wrong_type[0]
    assert data_type_mismatch.column == "id"
    assert data_type_mismatch.expected_data_type == "WRONG_VARCHAR"
    assert data_type_mismatch.actual_data_type == sql_dialect.get_schema_check_sql_type_text()

    assert "Column 'id': Expected type 'WRONG_VARCHAR', but was 'character varying'" in str(contract_result)
