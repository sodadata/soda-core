import logging

from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import SchemaCheck, SchemaCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult
from soda.contracts.impl.sql_dialect import SqlDialect

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
    id_casified = sql_dialect.default_casify("id")
    size_casified = sql_dialect.default_casify("size")
    distance_casified = sql_dialect.default_casify("distance")
    created_casified = sql_dialect.default_casify("created")

    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {DataType.TEXT}
              - name: size
                data_type: {DataType.DECIMAL}
              - name: distance
                data_type: {DataType.INTEGER}
              - name: created
                data_type: {DataType.DATE}
         """,
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        id_casified: sql_dialect.get_schema_check_sql_type_text(),
        size_casified: sql_dialect.get_schema_check_sql_type_decimal(),
        distance_casified: sql_dialect.get_schema_check_sql_type_integer(),
        created_casified: sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check: SchemaCheck = schema_check_result.check
    assert check.columns == {
        id_casified: sql_dialect.get_schema_check_sql_type_text(),
        size_casified: sql_dialect.get_schema_check_sql_type_decimal(),
        distance_casified: sql_dialect.get_schema_check_sql_type_integer(),
        created_casified: sql_dialect.get_schema_check_sql_type_date(),
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
        """,
    )

    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect
    id_casified = sql_dialect.default_casify("id")
    size_casified = sql_dialect.default_casify("size")
    distance_casified = sql_dialect.default_casify("distance")
    created_casified = sql_dialect.default_casify("created")

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        id_casified: sql_dialect.get_schema_check_sql_type_text(),
        size_casified: sql_dialect.get_schema_check_sql_type_decimal(),
        distance_casified: sql_dialect.get_schema_check_sql_type_integer(),
        created_casified: sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check: SchemaCheck = schema_check_result.check
    assert check.columns == {
        id_casified: None,
        size_casified: None,
        distance_casified: None,
        created_casified: None,
    }


def test_contract_schema_missing_column(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect
    id_casified = sql_dialect.default_casify("id")
    size_casified = sql_dialect.default_casify("size")
    distance_casified = sql_dialect.default_casify("distance")
    created_casified = sql_dialect.default_casify("created")
    themissingcolumn_casified = sql_dialect.default_casify("themissingcolumn")

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {DataType.TEXT}
              - name: size
                data_type: {DataType.DECIMAL}
              - name: distance
                data_type: {DataType.INTEGER}
              - name: themissingcolumn
                data_type: {DataType.TEXT}
              - name: created
                data_type: {DataType.DATE}
        """,
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        id_casified: sql_dialect.get_schema_check_sql_type_text(),
        size_casified: sql_dialect.get_schema_check_sql_type_decimal(),
        distance_casified: sql_dialect.get_schema_check_sql_type_integer(),
        created_casified: sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == [themissingcolumn_casified]
    assert schema_check_result.columns_having_wrong_type == []

    assert "column 'themissingcolumn' was missing" in str(contract_result).lower()


def test_contract_schema_missing_optional_column(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect
    id_casified = sql_dialect.default_casify("id")
    size_casified = sql_dialect.default_casify("size")
    distance_casified = sql_dialect.default_casify("distance")
    created_casified = sql_dialect.default_casify("created")

    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {DataType.TEXT}
              - name: size
                data_type: {DataType.DECIMAL}
              - name: distance
                data_type: {DataType.INTEGER}
              - name: themissingcolumn
                data_type: {DataType.TEXT}
                optional: true
              - name: created
                data_type: {DataType.DATE}
        """,
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        id_casified: sql_dialect.get_schema_check_sql_type_text(),
        size_casified: sql_dialect.get_schema_check_sql_type_decimal(),
        distance_casified: sql_dialect.get_schema_check_sql_type_integer(),
        created_casified: sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []


def test_contract_schema_extra_column(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect
    id_casified = sql_dialect.default_casify("id")
    size_casified = sql_dialect.default_casify("size")
    distance_casified = sql_dialect.default_casify("distance")
    created_casified = sql_dialect.default_casify("created")

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: {DataType.TEXT}
              - name: size
                data_type: {DataType.DECIMAL}
              - name: created
                data_type: {DataType.DATE}
        """,
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        id_casified: sql_dialect.get_schema_check_sql_type_text(),
        size_casified: sql_dialect.get_schema_check_sql_type_decimal(),
        distance_casified: sql_dialect.get_schema_check_sql_type_integer(),
        created_casified: sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == [distance_casified]
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    assert "column 'distance' was present and not allowed" in str(contract_result).lower()


def test_contract_schema_data_type_mismatch(data_source_test_helper: ContractDataSourceTestHelper):
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect
    id_casified = sql_dialect.default_casify("id")
    size_casified = sql_dialect.default_casify("size")
    distance_casified = sql_dialect.default_casify("distance")
    created_casified = sql_dialect.default_casify("created")

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_schema_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                data_type: WRONG_VARCHAR
              - name: size
                data_type: {DataType.DECIMAL}
              - name: distance
                data_type: {DataType.INTEGER}
              - name: created
                data_type: {DataType.DATE}
        """,
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        id_casified: sql_dialect.get_schema_check_sql_type_text(),
        size_casified: sql_dialect.get_schema_check_sql_type_decimal(),
        distance_casified: sql_dialect.get_schema_check_sql_type_integer(),
        created_casified: sql_dialect.get_schema_check_sql_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []

    data_type_mismatch = schema_check_result.columns_having_wrong_type[0]
    assert data_type_mismatch.column == id_casified
    assert data_type_mismatch.expected_data_type == "WRONG_VARCHAR"
    assert data_type_mismatch.actual_data_type == sql_dialect.get_schema_check_sql_type_text()

    assert f"column 'id': expected type 'wrong_varchar', but was " in str(contract_result).lower()
