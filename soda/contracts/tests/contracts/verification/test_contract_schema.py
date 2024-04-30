import logging

from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import CheckOutcome, ContractResult, SchemaCheckResult

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


def test_contract_schema_pass_with_data_types(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_schema_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            data_type: {test_connection.data_type_text()}
          - name: size
            data_type: {test_connection.data_type_decimal()}
          - name: distance
            data_type: {test_connection.data_type_integer()}
          - name: created
            data_type: {test_connection.data_type_date()}
    """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check = schema_check_result.check
    assert check.name == "Schema"
    assert check.location is None
    assert check.dataset == table_name
    assert check.column is None


def test_contract_schema_pass_without_data_types(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_schema_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: size
          - name: distance
          - name: created
    """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check = schema_check_result.check
    assert check.name == "Schema"
    assert check.location is None
    assert check.dataset == table_name
    assert check.column is None


def test_contract_schema_missing_column(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_schema_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            data_type: {test_connection.data_type_text()}
          - name: size
            data_type: {test_connection.data_type_decimal()}
          - name: distance
            data_type: {test_connection.data_type_integer()}
          - name: themissingcolumn
            data_type: {test_connection.data_type_text()}
          - name: created
            data_type: {test_connection.data_type_date()}
    """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == ["themissingcolumn"]
    assert schema_check_result.columns_having_wrong_type == []

    check = schema_check_result.check
    assert check.name == "Schema"
    assert check.location is None
    assert check.dataset == table_name
    assert check.column is None

    assert "Column 'themissingcolumn' was missing" in str(contract_result)


def test_contract_schema_missing_optional_column(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_schema_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            data_type: {test_connection.data_type_text()}
          - name: size
            data_type: {test_connection.data_type_decimal()}
          - name: distance
            data_type: {test_connection.data_type_integer()}
          - name: themissingcolumn
            data_type: {test_connection.data_type_text()}
            optional: true
          - name: created
            data_type: {test_connection.data_type_date()}
    """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.measured_schema == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check = schema_check_result.check
    assert check.name == "Schema"
    assert check.location is None
    assert check.dataset == table_name
    assert check.column is None


def test_contract_schema_extra_column(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_schema_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            data_type: {test_connection.data_type_text()}
          - name: size
            data_type: {test_connection.data_type_decimal()}
          - name: created
            data_type: {test_connection.data_type_date()}
    """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == ["distance"]
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []

    check = schema_check_result.check
    assert check.name == "Schema"
    assert check.location is None
    assert check.dataset == table_name
    assert check.column is None

    assert "Column 'distance' was present and not allowed" in str(contract_result)


def test_contract_schema_data_type_mismatch(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_schema_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            data_type: WRONG_VARCHAR
          - name: size
            data_type: {test_connection.data_type_decimal()}
          - name: distance
            data_type: {test_connection.data_type_integer()}
          - name: created
            data_type: {test_connection.data_type_date()}
    """
    )

    schema_check_result = contract_result.check_results[0]
    assert isinstance(schema_check_result, SchemaCheckResult)
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.measured_schema == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date(),
    }
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []

    data_type_mismatch = schema_check_result.columns_having_wrong_type[0]
    assert data_type_mismatch.column == "id"
    assert data_type_mismatch.expected_data_type == "WRONG_VARCHAR"
    assert data_type_mismatch.actual_data_type == test_connection.data_type_text()

    check = schema_check_result.check
    assert check.name == "Schema"
    assert check.location is None
    assert check.dataset == table_name
    assert check.column is None

    assert "Column 'id': Expected type 'WRONG_VARCHAR', but was 'character varying'" in str(contract_result)
