import logging

from contracts.helpers.contract_test_tables import contracts_test_table
from contracts.helpers.test_connection import TestConnection
from soda.contracts.contract import ContractResult, SchemaCheckResult, CheckOutcome, Measurement

logger = logging.getLogger(__name__)


def test_contract_schema_pass_with_data_types(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(f"""
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
    """)

    schema_check_result: SchemaCheckResult = contract_result.check_results[0]
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []
    assert schema_check_result.check.name == "Schema"
    assert schema_check_result.check.location is None
    measurement: Measurement = schema_check_result.measurements[0]
    assert measurement.name == "schema"
    assert measurement.type == "schema"
    assert measurement.value == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date()
    }


def test_contract_schema_pass_without_data_types(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: size
          - name: distance
          - name: created
    """)

    schema_check_result: SchemaCheckResult = contract_result.check_results[0]
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []
    assert schema_check_result.check.name == "Schema"
    assert schema_check_result.check.location is None
    measurement: Measurement = schema_check_result.measurements[0]
    assert measurement.name == "schema"
    assert measurement.type == "schema"
    assert measurement.value == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date()
    }


def test_contract_schema_missing_column(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
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
    """)

    schema_check_result: SchemaCheckResult = contract_result.check_results[0]
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == ["themissingcolumn"]
    assert schema_check_result.columns_having_wrong_type == []
    assert schema_check_result.check.name == "Schema"
    assert schema_check_result.check.location is None
    measurement: Measurement = schema_check_result.measurements[0]
    assert measurement.name == "schema"
    assert measurement.type == "schema"
    assert measurement.value == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date()
    }
    assert "Column 'themissingcolumn' was missing" in str(contract_result)


def test_contract_schema_missing_optional_column(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(f"""
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
    """)

    schema_check_result: SchemaCheckResult = contract_result.check_results[0]
    assert schema_check_result.outcome == CheckOutcome.PASS
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []
    assert schema_check_result.check.name == "Schema"
    assert schema_check_result.check.location is None
    measurement: Measurement = schema_check_result.measurements[0]
    assert measurement.name == "schema"
    assert measurement.type == "schema"
    assert measurement.value == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date()
    }


def test_contract_schema_extra_column(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
            data_type: {test_connection.data_type_text()}
          - name: size
            data_type: {test_connection.data_type_decimal()}
          - name: created
            data_type: {test_connection.data_type_date()}
    """)

    schema_check_result: SchemaCheckResult = contract_result.check_results[0]
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.columns_not_allowed_and_present == ["distance"]
    assert schema_check_result.columns_required_and_not_present == []
    assert schema_check_result.columns_having_wrong_type == []
    assert schema_check_result.check.name == "Schema"
    assert schema_check_result.check.location is None
    measurement: Measurement = schema_check_result.measurements[0]
    assert measurement.name == "schema"
    assert measurement.type == "schema"
    assert measurement.value == {
        "id": test_connection.data_type_text(),
        "size": test_connection.data_type_decimal(),
        "distance": test_connection.data_type_integer(),
        "created": test_connection.data_type_date()
    }
    assert "Column 'distance' was present and not allowed" in str(contract_result)


def test_contract_schema_data_type_mismatch(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
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
    """)

    schema_check_result: SchemaCheckResult = contract_result.check_results[0]
    assert schema_check_result.outcome == CheckOutcome.FAIL
    assert schema_check_result.columns_not_allowed_and_present == []
    assert schema_check_result.columns_required_and_not_present == []
    data_type_mismatch = schema_check_result.columns_having_wrong_type[0]
    assert data_type_mismatch.column == "id"
    assert data_type_mismatch.expected_data_type == "WRONG_VARCHAR"
    assert data_type_mismatch.actual_data_type == test_connection.data_type_text()
    assert "Column 'id': Expected type 'WRONG_VARCHAR', but was 'character varying'" in str(contract_result)
