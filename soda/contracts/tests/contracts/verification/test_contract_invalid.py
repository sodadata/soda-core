from conftest import get_parse_errors_str
from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import CheckOutcome, ContractResult, NumericMeasurement

contracts_invalid_test_table = TestTable(
    name="contracts_invalid",
    # fmt: off
    columns=[
        ("id", DataType.TEXT)
    ],
    values=[
        ('ID1',),
        ('XXX',),
        ('N/A',),
        (None,),
    ]
    # fmt: on
)


def test_contract_no_invalid_with_valid_values_pass(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_invalid_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: no_invalid
              valid_length: 3
    """
    )

    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "id"
    assert measurement.metric == "invalid_count"
    assert measurement.value == 0


def test_contract_no_invalid_with_valid_values_fail(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_invalid_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: no_invalid
              valid_values: ['ID1']
    """
    )

    assert "Actual invalid_count(id) was 2" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "id"
    assert measurement.metric == "invalid_count"
    assert measurement.value == 2


def test_no_invalid_with_threshold():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: id
              checks:
                - type: no_invalid
                  valid_values: ['ID1']
                  must_be: 0
        """
    )

    assert "Check type 'no_invalid' does not allow for threshold keys must_be_..." in errors_str


def test_no_invalid_without_valid_configuration():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: id
              checks:
                - type: no_invalid
        """
    )

    assert "Check type 'no_invalid' must have a validity configuration like" in errors_str


def test_contract_invalid_count_pass(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_invalid_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
              - type: invalid_count
                valid_values: ['ID1']
                must_be: 2
    """
    )
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "id"
    assert measurement.metric == "invalid_count"
    assert measurement.value == 2


def test_contract_invalid_count_fail(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_invalid_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
              - type: invalid_count
                valid_values: ['ID1']
                must_be: 0
    """
    )
    assert "Actual invalid_count(id) was 2" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "id"
    assert measurement.metric == "invalid_count"
    assert measurement.value == 2


def test_contract_missing_and_invalid_values_pass(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_invalid_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
              - type: missing_count
                missing_values: ['N/A']
                must_be: 2
              - type: invalid_count
                valid_values: ['ID1']
                must_be: 1
    """
    )
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "id"
    assert measurement.metric == "missing_count"
    assert measurement.value == 2

    check_result = contract_result.check_results[2]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "id"
    assert measurement.metric == "invalid_count"
    assert measurement.value == 1


contracts_invalid_multi_test_table = TestTable(
    name="contracts_missing_multi_test_table",
    # fmt: off
    columns=[
        ("id", DataType.TEXT)
    ],
    values=[
        ('ID1',),
        ('XXX',),
        ('N/A',),
        ('1234567890',),
        (None,),
    ]
    # fmt: on
)


def test_contract_multi_validity_configs(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_invalid_multi_test_table)

    # AND logic is applied between all the specified validity configs
    # So ALL of the validity constraints have to be met

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
              - type: invalid_count
                valid_values: ['ID1', 'XXX', '1234567890' ]
                valid_max_length: 4
                must_be: 2
    """
    )
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "id"
    assert measurement.metric == "invalid_count"
    assert measurement.value == 2


contract_refs_test_table = TestTable(
    name="contract_refs",
    columns=[("id", DataType.TEXT), ("contract_id", DataType.TEXT)],
    # fmt: off
    values=[
        ('1', 'ID1'),
        ('2', 'ID-BUZZZ'),
        ('2', 'Undefined'),
        ('3', None),
    ]
    # fmt: on
)


def test_contract_column_invalid_reference_check(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contract_refs_test_table)
    customers_table_name: str = test_connection.ensure_test_table(contracts_invalid_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: contract_id
            checks:
              - type: no_invalid
                valid_values_reference_data:
                    dataset: {customers_table_name}
                    column: id
                samples_limit: 20
    """
    )
    assert "Actual invalid_count(contract_id) was 2" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "contract_id"
    assert measurement.metric == "invalid_count"
    assert measurement.value == 2
