from conftest import get_parse_errors_str
from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import CheckOutcome, ContractResult, NumericMeasurement

contracts_missing_test_table = TestTable(
    name="contracts_missing",
    # fmt: off
    columns=[
        ("colinv", DataType.TEXT),
        ("colval", DataType.TEXT)
    ],
    values=[
        ('ID1', 'ID1'),
        ('N/A', 'ID2'),
        (None,  'ID3'),
    ]
    # fmt: on
)


def test_no_missing_with_threshold():
    errors_str = get_parse_errors_str("""
          dataset: TABLE_NAME
          columns:
            - name: id
              checks:
                - type: no_missing
                  must_be: 5
        """
    )

    assert "Check type 'no_missing' does not allow for threshold keys must_be_..." in errors_str


def test_missing_count_without_threshold():
    errors_str = get_parse_errors_str("""
          dataset: TABLE_NAME
          columns:
            - name: id
              checks:
                - type: missing_count
        """
    )

    assert "Check type 'missing_count' requires threshold configuration" in errors_str


def test_contract_nomissing_with_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: holes
            checks:
            - type: no_missing
          - name: solid
    """
    )

    assert "Actual missing_count(holes) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "holes"
    assert measurement.metric == "missing_count"
    assert measurement.value == 1


def test_contract_nomissing_without_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: holes
          - name: solid
            checks:
            - type: no_missing
    """
    )

    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "solid"
    assert measurement.metric == "missing_count"
    assert measurement.value == 0


def test_contract_missing_count_with_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: holes
            checks:
            - type: missing_count
              must_be: 0
          - name: solid
    """
    )

    assert "Actual missing_count(holes) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "holes"
    assert measurement.metric == "missing_count"
    assert measurement.value == 1


def test_contract_missing_count_pass(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: holes
            checks:
            - type: missing_count
              must_be_less_than: 10
          - name: solid
    """
    )

    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "holes"
    assert measurement.metric == "missing_count"
    assert measurement.value == 1


def test_contract_missing_count_with_missing_values_pass(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: holes
            checks:
            - type: missing_count
              missing_values: ['N/A']
              must_be: 2
          - name: solid
    """
    )

    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    measurement = check_result.measurements[0]
    assert isinstance(measurement, NumericMeasurement)
    assert measurement.dataset == table_name
    assert measurement.column == "holes"
    assert measurement.metric == "missing_count"
    assert measurement.value == 2


def test_contract_missing_count_with_missing_regex(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: holes
            checks:
            - type: missing_count
              missing_regex: ^N/A$
              must_be: 0
          - name: solid
    """
    )
    assert "Actual missing_count(holes) was 2" in str(contract_result)


def test_contract_missing_count_name_and_threshold(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: holes
            checks:
            - type: missing_count
              name: Volume
              must_be_between: [0, 3]
          - name: solid
    """
    )

    assert contract_result.check_results[1].check.name == "Volume"
