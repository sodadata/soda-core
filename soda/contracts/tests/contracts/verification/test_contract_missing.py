from contracts.helpers.contract_parse_errors import get_parse_errors_str
from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import (
    CheckOutcome,
    ContractResult,
    NumericMetricCheck,
    NumericMetricCheckResult,
)

contracts_missing_test_table = TestTable(
    name="contracts_missing",
    # fmt: off
    columns=[
        ("one", DataType.TEXT),
        ("two", DataType.TEXT)
    ],
    values=[
        ('ID1', 'ID1'),
        ('N/A', 'ID2'),
        (None,  'ID3'),
    ]
    # fmt: on
)


def test_no_missing_with_threshold():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
              checks:
                - type: no_missing_values
                  must_be: 5
        """
    )

    assert "Check type 'no_missing_values' does not allow for threshold keys must_..." in errors_str


def test_missing_count_without_threshold():
    errors_str = get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
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
          - name: one
            checks:
            - type: no_missing_values
          - name: two
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "no_missing_values"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "one"

    assert "Actual missing_count(one) was 1" in str(contract_result)


def test_contract_nomissing_without_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: one
          - name: two
            checks:
            - type: no_missing_values
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 0

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "no_missing_values"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "two"


def test_contract_missing_count_with_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: missing_count
              must_be: 0
          - name: two
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "missing_count"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "one"

    assert "Actual missing_count(one) was 1" in str(contract_result)


def test_contract_missing_count_pass(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: missing_count
              must_be_less_than: 10
          - name: two
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "missing_count"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "one"


def test_contract_missing_count_with_missing_values_pass(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: missing_count
              missing_values: ['N/A']
              must_be: 2
          - name: two
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "missing_count"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "one"


def test_contract_missing_count_with_missing_sql_regex(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: missing_count
              missing_sql_regex: ^N/A$
              must_be: 0
          - name: two
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "missing_count"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "one"

    assert "Actual missing_count(one) was 2" in str(contract_result)


def test_contract_missing_count_name_and_threshold(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: missing_count
              name: Missing values count must be between 0 and 3
              must_be_between: [0, 3]
          - name: two
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "missing_count"
    assert check.name == "Missing values count must be between 0 and 3"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "one"
