from contracts.helpers.test_data_source import TestDataSource
from helpers.test_table import TestTable
from soda.contracts.check import MetricCheck, MetricCheckResult
from soda.execution.data_type import DataType

from soda.contracts.contract import (
    CheckOutcome,
    ContractResult,
)

contracts_duplicate_test_table = TestTable(
    name="contracts_duplicate",
    # fmt: off
    columns=[
        ("one", DataType.TEXT)
    ],
    values=[
        ('1', ),
        ('1', ),
        ('2', ),
        (None,),
    ]
    # fmt: on
)


def test_contract_no_duplicate_values(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_duplicate_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
              - type: no_duplicate_values
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "no_duplicate_values"
    assert check.metric == "duplicate_count"
    assert check.column == "one"

    assert "Actual duplicate_count(one) was 1" in str(contract_result)


def test_contract_duplicate_count(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_duplicate_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
              - type: duplicate_count
                must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "duplicate_count"
    assert check.metric == "duplicate_count"
    assert check.column == "one"

    assert "Actual duplicate_count(one) was 1" in str(contract_result)


def test_contract_duplicate_percent(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_duplicate_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
              - type: duplicate_percent
                must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 25

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "duplicate_percent"
    assert check.metric == "duplicate_percent"
    assert check.column == "one"

    assert "Actual duplicate_percent(one) was 25" in str(contract_result)
