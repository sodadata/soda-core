from contracts.helpers.test_connection import TestDataSource
from helpers.test_table import TestTable
from soda.contracts.check import MetricCheck, MetricCheckResult
from soda.execution.data_type import DataType

from soda.contracts.contract import (
    CheckOutcome,
    ContractResult,
)

contracts_basic_sql_functions_check_types_test_table = TestTable(
    name="contracts_basic_sql_functions_check_type",
    # fmt: off
    columns=[
        ("one", DataType.DECIMAL)
    ],
    values=[
        (1, ),
        (2, ),
        (3, ),
        (None,),
    ]
    # fmt: on
)


def test_contract_avg(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_basic_sql_functions_check_types_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
              - type: avg
                must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "avg"
    assert check.metric == "avg"
    assert check.column == "one"

    assert "Actual avg(one) was 2" in str(contract_result)


def test_contract_sum(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_basic_sql_functions_check_types_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
              - type: sum
                must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 6

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "sum"
    assert check.metric == "sum"
    assert check.column == "one"

    assert "Actual sum(one) was 6" in str(contract_result)
