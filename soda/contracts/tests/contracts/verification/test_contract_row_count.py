from contracts.helpers.test_data_source import TestDataSource
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import MetricCheck, MetricCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult

contracts_row_count_test_table = TestTable(
    name="contracts_row_count",
    # fmt: off
    columns=[
        ("one", DataType.TEXT)
    ],
    values=[
        ('1', ),
        ('2', ),
        (None,),
    ]
    # fmt: on
)


def test_contract_row_count(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_row_count_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: one
        checks:
          - type: rows_exist
    """
    )
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.metric_value == 3

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "rows_exist"
    assert check.metric == "row_count"
    assert check.column is None


def test_contract_row_count2(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_row_count_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: one
        checks:
          - type: row_count
            must_be_between: [100, 120]
    """
    )
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 3

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "row_count"
    assert check.metric == "row_count"
    assert check.column is None

    assert "Actual row_count was 3" in str(contract_result)
