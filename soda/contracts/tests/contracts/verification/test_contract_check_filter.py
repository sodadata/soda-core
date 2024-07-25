from contracts.helpers.test_data_source import ContractDataSourceTestHelper
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import MetricCheck, MetricCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult

contracts_check_filter_test_table = TestTable(
    name="contracts_check_filter",
    # fmt: off
    columns=[
        ("id", DataType.TEXT),
        ("country", DataType.TEXT),
        ("currency", DataType.TEXT),
    ],
    values=[
        ('1', 'UK', 'euros'),
        ('2', 'UK', 'pounds'),
        ('3', 'USA', 'dollars'),
        ('4', 'USA', 'pounds'),
    ]
    # fmt: on
)


def test_contract_check_filter(data_source_test_helper: ContractDataSourceTestHelper):
    table_name: str = data_source_test_helper.ensure_test_table(contracts_check_filter_test_table)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
        - name: id
        - name: country
        - name: currency
          checks:
          - type: no_invalid_values
            valid_values: ['pounds']
            filter_sql: country = 'UK'
    """
    )
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "no_invalid_values"
    assert check.metric == "invalid_count"
    assert check.column == "currency"

    assert "Actual invalid_count(currency) was 1" in str(contract_result)
