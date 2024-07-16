from contracts.helpers.test_data_source import DataSourceTestHelper
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import MetricCheckResult, UserDefinedMetricExpressionCheck
from soda.contracts.contract import CheckOutcome, ContractResult

user_defined_metric_expression_test_table = TestTable(
    name="user_defined_metric_expression",
    # fmt: off
    columns=[
        ("id", DataType.INTEGER),
        ("country", DataType.TEXT)
    ],
    values=[
        (1, 'US'),
        (2, 'US'),
        (3, 'BE'),
    ]
    # fmt: on
)


def test_contract_column_metric_expression(data_source_test_helper: DataSourceTestHelper):
    table_name: str = data_source_test_helper.ensure_test_table(user_defined_metric_expression_test_table)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
            checks:
            - type: metric_expression
              metric: us_count
              expression_sql: COUNT(CASE WHEN country = 'US' THEN 1 END)
              must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricExpressionCheck)
    assert check.type == "metric_expression"
    assert check.metric == "us_count"
    assert check.column == "country"

    assert "Actual us_count(country) was 2" in str(contract_result)


def test_contract_dataset_metric_expression(data_source_test_helper: DataSourceTestHelper):
    table_name: str = data_source_test_helper.ensure_test_table(user_defined_metric_expression_test_table)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
        checks:
        - type: metric_expression
          metric: us_count
          expression_sql: COUNT(CASE WHEN country = 'US' THEN 1 END)
          must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricExpressionCheck)
    assert check.type == "metric_expression"
    assert check.metric == "us_count"
    assert check.column is None

    assert "Actual us_count was 2" in str(contract_result)
