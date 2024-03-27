from contracts.helpers.test_connection import TestDataSource
from helpers.test_table import TestTable
from soda.contracts.check import MetricCheckResult, UserDefinedMetricQueryCheck
from soda.contracts.contract import (
    CheckOutcome,
    ContractResult,
)
from soda.execution.data_type import DataType

user_defined_metric_query_sql_test_table = TestTable(
    name="metric_query_query",
    # fmt: off
    columns=[
        ("id", DataType.TEXT),
        ("country", DataType.TEXT)
    ],
    values=[
        ('1', 'US'),
        ('2', 'US'),
        ('3', 'BE'),
    ]
    # fmt: on
)


def test_contract_metric_query_on_column(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(user_defined_metric_query_sql_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: metric_query
              metric: us_count
              query_sql: |
                SELECT COUNT(*)
                FROM {table_name}
                WHERE country = 'US'
              must_be_not_between: [0, 5]
          - name: country
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricQueryCheck)
    assert check.type == "metric_query"
    assert check.metric == "us_count"
    assert check.column == "id"

    assert "Actual us_count(id) was 2" in str(contract_result)


def test_contract_metric_query_on_dataset(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(user_defined_metric_query_sql_test_table)

    contract_result: ContractResult = test_data_source.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
        checks:
          - type: metric_query
            metric: us_count
            query_sql: |
              SELECT COUNT(*)
              FROM {table_name}
              WHERE country = 'US'
            must_be_not_between: [0, 5]
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricQueryCheck)
    assert check.type == "metric_query"
    assert check.metric == "us_count"
    assert check.column is None

    assert "Actual us_count was 2" in str(contract_result)
