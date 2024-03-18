from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import (
    CheckOutcome,
    ContractResult,
    NumericMetricCheckResult,
    UserDefinedMetricSqlExpressionCheck,
    UserDefinedMetricSqlQueryCheck,
)

user_defined_metric_sql_query_test_table = TestTable(
    name="user_defined_sql_query",
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


def test_contract_user_defined_sql_query(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(user_defined_metric_sql_query_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
        checks:
          - type: metric_sql_query
            metric: us_count
            sql_query: |
              SELECT COUNT(*)
              FROM {table_name}
              WHERE country = 'US'
            must_be_not_between: [0, 5]
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricSqlQueryCheck)
    assert check.type == "metric_sql_query"
    assert check.metric == "us_count"
    assert check.column is None

    assert "Actual us_count was 2" in str(contract_result)
