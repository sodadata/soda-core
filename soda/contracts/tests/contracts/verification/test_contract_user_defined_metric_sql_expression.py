from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.contracts.contract import CheckOutcome, ContractResult, NumericMetricCheckResult, \
    UserDefinedMetricSqlExpressionCheck
from soda.execution.data_type import DataType

user_defined_metric_sql_expression_test_table = TestTable(
    name="user_defined_metric_sql_expression",
    columns=[("id", DataType.TEXT), ("country", DataType.TEXT)],
    # fmt: off
    values=[
        ('1', 'US'),
        ('2', 'US'),
        ('3', 'BE'),
    ]
    # fmt: on
)


def test_contract_column_metric_sql_expression(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(user_defined_metric_sql_expression_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
            checks:
            - type: metric_sql_expression
              metric: us_count
              expression: COUNT(CASE WHEN country = 'US' THEN 1 END)
              must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricSqlExpressionCheck)
    assert check.type == 'metric_sql_expression'
    assert check.metric == 'us_count'
    assert check.column == 'country'

    assert "Actual us_count(country) was 2" in str(contract_result)


def test_contract_dataset_metric_sql_expression(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(user_defined_metric_sql_expression_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: country
        checks:
        - type: metric_sql_expression
          metric: us_count
          expression: COUNT(CASE WHEN country = 'US' THEN 1 END)
          must_be: 0
    """
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricSqlExpressionCheck)
    assert check.type == 'metric_sql_expression'
    assert check.metric == 'us_count'
    assert check.column is None

    assert "Actual us_count was 2" in str(contract_result)
