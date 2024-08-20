from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import MetricCheckResult, UserDefinedMetricQueryCheck
from soda.contracts.contract import CheckOutcome, ContractResult

user_defined_metric_query_sql_test_table = TestTable(
    name="user_defined_metric_query_sql",
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


def test_contract_metric_query_on_column(data_source_test_helper: ContractDataSourceTestHelper):
    unique_table_name: str = data_source_test_helper.ensure_test_table(user_defined_metric_query_sql_test_table)
    qualified_table_name: str = data_source_test_helper.contract_data_source.qualify_table(unique_table_name)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=user_defined_metric_query_sql_test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                - type: metric_query
                  metric: us_count
                  query_sql: |
                    SELECT COUNT(*)
                    FROM {qualified_table_name}
                    WHERE country = 'US'
                  must_be_not_between: [0, 5]
              - name: country
        """,
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 2

    check = check_result.check
    assert isinstance(check, UserDefinedMetricQueryCheck)
    assert check.type == "metric_query"
    assert check.metric == "us_count"
    assert check.column.lower() == "id"

    assert "actual us_count(id) was 2" in str(contract_result).lower()


def test_contract_metric_query_on_dataset(data_source_test_helper: ContractDataSourceTestHelper):
    unique_table_name: str = data_source_test_helper.ensure_test_table(user_defined_metric_query_sql_test_table)
    qualified_table_name: str = data_source_test_helper.contract_data_source.qualify_table(unique_table_name)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=user_defined_metric_query_sql_test_table,
        contract_yaml_str=f"""
        columns:
          - name: id
          - name: country
        checks:
          - type: metric_query
            metric: us_count
            query_sql: |
              SELECT COUNT(*)
              FROM {qualified_table_name}
              WHERE country = 'US'
            must_be_not_between: [0, 5]
    """,
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

    assert "actual us_count was 2" in str(contract_result).lower()
