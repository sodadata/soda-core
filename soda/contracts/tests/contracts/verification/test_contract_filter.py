from datetime import datetime

from contracts.helpers.contract_data_source_test_helper import ContractDataSourceTestHelper
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import MetricCheck, MetricCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult

contracts_filter_test_table = TestTable(
    name="contracts_filter",
    # fmt: off
    columns=[
        ("id", DataType.TEXT),
        ("created", DataType.DATE),
    ],
    values=[
        ('1',  datetime(2020, 6, 23, 12, 45)),
        ('2',  datetime(2020, 6, 23, 12, 45)),
        ('3',  datetime(2021, 6, 23, 12, 45)),
    ]
    # fmt: on
)


def test_contract_filter_row_count(data_source_test_helper: ContractDataSourceTestHelper, environ: dict):
    filter_start_time = datetime(2021, 1, 1, 1, 1, 1)
    sql_dialect = data_source_test_helper.contract_data_source.sql_dialect
    environ["FILTER_START_TIME"] = sql_dialect.literal_datetime(filter_start_time)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_filter_test_table,
        contract_yaml_str=f"""
            filter_sql: |
              created > ${{FILTER_START_TIME}}
            columns:
              - name: id
              - name: created
            checks:
              - type: row_count
                must_be: 0
        """
    )
    check_result = contract_result.check_results[1]
    assert isinstance(check_result, MetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, MetricCheck)
    assert check.type == "row_count"
    assert check.metric == "row_count"
    assert check.column is None

    assert "Actual row_count was 1" in str(contract_result)
