from datetime import datetime

from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import CheckOutcome, ContractResult

contracts_filter_test_table = TestTable(
    name="contracts_filter",
    columns=[
        ("id", DataType.TEXT),
        ("created", DataType.DATE),
    ],
    # fmt: off
    values=[
        ('1',  datetime(2020, 6, 23, 12, 45)),
        ('2',  datetime(2020, 6, 23, 12, 45)),
        ('3',  datetime(2021, 6, 23, 12, 45)),
    ]
    # fmt: on
)


def test_contract_filter_row_count(test_connection: TestConnection, environ: dict):
    table_name: str = test_connection.ensure_test_table(contracts_filter_test_table)

    filter_start_time = datetime(2021, 1, 1, 1, 1, 1)
    environ["FILTER_START_TIME"] = test_connection.data_source.literal_datetime(filter_start_time)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        sql_filter: |
          created > ${{FILTER_START_TIME}}
        columns:
          - name: id
          - name: created
        checks:
          - type: row_count
            fail_when_less_than: 10
    """
    )
    assert "Actual row_count was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "row_count"
    assert measurement.value == 1
    assert measurement.type == "numeric"
