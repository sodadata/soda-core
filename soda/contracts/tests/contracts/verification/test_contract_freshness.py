from datetime import datetime, timezone

from contracts.helpers.test_connection import TestConnection
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import CheckOutcome, ContractResult

contracts_freshness_test_table = TestTable(
    name="contracts_freshness",
    columns=[
        ("id", DataType.TEXT),
        ("created", DataType.TIMESTAMP_TZ),
    ],
    # fmt: off
    values=[
        ('1',  datetime(2020, 1, 1, 10, 10, 10, tzinfo=timezone.utc)),
        ('2',  datetime(2020, 1, 1, 10, 10, 10, tzinfo=timezone.utc)),
        ('3',  datetime(2021, 1, 1, 10, 10, 10, tzinfo=timezone.utc)),
    ]
    # fmt: on
)


def test_contract_freshness_pass(test_connection: TestConnection, environ: dict):
    table_name: str = test_connection.ensure_test_table(contracts_freshness_test_table)

    variables: dict[str, str] = {"NOW": "2021-01-01 12:30"}

    contract_result: ContractResult = test_connection.assert_contract_pass(
        contract_yaml_str=f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: created
            checks:
            - type: freshness_in_hours
              fail_when_greater_than_or_equal: 3
    """,
        variables=variables,
    )


def test_contract_freshness_fail(test_connection: TestConnection, environ: dict):
    table_name: str = test_connection.ensure_test_table(contracts_freshness_test_table)

    variables: dict[str, str] = {"NOW": "2021-01-01 13:30"}

    contract_result: ContractResult = test_connection.assert_contract_fail(
        contract_yaml_str=f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: created
            checks:
            - type: freshness_in_hours
              fail_when_greater_than_or_equal: 3
    """,
        variables=variables,
    )
    contract_result_str = str(contract_result)
    assert "Expected freshness(created) < 3h" in contract_result_str
    assert "freshness was 3:19:50" in contract_result_str
    assert "freshness_column_max_value was 2021-01-01 10:10:10+00:00" in contract_result_str
    assert "freshness_column_max_value_utc was 2021-01-01 10:10:10+00:00" in contract_result_str
    assert "now was 2021-01-01 13:30" in contract_result_str
    assert "now_utc was 2021-01-01 13:30:00+00:00" in contract_result_str
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
