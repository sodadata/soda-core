from datetime import datetime, timezone

from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import FreshnessCheckResult
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


def test_contract_freshness_pass(data_source_test_helper: ContractDataSourceTestHelper, environ: dict):
    variables: dict[str, str] = {"NOW": "2021-01-01 12:30"}

    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_freshness_test_table,
        contract_yaml_str=f"""
        columns:
          - name: id
          - name: created
            checks:
            - type: freshness_in_hours
              must_be_less_than: 3
    """,
        variables=variables,
    )

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, FreshnessCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.freshness == "2:19:50"


def test_contract_freshness_fail(data_source_test_helper: ContractDataSourceTestHelper, environ: dict):
    variables: dict[str, str] = {"NOW": "2021-01-01 13:30"}

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=contracts_freshness_test_table,
        contract_yaml_str=f"""
        columns:
          - name: id
          - name: created
            checks:
            - type: freshness_in_hours
              must_be_less_than: 3
    """,
        variables=variables,
    )
    contract_result_str = str(contract_result)

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, FreshnessCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.freshness == "3:19:50"

    assert "expected freshness(created) < 3h" in contract_result_str.lower()
    assert "actual freshness(created) was 3:19:50" in contract_result_str.lower()
    assert "max value in column was ...... 2021-01-01 10:10:10" in contract_result_str.lower()
    assert "max value in column in utc was 2021-01-01 10:10:10" in contract_result_str.lower()
    assert "now was ...................... 2021-01-01 13:30" in contract_result_str.lower()
    assert "now in utc was ............... 2021-01-01 13:30:00" in contract_result_str.lower()
