import logging
from textwrap import dedent


from soda.contracts.contract import (
    CheckOutcome,
    ContractResult,
    NumericMetricCheck,
    NumericMetricCheckResult, Contract,
)
from soda.execution.data_type import DataType
from tests.helpers.test_table import TestTable

contracts_missing_test_table = TestTable(
    name="contracts_skip",
    # fmt: off
    columns=[
        ("one", DataType.TEXT),
    ],
    values=[
    ]
    # fmt: on
)


def test_no_missing_with_threshold(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_missing_test_table)

    contract_yaml_str: str = dedent(f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: no_missing_values
    """).strip()


    contract_yaml_str = dedent(contract_yaml_str).strip()
    logging.debug(contract_yaml_str)
    contract: Contract = Contract.from_yaml_str(contract_yaml_str=contract_yaml_str)
    for check in contract.checks:
        if check.type != "schema":
            check.skip()

    contract_result: ContractResult = contract.verify(connection=test_connection)

    check_result = contract_result.check_results[1]
    assert isinstance(check_result, NumericMetricCheckResult)
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.metric_value == 1

    check = check_result.check
    assert isinstance(check, NumericMetricCheck)
    assert check.type == "no_missing_values"
    assert check.metric == "missing_count"
    assert check.dataset == table_name
    assert check.column == "one"

    assert "Actual missing_count(one) was 1" in str(contract_result)
