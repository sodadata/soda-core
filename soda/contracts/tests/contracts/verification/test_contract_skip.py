import logging
from textwrap import dedent

from contracts.helpers.test_connection import TestDataSource
from helpers.test_table import TestTable
from soda.contracts.check import MetricCheck, MetricCheckResult, SchemaCheckResult
from soda.contracts.contract import (
    CheckOutcome,
    ContractResult, Contract,
)
from soda.execution.data_type import DataType


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


def test_skip_all_checks_except_schema_check(test_data_source: TestDataSource):
    table_name: str = test_data_source.ensure_test_table(contracts_missing_test_table)

    contract_yaml_str: str = dedent(f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: no_missing_values
    """).strip()


    contract_yaml_str = dedent(contract_yaml_str).strip()
    logging.debug(contract_yaml_str)

    contract: Contract = (Contract
        .from_yaml_str(contract_yaml_str=contract_yaml_str)
        .with_data_source(test_data_source)
        .parse()
    )

    for check in contract.checks:
        if check.type != "schema":
            check.skip = True

    contract_result: ContractResult = contract.verify()

    check_result = contract_result.check_results[0]
    assert isinstance(check_result, SchemaCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
