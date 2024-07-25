import logging
from textwrap import dedent

from contracts.helpers.test_data_source import TestContractVerification, ContractDataSourceTestHelper
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import SchemaCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult
from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationResult,
)

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


def test_skip_all_checks_except_schema_check(data_source_test_helper: ContractDataSourceTestHelper):
    table_name: str = data_source_test_helper.ensure_test_table(contracts_missing_test_table)

    contract_yaml_str: str = dedent(
        f"""
        dataset: {table_name}
        columns:
          - name: one
            checks:
            - type: no_missing_values
    """
    ).strip()

    contract_yaml_str = dedent(contract_yaml_str).strip()
    logging.debug(contract_yaml_str)

    contract_verification: ContractVerification = (
        TestContractVerification.builder()
        .with_data_source(test_data_source)
        .with_contract_yaml_str(contract_yaml_str=contract_yaml_str)
        .build()
    )

    contract = contract_verification.contracts[0]
    for check in contract.checks:
        if check.type != "schema":
            check.skip = True

    contract_verification_result: ContractVerificationResult = contract_verification.execute()
    contract_result: ContractResult = contract_verification_result.contract_results[0]

    check_result = contract_result.check_results[0]
    assert isinstance(check_result, SchemaCheckResult)
    assert check_result.outcome == CheckOutcome.PASS
