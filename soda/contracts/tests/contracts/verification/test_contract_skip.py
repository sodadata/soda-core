import logging
from textwrap import dedent

from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.check import SchemaCheckResult
from soda.contracts.contract import CheckOutcome, ContractResult
from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationResult,
)
from soda.contracts.impl.sql_dialect import SqlDialect

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
    sql_dialect: SqlDialect = data_source_test_helper.contract_data_source.sql_dialect

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

    contract_yaml_str = data_source_test_helper.casify_contract_yaml_str(
        test_table=contracts_missing_test_table, contract_yaml_str=contract_yaml_str
    )

    contract_yaml_str = dedent(contract_yaml_str).strip()

    logging.debug(contract_yaml_str)

    contract_verification: ContractVerification = (
        data_source_test_helper.create_test_verification_builder()
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
