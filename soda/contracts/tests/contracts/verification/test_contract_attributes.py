from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract import ContractResult

contracts_attributes_test_table = TestTable(
    name="contracts_attributes",
    # fmt: off
    columns=[
        ("one", DataType.TEXT),
    ],
    values=[
    ]
    # fmt: on
)


def test_contract_attributes(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=contracts_attributes_test_table,
        contract_yaml_str=f"""
        columns:
          - name: one
            checks:
            - type: no_invalid_values
              valid_values: ['ID1']
              attributes:
                pii: true
                category: missing_data
    """,
    )
