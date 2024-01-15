from contracts.helpers.schema_table import contracts_test_table
from contracts.helpers.test_connection import TestConnection
from soda.contracts.contract import ContractResult


def test_contract_schema_data_type_mismatch(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: missing
          - name: size
          - name: distance
          - name: created
    """)
