from contracts.helpers.schema_table import contracts_test_table
from contracts.helpers.test_connection import TestConnection
from soda.contracts.contract import ContractResult, CheckOutcome


def test_contract_missing_default(test_connection: TestConnection):
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
    assert "Measurement missing_count(id) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "missing_count(id)"
    assert measurement.value == 1
    assert measurement.type == "numeric"


def test_contract_missing_count_with_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: missing_count
              missing_values: ['N/A']
          - name: size
          - name: distance
          - name: created
    """)
    assert "Measurement missing_count(id) was 2" in str(contract_result)
