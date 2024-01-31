from contracts.helpers.contract_test_tables import contracts_test_table
from contracts.helpers.test_connection import TestConnection
from soda.contracts.contract import ContractResult, CheckOutcome


def test_contract_unique(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
              - type: unique
          - name: size
            checks:
              - type: duplicate_count
          - name: distance
          - name: created
    """)
    assert "Actual duplicate_count(size) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.PASS
    assert check_result.check.column == "id"
    measurement = check_result.measurements[0]
    assert measurement.name == "duplicate_count(id)"
    assert measurement.value == 0
    assert measurement.type == "numeric"
    check_result = contract_result.check_results[2]
    assert check_result.outcome == CheckOutcome.FAIL
    assert check_result.check.column == "size"
    measurement = check_result.measurements[0]
    assert measurement.name == "duplicate_count(size)"
    assert measurement.value == 1
    assert measurement.type == "numeric"
