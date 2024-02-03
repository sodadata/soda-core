from contracts.helpers.contract_test_tables import contracts_test_table
from contracts.helpers.test_connection import TestConnection

from soda.contracts.contract import CheckOutcome, ContractResult


def test_contract_row_count(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: size
          - name: distance
          - name: created
        checks:
          - type: row_count
            fail_when_not_between: [100, 120]
    """
    )
    assert "Actual row_count was 3" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "row_count"
    assert measurement.value == 3
    assert measurement.type == "numeric"
