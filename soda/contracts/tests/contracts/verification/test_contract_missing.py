from contracts.helpers.contract_test_tables import contracts_test_table
from contracts.helpers.test_connection import TestConnection

from soda.contracts.contract import CheckOutcome, ContractResult


def test_contract_missing_default(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: missing
          - name: size
          - name: distance
          - name: created
    """
    )
    assert "Actual missing_count(id) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "missing_count(id)"
    assert measurement.value == 1
    assert measurement.type == "numeric"


def test_contract_missing_count_with_missing_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: missing_count
              missing_values: ['N/A']
          - name: size
          - name: distance
          - name: created
    """
    )
    assert "Actual missing_count(id) was 2" in str(contract_result)


def test_contract_missing_count_with_missing_regex(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: missing_count
              missing_regex: ^N/A$
          - name: size
          - name: distance
          - name: created
    """
    )
    assert "Actual missing_count(id) was 2" in str(contract_result)


def test_contract_missing_count_name_and_threshold(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_pass(
        f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
            - type: missing_count
              name: Volume
              fail_when_not_between: [0, 3]
          - name: size
          - name: distance
          - name: created
    """
    )

    assert contract_result.check_results[1].check.name == "Volume"
