from contracts.helpers.contract_test_tables import contracts_test_table, contract_refs_test_table
from contracts.helpers.test_connection import TestConnection
from soda.contracts.contract import ContractResult, CheckOutcome


def test_contract_avg(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: size
            checks:
              - type: avg
                fail_when_not_between: [10, 20]
          - name: distance
          - name: created
    """)
    assert "Actual avg(size) was 1.0" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "avg(size)"
    assert measurement.value == 1
    assert measurement.type == "numeric"


def test_contract_sum(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: size
            checks:
              - type: sum
                fail_when_not_between: [10, 20]
          - name: distance
          - name: created
    """)
    assert "Actual sum(size) was 3.0" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "sum(size)"
    assert measurement.value == 3
    assert measurement.type == "numeric"


def test_contract_missing_and_invalid_values(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
              - type: missing
                missing_values: ['N/A']
              - type: invalid
                valid_values: ['XXX']
          - name: size
          - name: distance
          - name: created
    """)
    assert "Actual invalid_count(id) was 1" in str(contract_result)
    check_result = contract_result.check_results[2]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "invalid_count(id)"
    assert measurement.value == 1
    assert measurement.type == "numeric"


def test_contract_multi_validity_configs(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
            checks:
              - type: invalid
                valid_values: ['ID1', 'N/A']
                valid_length: 3
                valid_regex: '^ID.$'
          - name: size
          - name: distance
          - name: created
    """)
    assert "Actual invalid_count(id) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "invalid_count(id)"
    assert measurement.value == 1
    assert measurement.type == "numeric"


def test_contract_column_invalid_reference_check(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contract_refs_test_table)
    customers_table_name: str = test_connection.ensure_test_table(contracts_test_table)

    contract_result: ContractResult = test_connection.assert_contract_fail(f"""
        dataset: {table_name}
        columns:
          - name: id
          - name: contract_id
            checks:
              - type: invalid
                valid_values_column:
                    dataset: {customers_table_name}
                    column: id
                samples_limit: 20
    """)
    assert "Actual invalid_count(contract_id) was 1" in str(contract_result)
    check_result = contract_result.check_results[1]
    assert check_result.outcome == CheckOutcome.FAIL
    measurement = check_result.measurements[0]
    assert measurement.name == "invalid_count(contract_id)"
    assert measurement.value == 1
    assert measurement.type == "numeric"
