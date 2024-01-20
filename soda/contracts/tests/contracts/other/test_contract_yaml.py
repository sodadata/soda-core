from contracts.helpers.contract_test_tables import contracts_test_table
from contracts.helpers.test_connection import TestConnection


def test_contract_without_dataset(test_connection: TestConnection):
    contract_result = test_connection.assert_contract_error(f"""
        columns:
          - name: id
          - name: size
          - name: distance
          - name: created
    """)
    assert "'dataset' is a required property" in str(contract_result)


def test_contract_without_columns(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)
    contract_result = test_connection.assert_contract_error(f"""
        dataset: {table_name}
    """)
    assert "'columns' is a required property" in str(contract_result)


def test_contract_invalid_column_type_dict(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)
    contract_result = test_connection.assert_contract_error(f"""
        dataset: {table_name}
        columns:
          - plainstringascheck
    """)
    assert "'plainstringascheck' is not of type 'object'" in str(contract_result)


def test_contract_invalid_column_no_name(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)
    contract_result = test_connection.assert_contract_error(f"""
        dataset: {table_name}
        columns:
          - noname: xyz
    """)
    assert "'name' is required" in str(contract_result)


def test_contract_row_count_ignore_other_keys(test_connection: TestConnection):
    table_name: str = test_connection.ensure_test_table(contracts_test_table)

    test_connection.assert_contract_pass(f"""
        another_top_level_key: check
        dataset: {table_name}
        columns:
          - name: id
            another_column_key: check
          - name: size
            checks:
              - type: missing
                another_column_check_key: check
          - name: distance
          - name: created
        checks:
          - type: row_count
            fail_when_not_between: [0, 10]
            another_dataset_check_key: check
    """)
