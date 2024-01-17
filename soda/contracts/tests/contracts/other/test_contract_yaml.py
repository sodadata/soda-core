from contracts.helpers.contract_test_tables import contracts_test_table
from contracts.helpers.test_connection import TestConnection


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
