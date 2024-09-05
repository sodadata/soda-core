from contracts.helpers.contract_data_source_test_helper import (
    ContractDataSourceTestHelper,
)
from contracts.helpers.contract_test_tables import contracts_test_table


def test_contract_without_dataset(data_source_test_helper: ContractDataSourceTestHelper):
    contract_result = data_source_test_helper.assert_contract_error(
        """
        columns:
          - name: id
          - name: size
          - name: distance
          - name: created
    """
    )
    assert "'dataset' is a required property" in str(contract_result)


def test_contract_without_columns(data_source_test_helper: ContractDataSourceTestHelper):
    table_name: str = data_source_test_helper.ensure_test_table(contracts_test_table)
    contract_result = data_source_test_helper.assert_contract_error(
        f"""
        dataset: {table_name}
    """
    )
    assert "'columns' is a required property" in str(contract_result)


def test_contract_invalid_column_type_dict(data_source_test_helper: ContractDataSourceTestHelper):
    table_name: str = data_source_test_helper.ensure_test_table(contracts_test_table)
    contract_result = data_source_test_helper.assert_contract_error(
        f"""
        dataset: {table_name}
        columns:
          - plainstringascheck
    """
    )
    assert "'plainstringascheck' is not of type 'object'" in str(contract_result)


def test_contract_invalid_column_no_name(data_source_test_helper: ContractDataSourceTestHelper):
    table_name: str = data_source_test_helper.ensure_test_table(contracts_test_table)
    contract_result = data_source_test_helper.assert_contract_error(
        f"""
        dataset: {table_name}
        columns:
          - noname: xyz
    """
    )
    assert "'name' is required" in str(contract_result)


def test_contract_row_count_ignore_other_keys(data_source_test_helper: ContractDataSourceTestHelper):
    data_source_test_helper.assert_contract_pass(
        test_table=contracts_test_table,
        contract_yaml_str=f"""
            another_top_level_key: check
            columns:
              - name: id
                another_column_key: check
              - name: size
                checks:
                  - type: no_missing_values
                    another_column_check_key: check
              - name: distance
              - name: created
            checks:
              - type: rows_exist
                fail_when_not_between: [0, 10]
                another_dataset_check_key: check
    """,
    )
