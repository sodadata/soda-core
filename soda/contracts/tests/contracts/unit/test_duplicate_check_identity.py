from contracts.helpers.contract_data_source_test_helper import ContractDataSourceTestHelper


def test_duplicate_column_check_identity_not_unique_error(data_source_test_helper: ContractDataSourceTestHelper):
    errors_str = data_source_test_helper.get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
              checks:
                - type: missing_count
                  must_be: 5
                - type: missing_count
                  must_be: 7
        """
    )

    assert "Duplicate check identity" in errors_str


def test_duplicate_column_check_identity_unique_by_name(data_source_test_helper: ContractDataSourceTestHelper):
    errors_str = data_source_test_helper.get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
              checks:
                - type: missing_count
                  name: Missing less than 5
                  must_be: 5
                - type: missing_count
                  must_be: 7
        """
    )

    assert "" == errors_str


def test_duplicate_dataset_check_identity_not_unique_error(data_source_test_helper: ContractDataSourceTestHelper):
    errors_str = data_source_test_helper.get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
          checks:
            - type: rows_exist
            - type: rows_exist
        """
    )

    assert "Duplicate check identity" in errors_str


def test_duplicate_dataset_check_identity_unique_by_name(data_source_test_helper: ContractDataSourceTestHelper):
    errors_str = data_source_test_helper.get_parse_errors_str(
        """
          dataset: TABLE_NAME
          columns:
            - name: one
          checks:
            - type: rows_exist
              name: Rows must exist
            - type: rows_exist
              name: Table not empty
        """
    )

    assert "" == errors_str
