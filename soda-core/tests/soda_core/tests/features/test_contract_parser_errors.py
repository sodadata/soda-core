from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper


def test_error_duplicate_column_names(data_source_test_helper: DataSourceTestHelper):
    errors_msg: str = str(data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            dataset: sometable
            columns:
              - name: id
              - name: id
        """
    ))

    assert "Duplicate columns with name 'id': [line=2,column=4], [line=3,column=4]" in errors_msg


def test_error_no_dataset(data_source_test_helper: DataSourceTestHelper):
    errors_msg: str = str(data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            columns:
              - name: id
        """
    ))

    assert "'dataset' is required" in errors_msg
