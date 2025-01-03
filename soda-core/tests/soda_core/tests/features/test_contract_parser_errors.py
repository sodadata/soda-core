from unittest import skip

from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper


@skip
def test_error_duplicate_column_names(data_source_test_helper: DataSourceTestHelper):
    errors_msg: str = str(data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            dataset: required
            columns:
              - name: id
              - name: id
        """
    ))

    assert "duplicate column definition: 'id'" in errors_msg
