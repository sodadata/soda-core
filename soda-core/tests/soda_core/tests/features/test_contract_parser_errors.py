from soda_core.contracts.contract_verification import ContractVerificationResult
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper


def test_error_duplicate_column_names(data_source_test_helper: DataSourceTestHelper):
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            dataset: sometable
            columns:
              - name: id
              - name: id
        """
    )

    assert (
        "Duplicate columns with name 'id':  In yaml_string.yml at: [2,4], [3,4]"
        in contract_verification_result.get_logs_str()
    )


def test_error_no_dataset(data_source_test_helper: DataSourceTestHelper):
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            columns:
              - name: id
        """
    )

    assert (
        "'dataset' is required"
        in contract_verification_result.get_logs_str()
    )
