from soda_core.contracts.contract_verification import ContractVerificationResult
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_functions import dedent_and_strip


def test_parsing_error_wrong_type(data_source_test_helper: DataSourceTestHelper):
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_error(
        dedent_and_strip(
            """
            dataset:
              data_source: milan_nyc
              namespace_prefix: [soda_test, dev_tom]
              name: SODATEST_soda_cloud_11c53cd6

            columns:
              - name: id
                valid_values: ['1', '2', '3']
                checks:
                  - invalid:
              - name: age
            checks:
              - schema:
    """
        )
    )

    logs_str = contract_verification_result.get_logs_str()
    assert "YAML key 'dataset' expected a str, but was YAML object" in logs_str
