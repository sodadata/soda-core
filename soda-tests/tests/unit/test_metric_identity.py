from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("metric_identity")
    .column_integer("id")
    .rows(
        rows=[
            (1,),
            (2,),
            (3,),
        ]
    )
    .build()
)


def test_metric_identity_row_count(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
                - name: id
                  checks:
                    - row_count:
                    - row_count:
                        name: "This custom name should not affect metric identity"
                        qualifier: "custom"
            checks:
                - row_count:
                - row_count:
                    name: "This custom name should not affect metric identity"
                    qualifier: "custom"

        """,
    )
    # Only one metric for row counts should be created.
    assert len(contract_verification_result.measurements) == 1


def test_metric_identity_invalid_count(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
                - name: id
                  invalid_values: [10]
                  checks:
                    - invalid:
                    - invalid:
                        qualifier: "custom"
                        name: "This custom name should not affect metric identity"
        """,
    )
    # row_count, invalid_count, and missing_count
    assert len(contract_verification_result.measurements) == 3
