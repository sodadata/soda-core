from soda_core.contracts.contract_verification import (
    CheckOutcome,
    ContractVerificationResult,
)
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification

missing_no_rows_specification = (
    TestTableSpecification.builder().table_purpose("missing_no_rows").column_text("id").build()
)


def test_missing_percent_no_division_by_zero(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(missing_no_rows_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - missing:
                      metric: percent
        """,
    )

    assert contract_verification_result.check_results[0].outcome == CheckOutcome.PASSED


def test_missing_count_no_rows(data_source_test_helper: DataSourceTestHelper):
    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/dbd4e3da-62f4-4cdc-a347-f777118e5aeb/checks

    test_table = data_source_test_helper.ensure_test_table(missing_no_rows_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - missing:
        """,
    )

    assert contract_verification_result.check_results[0].outcome == CheckOutcome.PASSED
