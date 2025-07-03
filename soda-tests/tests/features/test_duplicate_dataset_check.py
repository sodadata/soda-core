from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("duplicate_dataset")
    .column_text("rep")
    .column_text("country")
    .column_text("zip")
    .rows(
        rows=[
            ("Joe", "USA", "1000"),
            ("Joe", "USA", "1234"),
            ("Joe", "USA", "2000"),
            ("Joe", "USA", "2000"),
            ("Joe", "USA", "2000"),
            ("Joe", "JAP", "1000"),
            ("Joe", "JAP", "1234"),
            ("Jack", "USA", "1000"),
            ("Jack", "USA", "1234"),
            ("Jack", "USA", "2000"),
            ("Jack", "JAP", "1000"),
            ("Jack", "JAP", "1234"),
            (None, None, "9999"),
        ]
    )
    .build()
)


def test_dataset_duplicate(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - duplicate:
                  columns: ['rep', 'country', 'zip']
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "duplicate_count") == 2
    assert 15 < get_diagnostic_value(check_result, "duplicate_percent") < 16
    assert get_diagnostic_value(check_result, "dataset_rows_tested") == 13
    assert get_diagnostic_value(check_result, "check_rows_tested") == 13


def test_dataset_duplicate_with_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - duplicate:
                  columns: ['rep', 'country', 'zip']
                  filter: |
                    {data_source_test_helper.quote_column("rep")} = 'Jack'
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "duplicate_count") == 0
    assert get_diagnostic_value(check_result, "duplicate_percent") == 0
    assert get_diagnostic_value(check_result, "check_rows_tested") == 5
    assert get_diagnostic_value(check_result, "dataset_rows_tested") == 13
