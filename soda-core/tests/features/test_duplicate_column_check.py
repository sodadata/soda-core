from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("duplicate_column")
    .column_text("id")
    .column_integer("age")
    .column_text("country")
    .rows(
        rows=[
            ("1",  1,    "USA"),
            ("2",  2,    "USA"),
            ("3",  2,    "BE"),
            ("4",  3,    "USA"),
            ("5",  3,    "BE"),
            ("6",  3,    "BE"),
            ("7",  3,    "BE"),
            ("8",  4,    "USA"),
            ("9",  4,    "BE"),
            (None, None, "BE"),
        ]
    )
    .build()
)


def test_duplicate_str_pass(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
                checks:
                  - duplicate:
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "distinct_count") == 9
    assert get_diagnostic_value(check_result, "duplicate_count") == 0
    assert get_diagnostic_value(check_result, "valid_count") == 9
    assert get_diagnostic_value(check_result, "duplicate_percent") == 0


def test_duplicate_int_fail(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: age
                checks:
                  - duplicate:
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert 5 == get_diagnostic_value(
        check_result=check_result, diagnostic_name="duplicate_count"
    )


def test_duplicate_with_check_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - duplicate:
                      filter: |
                        {data_source_test_helper.quote_column("country")} = 'USA'
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "duplicate_count") == 0


def test_duplicate_with_missing_and_validity(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [2]
                valid_values: [1, 2, 3]
                checks:
                  - duplicate:
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "duplicate_count") == 3


def test_duplicate_with_missing_and_validity_and_check_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                missing_values: [2]
                valid_values: [1, 2, 3]
                checks:
                  - duplicate:
                      filter: |
                        {data_source_test_helper.quote_column("country")} = 'BE'
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "duplicate_count") == 2
