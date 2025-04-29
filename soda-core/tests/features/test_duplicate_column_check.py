import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_fixtures import data_source_test_helper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    ContractVerificationResult,
    Diagnostic,
    NumericDiagnostic, CheckResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("duplicate_column")
    .column_text("id")
    .column_integer("age")
    .rows(
        rows=[
            ("1", 1),
            ("2", 2),
            ("3", 2),
            ("4", 3),
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
    assert 0 == data_source_test_helper.get_first_numeric_diagnostic_value(
        check_result=check_result,
        diagnostic_name="duplicate_count"
    )


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
    assert 1 == data_source_test_helper.get_first_numeric_diagnostic_value(
        check_result=check_result,
        diagnostic_name="duplicate_count"
    )
