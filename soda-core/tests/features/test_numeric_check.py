from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult, CheckResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("numeric")
    .column_text("country")
    .column_integer("age")
    .column_integer("age_wm")
    .column_integer("age_wi")
    .rows(
        rows=[
            ("USA", 5,    0,    0),
            ("BE",  None, 5,    999),
            ("BE",  0,    -1,   10),
            ("BE",  None, 10,   5),
            ("BE",  10,   None, None),
        ]
    )
    .build()
)


def test_numeric_function_avg(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - numeric:
                      function: avg
                      threshold:
                        must_be: 5
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg") == 5


def test_numeric_function_avg_with_missing(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age_wm
                missing_values: [-1]
                checks:
                  - numeric:
                      function: avg
                      threshold:
                        must_be: 5
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg") == 5


def test_numeric_function_avg_with_invalid(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age_wi
                missing_values: [999]
                checks:
                  - numeric:
                      function: avg
                      threshold:
                        must_be: 5
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg") == 5


def test_numeric_function_sum_with_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - numeric:
                      filter: |
                        "country" = 'BE'
                      function: sum
                      threshold:
                        must_be: 15
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "sum") == 15


def test_numeric_function_unsupported(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - numeric:
                      function: xyz
                      threshold:
                        must_be: 0
        """,
    )
    assert "Function 'xyz' is not supported on" in contract_verification_result.get_errors_str()
