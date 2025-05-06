from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("metric_expression")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (0, 10),
            (10, 20),
            (5, 15),
        ]
    )
    .build()
)


# Ensure this test is skipped on other data sources than
def test_metric_expression(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - metric_expression:
                  expression: |
                    AVG({end_quoted} - {start_quoted})
                  threshold:
                    must_be_between: [9, 11]
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "expression_value") == 10
