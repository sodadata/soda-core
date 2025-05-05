from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("failed_rows")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (0, 4),
            (10, 20),
            (10, 17),
        ]
    )
    .build()
)


def test_failed_rows_basics(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  metric: rows_with_high_duration
                  query: |
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "rows_with_high_duration") == 2
