from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("combined_filters")
    .column_varchar("country", 3)
    .column_integer("size")
    .column_varchar("cat")
    .rows(
        rows=[
            ("USA", 10, "L"),
            ("USA", 5, "X"),
            ("USA", 8, "-"),
            ("USA", None, "S"),
            ("BE", 6, "S"),
            ("BE", 7, None),
            ("GR", 1, "S"),
            ("NL", None, "S"),
        ]
    )
    .build()
)


def test_dataset_filter_combined_with_check_filters(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    country_quoted = data_source_test_helper.quote_column("country")
    size_quoted = data_source_test_helper.quote_column("size")

    contract_yaml_str: str = f"""
        filter: |
          {size_quoted} >= 5

        columns:
          - name: cat
            checks:
              - missing:
                  filter: |
                    {country_quoted} = 'BE'
              - invalid:
                  invalid_values: ['X', '-']
                  valid_max_length: 2
                  filter: |
                    {country_quoted} = 'USA'

        checks:
          - row_count:
              filter: |
                {country_quoted} IN ('USA', 'BE')
    """

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table, contract_yaml_str=contract_yaml_str
    )

    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "missing_count") == 1

    check_result = contract_verification_result.check_results[1]
    assert get_diagnostic_value(check_result, "invalid_count") == 2

    check_result = contract_verification_result.check_results[2]
    assert get_diagnostic_value(check_result, "check_rows_tested") == 5
