from numbers import Number

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult, CheckResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("combined_filters")
    .column_text("country")
    .column_integer("size")
    .column_text("cat")
    .rows(
        rows=[
            ("USA", 10,   "L"),
            ("USA", 5,    "M"),
            ("USA", 1,    "-"),
            ("USA", None, "S"),
            ("BE", 6,     "S"),
            ("BE", 7,     None),
            ("GR", 1,     "S"),
            ("NL", None,  "S"),
        ]
    )
    .build()
)


def test_dataset_filter_combined_with_check_filters(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    country_quoted = data_source_test_helper.quote_column("country")
    size_quoted = data_source_test_helper.quote_column("size")

    contract_yaml_str: str = f"""
        checks_filter: |
          {size_quoted} >= 5

        columns:
          - name: cat
            checks:
              - missing:
                  filter: |
                    {country_quoted} IN ('USA')
              - invalid:
                  valid_values: ['S', 'M', 'L']
                  filter: |
                    {country_quoted} IN ('BE')

        checks:
          - row_count:
              filter: |
                {country_quoted} IN ('USA', 'BE')
    """

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table, contract_yaml_str=contract_yaml_str
    )
    assert contract_verification_result.check_results[0].get_numeric_diagnostic_value("missing_count") == 1
    assert contract_verification_result.check_results[1].get_numeric_diagnostic_value("invalid_count") == 1
    assert contract_verification_result.check_results[2].get_numeric_diagnostic_value("row_count") == 4
