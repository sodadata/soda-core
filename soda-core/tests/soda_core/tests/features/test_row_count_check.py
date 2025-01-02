from soda_core.contracts.contract_verification import ContractResult, CheckResult, CheckOutcome, \
    ContractVerificationResult
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("row_count")
    .column_text("id")
    .rows(rows=[
        ("1",),
        ("2",),
        ("3",),
    ])
    .build()
)


def test_row_count(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - type: row_count
        """
    )


def test_row_count_thresholds_pass(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - type: row_count
                must_be: 3
              - type: row_count
                must_not_be: 2
              - type: row_count
                must_be_greater_than: 2
              - type: row_count
                must_be_greater_than_or_equal: 3
              - type: row_count
                must_be_less_than: 4
              - type: row_count
                must_be_less_than_or_equal: 3
              - type: row_count
                must_be_between: [2, 3]
              - type: row_count
                must_be_between: [3, 4]
              - type: row_count
                must_be_greater_than: 2
                must_be_less_than_or_equal: 3
              - type: row_count
                must_be_greater_than_or_equal: 3
                must_be_less_than: 4
        """
    )


def test_row_count_thresholds_fail(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - type: row_count
                must_be: 4
              - type: row_count
                must_not_be: 3
              - type: row_count
                must_be_greater_than: 3
              - type: row_count
                must_be_greater_than_or_equal: 4
              - type: row_count
                must_be_less_than: 3
              - type: row_count
                must_be_less_than_or_equal: 2
              - type: row_count
                must_be_between: [-100, 2]
              - type: row_count
                must_be_between: [4, 100]
              - type: row_count
                must_be_greater_than_or_equal: -100
                must_be_less_than: 3
              - type: row_count
                must_be_greater_than: 3
                must_be_less_than: 100
              - type: row_count
                must_be_greater_than: 4
                must_be_less_than: 3
              - type: row_count
                must_be_greater_than: 3
                must_be_less_than: 4
        """
    )
    for i in range(0, len(contract_result.check_results)):
        assert contract_result.check_results[i].outcome == CheckOutcome.FAILED
