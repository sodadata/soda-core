from soda_core.contracts.contract_verification import ContractResult, CheckResult, CheckOutcome
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

    check_result: CheckResult = contract_result.check_results[0]
    assert CheckOutcome.PASSED == check_result.outcome


def test_row_count_2(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - type: row_count
                must_be_greater_than: 2
        """
    )

    check_result: CheckResult = contract_result.check_results[0]
    assert CheckOutcome.PASSED == check_result.outcome
