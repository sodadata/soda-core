from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("row_count")
    .column_text("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)


def test_row_count(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - row_count:
        """,
    )


def test_row_count_thresholds_pass(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - row_count:
                  threshold:
                    must_be: 3
              - row_count:
                  qualifier: 2
                  threshold:
                    must_not_be: 2
              - row_count:
                  qualifier: 3
                  threshold:
                    must_be_greater_than: 2
              - row_count:
                  qualifier: 4
                  threshold:
                    must_be_greater_than_or_equal: 3
              - row_count:
                  qualifier: 5
                  threshold:
                    must_be_less_than: 4
              - row_count:
                  qualifier: 6
                  threshold:
                    must_be_less_than_or_equal: 3
              - row_count:
                  qualifier: 7
                  threshold:
                    must_be_between: [2, 3]
              - row_count:
                  qualifier: 8
                  threshold:
                    must_be_between: [3, 4]
              - row_count:
                  qualifier: 9
                  threshold:
                    must_be_greater_than: 2
                    must_be_less_than_or_equal: 3
              - row_count:
                  qualifier: 10
                  threshold:
                  must_be_greater_than_or_equal: 3
                  must_be_less_than: 4
        """,
    )


def test_row_count_thresholds_fail(data_source_test_helper: DataSourceTestHelper):
    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/f089d7ef-559a-47ea-aa14-a648823c1f9e/checks

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - row_count:
                  threshold:
                    must_be: 4
              - row_count:
                  qualifier: 2
                  threshold:
                    must_not_be: 3
              - row_count:
                  qualifier: 3
                  threshold:
                    must_be_greater_than: 3
              - row_count:
                  qualifier: 4
                  threshold:
                    must_be_greater_than_or_equal: 4
              - row_count:
                  qualifier: 5
                  threshold:
                    must_be_less_than: 3
              - row_count:
                  qualifier: 6
                  threshold:
                    must_be_less_than_or_equal: 2
              - row_count:
                  qualifier: 7
                  threshold:
                    must_be_between: [-100, 2]
              - row_count:
                  qualifier: 8
                  threshold:
                    must_be_between: [4, 100]
              - row_count:
                  qualifier: 9
                  threshold:
                    must_be_greater_than_or_equal: -100
                    must_be_less_than: 3
              - row_count:
                  qualifier: 10
                  threshold:
                    must_be_greater_than: 3
                    must_be_less_than: 100
              - row_count:
                  qualifier: 11
                  threshold:
                    must_be_greater_than: 4
                    must_be_less_than: 3
              - row_count:
                  qualifier: 12
                  threshold:
                    must_be_greater_than: 3
                    must_be_less_than: 4
        """,
    )
    for i in range(0, len(contract_verification_result.check_results)):
        assert contract_verification_result.check_results[i].outcome == CheckOutcome.FAILED
