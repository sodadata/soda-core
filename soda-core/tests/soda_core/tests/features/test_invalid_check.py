from soda_core.contracts.contract_verification import ContractResult, CheckOutcome
from soda_core.tests.helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.tests.helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid")
    .column_text("id")
    .column_integer("age")
    .rows(rows=[
        ("1",  1),
        (None, -1),
        ("3",  None),
        ("X",  2),
    ])
    .build()
)


def test_invalid_count(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_values: ['1', '2', '3']
                checks:
                  - type: invalid_count
                    # Expected check to fail because...
                    # must_be: 0 is the default threshold
        """
    )
    diagnostic_line: str = contract_result.check_results[0].diagnostic_lines[0]
    assert "Actual invalid_count was 1" in diagnostic_line


def test_invalid_count_valid_regex_sql(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_regex_sql: ^[123]$
                checks:
                  - type: invalid_count
                    # Expected check to fail because...
                    # must_be: 0 is the default threshold
        """
    )
    diagnostic_line: str = contract_result.check_results[0].diagnostic_lines[0]
    assert "Actual invalid_count was 1" in diagnostic_line


def test_invalid_count_valid_min_max(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                valid_min: 1
                valid_max: 2
                checks:
                  - type: invalid_count
                    # Expected check to fail because...
                    # must_be: 0 is the default threshold
        """
    )
    diagnostic_line: str = contract_result.check_results[0].diagnostic_lines[0]
    assert "Actual invalid_count was 1" in diagnostic_line


def test_invalid_count_invalid_regex_sql(data_source_test_helper: DataSourceTestHelper):

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_result: ContractResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                invalid_regex_sql: ^[X]$
                checks:
                  - type: invalid_count
                    # Expected check to fail because...
                    # must_be: 0 is the default threshold
        """
    )
    diagnostic_line: str = contract_result.check_results[0].diagnostic_lines[0]
    assert "Actual invalid_count was 1" in diagnostic_line


# def test_missing_count_custom_missing_values(data_source_test_helper: DataSourceTestHelper):
#
#     test_table = data_source_test_helper.ensure_test_table(test_table_specification)
#
#     data_source_test_helper.assert_contract_pass(
#         test_table=test_table,
#         contract_yaml_str=f"""
#             columns:
#               - name: id
#                 missing_values: ['X', 'Y']
#                 checks:
#                   - type: missing_count
#                     must_be: 2
#         """
#     )
#
#
# def test_missing_count_custom_missing_values_int_on_column(data_source_test_helper: DataSourceTestHelper):
#
#     test_table = data_source_test_helper.ensure_test_table(test_table_specification)
#
#     data_source_test_helper.assert_contract_pass(
#         test_table=test_table,
#         contract_yaml_str=f"""
#             columns:
#               - name: age
#                 missing_values: [-1, -2]
#                 checks:
#                   - type: missing_count
#                     must_be: 2
#         """
#     )
#
#
# def test_missing_count_missing_values_on_check(data_source_test_helper: DataSourceTestHelper):
#
#     test_table = data_source_test_helper.ensure_test_table(test_table_specification)
#
#     data_source_test_helper.assert_contract_pass(
#         test_table=test_table,
#         contract_yaml_str=f"""
#             columns:
#               - name: id
#                 checks:
#                   - type: missing_count
#                     missing_values: ['X', 'Y']
#                     must_be: 2
#         """
#     )
#
#
# def test_missing_count_overwrite_missing_values_on_check(data_source_test_helper: DataSourceTestHelper):
#
#     test_table = data_source_test_helper.ensure_test_table(test_table_specification)
#
#     data_source_test_helper.assert_contract_pass(
#         test_table=test_table,
#         contract_yaml_str=f"""
#             columns:
#               - name: id
#                 missing_regex_sql: ^xxx$
#                 checks:
#                   - type: missing_count
#                     missing_values: ['X', 'Y']
#                     must_be: 2
#         """
#     )
#
#
# def test_missing_percent(data_source_test_helper: DataSourceTestHelper):
#
#     test_table = data_source_test_helper.ensure_test_table(test_table_specification)
#
#     data_source_test_helper.assert_contract_pass(
#         test_table=test_table,
#         contract_yaml_str=f"""
#             columns:
#               - name: id
#                 checks:
#                   - type: missing_percent
#                     must_be_between: [24, 26]
#         """
#     )
#
#
# missing_no_rows_specification = (
#     TestTableSpecification.builder()
#     .table_purpose("missing_no_rows")
#     .column_text("id")
#     .build()
# )
#
#
# def test_missing_percent_no_division_by_zero(data_source_test_helper: DataSourceTestHelper):
#
#     test_table = data_source_test_helper.ensure_test_table(missing_no_rows_specification)
#
#     contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
#         test_table=test_table,
#         contract_yaml_str=f"""
#             columns:
#               - name: id
#                 checks:
#                   - type: missing_percent
#         """
#     )
#
#     assert contract_result.check_results[0].outcome == CheckOutcome.NOT_EVALUATED
#
#
# def test_missing_count_no_rows(data_source_test_helper: DataSourceTestHelper):
#
#     test_table = data_source_test_helper.ensure_test_table(missing_no_rows_specification)
#
#     contract_result: ContractResult = data_source_test_helper.assert_contract_pass(
#         test_table=test_table,
#         contract_yaml_str=f"""
#             columns:
#               - name: id
#                 checks:
#                   - type: missing_count
#         """
#     )
#
#     assert contract_result.check_results[0].outcome == CheckOutcome.PASSED
