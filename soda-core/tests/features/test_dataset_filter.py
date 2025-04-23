from datetime import datetime

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.datetime_conversions import convert_datetime_to_str
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

t1 = datetime(year=2025, month=4, day=16, hour=12, minute=0, second=0)
t2 = datetime(year=2025, month=4, day=17, hour=12, minute=0, second=0)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("filter")
    .column_text("country")
    .column_integer("size")
    .column_timestamp("updated")
    .rows(
        rows=[
            # records with t1
            ("USA", 10, t1),
            ("BE", 1, t1),
            # records with t2 (must be more than a day apart)
            ("USA", 10, t2),
            ("BE", 1, t2),
            ("GR", 1, t2),
            ("NL", None, t2),
        ]
    )
    .build()
)


referenced_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("filter_invalid_referenced")
    .column_text("country_code")
    .rows(
        rows=[
            ("USA",),
            ("BE",),
            ("NL",),
        ]
    )
    .build()
)


def test_dataset_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    referenced_test_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    now_literal: str = data_source_test_helper.sql_expr_timestamp_literal("${var.NOW}")
    start_ts_value: str = data_source_test_helper.sql_expr_timestamp_truncate_day(now_literal)
    end_ts_value: str = data_source_test_helper.sql_expr_timestamp_add_day("${var.START_TS}")
    column_name_quoted: str = data_source_test_helper.data_source_impl.quote_identifier("updated")

    contract_yaml_str: str = f"""
        variables:
          START_TS:
            default: {start_ts_value}
          END_TS:
            default: {end_ts_value}

        filter: |
            ${{var.START_TS}} < {column_name_quoted}
            AND {column_name_quoted} <= ${{var.END_TS}}

        columns:
          - name: country
            valid_reference_data:
              dataset: {referenced_test_table.unique_name}
              column: country_code
            checks:
              - invalid:

          - name: size
            checks:
              - missing:
    """

    # On the first time partition t1 (16th) the filter should pass
    contract_verification_result_t1: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table, contract_yaml_str=contract_yaml_str, variables={"NOW": convert_datetime_to_str(t1)}
    )
    check_result: CheckResult = contract_verification_result_t1.check_results[0]
    assert next(d.value for d in check_result.diagnostics if d.name == "invalid_count") == 0

    check_result = contract_verification_result_t1.check_results[1]
    assert next(d.value for d in check_result.diagnostics if d.name == "row_count") == 2
    assert next(d.value for d in check_result.diagnostics if d.name == "missing_count") == 0

    # On the second time partition t2 (17th) the filter should fail
    contract_verification_result_t2: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table, contract_yaml_str=contract_yaml_str, variables={"NOW": convert_datetime_to_str(t2)}
    )
    invalid_check_result: CheckResult = contract_verification_result_t2.check_results[0]
    assert next(d.value for d in invalid_check_result.diagnostics if d.name == "invalid_count") == 1

    row_count_check_result: CheckResult = contract_verification_result_t2.check_results[1]
    assert next(d.value for d in row_count_check_result.diagnostics if d.name == "row_count") == 4
    assert next(d.value for d in row_count_check_result.diagnostics if d.name == "missing_count") == 1


def test_dataset_filter_in_user_defined_variable(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    now_literal: str = data_source_test_helper.sql_expr_timestamp_literal("${var.NOW}")
    start_ts_value: str = data_source_test_helper.sql_expr_timestamp_truncate_day(now_literal)
    end_ts_value: str = data_source_test_helper.sql_expr_timestamp_add_day("${var.START_TS}")
    column_name_quoted: str = data_source_test_helper.data_source_impl.quote_identifier("updated")

    contract_yaml_str: str = f"""
        variables:
          START_TS:
            default: {start_ts_value}
          END_TS:
            default: {end_ts_value}
          USER_DEFINED_FILTER_VARIABLE:
            default: |
              ${{var.START_TS}} < {column_name_quoted}
              AND {column_name_quoted} <= ${{var.END_TS}}

        filter: ${{var.USER_DEFINED_FILTER_VARIABLE}}

        checks:
          - row_count:
              name: The filter expression is ${{var.USER_DEFINED_FILTER_VARIABLE}}
    """

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table, contract_yaml_str=contract_yaml_str, variables={"NOW": convert_datetime_to_str(t2)}
    )
    schema_check_result: CheckResult = contract_verification_result.check_results[0]
    assert "The filter expression is" in schema_check_result.check.name
    assert "${var.USER_DEFINED_FILTER_VARIABLE}" not in schema_check_result.check.name
    assert "updated" in schema_check_result.check.name
    assert "AND" in schema_check_result.check.name
