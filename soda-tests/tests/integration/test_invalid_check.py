import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid")
    .column_varchar("id")
    .column_integer("age")
    .rows(
        rows=[
            ("1", 1),
            (None, -1),
            ("3", None),
            ("X", 2),
        ]
    )
    .build()
)

# Dedicated table for the escaped-metacharacter regex test: '1.5' must match
# `^1\.5$` and '1x5' must not. Both rows are needed -- with only '1.5' the
# pattern matches either way and the test proves nothing.
escaped_metacharacter_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_regex_escape")
    .column_varchar("value")
    .rows(
        rows=[
            ("1.5",),
            ("1x5",),
        ]
    )
    .build()
)


@pytest.mark.parametrize(
    "contract_yaml_str",
    [
        """
        columns:
          - name: id
            valid_values: ['1', '2', '3']
            checks:
              - invalid:
        """,
        """
        columns:
          - name: id
            checks:
              - invalid:
                  valid_values: ['1', '2', '3']
        """,
    ],
)
def test_valid_count(data_source_test_helper: DataSourceTestHelper, contract_yaml_str: str):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table, contract_yaml_str=contract_yaml_str
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    assert check_json["diagnostics"]["v4"] == {
        "type": "invalid",
        "failedRowsCount": 1,
        "failedRowsPercent": 25.0,
        "datasetRowsTested": 4,
        "checkRowsTested": 4,
        "missingCount": 1,
    }


def test_valid_values_with_null(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_values: ['1', '2', '3', null]
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_valid_values_with_null_warn(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_warn(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_values: ['1', '2', '3', null]
                checks:
                  - invalid:
                      threshold:
                        level: warn
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_invalid_values(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                invalid_values: ['X']
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_invalid_count_valid_regex_sql(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    if data_source_test_helper.data_source_impl.sql_dialect.supports_regex_advanced():
        regex_pattern = "^[123]$"
    else:
        regex_pattern = "[123]"

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_format:
                  regex: '{regex_pattern}'
                  name: one-two-threes
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_invalid_count_valid_regex_escaped_metacharacter(data_source_test_helper: DataSourceTestHelper):
    """A backslash in a regex must survive into the data source's regex engine.

    Data sources whose SQL parser consumes backslashes inside string literals
    (Snowflake, Databricks, Redshift) drop the backslash before the engine ever
    sees it, turning `1\\.5` into `1.5` -- where `.` matches any character, so
    '1x5' is silently accepted as valid. The other regex tests here all use
    backslash-free patterns (`^[123]$`), which is why that went unnoticed.
    See SCS-1413 / SCS-1230.

    Escaping a metacharacter is used rather than a shorthand like `\\d` because
    it is valid in POSIX ERE as well as PCRE -- Redshift's REGEXP_LIKE is POSIX,
    where `\\d` is not a digit class on any code path.
    """
    if not data_source_test_helper.data_source_impl.sql_dialect.supports_regex_advanced():
        pytest.skip("data source does not evaluate the pattern as a regex")

    test_table = data_source_test_helper.ensure_test_table(escaped_metacharacter_test_table_specification)

    # Only '1x5' is invalid. If the backslash is eaten, `.` matches 'x' too and
    # the check finds nothing invalid, so assert_contract_fail itself fails.
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: value
                valid_format:
                  regex: '^1\\.5$'
                  name: one-point-five
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_invalid_count_valid_min_max(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                valid_min: 1
                valid_max: 2
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_invalid_count_invalid_regex_sql(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    if data_source_test_helper.data_source_impl.sql_dialect.supports_regex_advanced():
        regex_pattern = "^[X]$"
    else:
        regex_pattern = "[X]"

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                invalid_format:
                  regex: '{regex_pattern}'
                  name: all X-es
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_invalid_count_valid_format(data_source_test_helper: DataSourceTestHelper):
    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/701311a4-6fc3-4f41-86a1-2a7fe4dc358f/checks

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    # This test depends on data source configuration 'format_regexes'
    # For example, see soda-postgres/tests/soda_postgres/contracts/impl/data_sources/postgres_data_source_test_helper.py
    #             "format_regexes": {
    #                 "single_digit_test_format": "^[0-9]$"
    #             }

    if data_source_test_helper.data_source_impl.sql_dialect.supports_regex_advanced():
        regex_pattern = "^[0-9]$"
    else:
        regex_pattern = "[0-9]"

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_format:
                  regex: '{regex_pattern}'
                  name: single_digit_test_format
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 1
    )


def test_valid_values_with_check_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_values: ['1', '2', '3']
                checks:
                  - invalid:
                      filter: '{data_source_test_helper.quote_column("age")} < 2'
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 0
    )


def test_invalid_check_with_variables_typing(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            variables:
              my_int_variable:
                default: 100
              my_str_variable:
                default: abcd
            columns:
              - name: id
                checks:
                  - invalid:
                      valid_max_length: ${var.my_int_variable}
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 0
    )
