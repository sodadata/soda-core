import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid")
    .column_text("id")
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

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_format:
                  regex: ^[123]$
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

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                invalid_format:
                  regex: ^[X]$
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

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                valid_format:
                  regex: ^[0-9]$
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
