from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("duplicate_dataset")
    .column_varchar("rep", 4)
    .column_varchar("country", 3)
    .column_varchar("zip", 4)
    .rows(
        rows=[
            ("Joe", "USA", "1000"),
            ("Joe", "USA", "1234"),
            ("Joe", "USA", "2000"),
            ("Joe", "USA", "2000"),
            ("Joe", "USA", "2000"),
            ("Joe", "JAP", "1000"),
            ("Joe", "JAP", "1234"),
            ("Jack", "USA", "1000"),
            ("Jack", "USA", "1234"),
            ("Jack", "USA", "2000"),
            ("Jack", "JAP", "1000"),
            ("Jack", "JAP", "1234"),
            (None, None, "9999"),
        ]
    )
    .build()
)


def test_dataset_duplicate(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - duplicate:
                  columns: ['rep', 'country', 'zip']
            """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    multicolumn_duplicate_diagnostics: dict = check_json["diagnostics"]["v4"]
    assert 15 < multicolumn_duplicate_diagnostics["failedRowsPercent"] < 16
    del multicolumn_duplicate_diagnostics["failedRowsPercent"]

    assert check_json["diagnostics"]["v4"] == {
        "type": "duplicate",
        "failedRowsCount": 2,
        # "failedRowsPercent": 15.384615384615385, # float value tested and removed above
        "datasetRowsTested": 13,
        "checkRowsTested": 13,
    }


def test_dataset_duplicate_percent(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - duplicate:
                  columns: ['rep', 'country', 'zip']
                  threshold:
                    metric: percent
                    must_be_greater_than: 10
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "duplicate_count") == 2
    assert 15 < get_diagnostic_value(check_result, "duplicate_percent") < 16
    assert get_diagnostic_value(check_result, "dataset_rows_tested") == 13
    assert get_diagnostic_value(check_result, "check_rows_tested") == 13


def test_dataset_duplicate_with_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - duplicate:
                  columns: ['rep', 'country', 'zip']
                  filter: |
                    {data_source_test_helper.quote_column("rep")} = 'Jack'
            """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "duplicate_count") == 0
    assert get_diagnostic_value(check_result, "duplicate_percent") == 0
    assert get_diagnostic_value(check_result, "check_rows_tested") == 5
    assert get_diagnostic_value(check_result, "dataset_rows_tested") == 13


def test_dataset_duplicate_warn(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_warn(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - duplicate:
                  columns: ['rep']
                  threshold:
                    level: warn
            """,
    )
