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
    .table_purpose("aggregate")
    .column_varchar("country", 3)
    .column_integer("age")
    .column_integer("age_wm")
    .column_integer("age_wi")
    .rows(
        rows=[
            ("USA", 5, 0, 0),
            ("BE", None, 5, 999),
            ("BE", 0, -1, 10),
            ("BE", None, 10, 5),
            ("BE", 10, None, None),
        ]
    )
    .build()
)


def test_aggregate_function_avg(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - aggregate:
                      function: avg
                      threshold:
                        must_be: 5
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg") == 5

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    assert check_json["diagnostics"]["v4"] == {"type": "aggregate", "datasetRowsTested": 5, "checkRowsTested": 5}


def test_aggregate_function_min_length(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                checks:
                  - aggregate:
                      function: min_length
                      threshold:
                        must_be: 2
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "min_length") == 2


def test_aggregate_function_max_length(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                checks:
                  - aggregate:
                      function: max_length
                      threshold:
                        must_be: 3
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "max_length") == 3


def test_aggregate_function_avg_length(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                checks:
                  - aggregate:
                      function: avg_length
                      threshold:
                        must_be: 2.2
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg_length") == 2.2


def test_aggregate_function_avg_with_missing(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age_wm
                missing_values: [-1]
                checks:
                  - aggregate:
                      function: avg
                      threshold:
                        must_be: 5
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg") == 5


def test_aggregate_function_avg_with_invalid(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age_wi
                missing_values: [999]
                checks:
                  - aggregate:
                      function: avg
                      threshold:
                        must_be: 5
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg") == 5


def test_aggregate_function_sum_with_filter(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    quoted_country_column_name: str = data_source_test_helper.quote_column("country")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - aggregate:
                      filter: |
                        {quoted_country_column_name} = 'BE'
                      function: sum
                      threshold:
                        must_be: 10
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "sum") == 10


def test_aggregate_function_unsupported(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - aggregate:
                      function: xyz
                      threshold:
                        must_be: 0
        """,
    )
    assert "Aggregate function 'xyz' is not supported on" in contract_verification_result.get_errors_str()


def test_aggregate_function_avg_warn(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_warn(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: age
                checks:
                  - aggregate:
                      function: avg
                      threshold:
                        must_be: 6
                        level: warn
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "avg") == 5

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    assert check_json["diagnostics"]["v4"] == {"type": "aggregate", "datasetRowsTested": 5, "checkRowsTested": 5}
