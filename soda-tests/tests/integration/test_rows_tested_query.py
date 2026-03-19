from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    ContractVerificationResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("rows_tested_query")
    .column_integer("id")
    .column_integer("amount")
    .rows(
        rows=[
            (1, 10),
            (2, 20),
            (3, -5),
            (4, 30),
            (5, -15),
        ]
    )
    .build()
)

# All-zero table for testing division-by-zero edge case
test_table_empty_specification = (
    TestTableSpecification.builder()
    .table_purpose("rows_tested_query_empty")
    .column_integer("id")
    .column_integer("amount")
    .rows(rows=[])
    .build()
)


def test_failed_rows_query_with_rows_tested_query(data_source_test_helper: DataSourceTestHelper):
    """Verify that rows_tested_query executes and checkRowsTested flows into v4 diagnostics JSON."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT * FROM {test_table.qualified_name} WHERE amount < 0
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    # RED: Currently the v4 diagnostics for failed_rows query mode does NOT include checkRowsTested.
    # The rows_tested_query feature should add it.
    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "datasetRowsTested": 5,
        "checkRowsTested": 5,
    }


def test_failed_rows_query_rows_tested_query_percent_threshold(data_source_test_helper: DataSourceTestHelper):
    """Verify percent threshold evaluates correctly with rows_tested_query."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    # 2 out of 5 rows fail = 40%, which is less than 50%, so check should pass
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT * FROM {test_table.qualified_name} WHERE amount < 0
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {test_table.qualified_name}
                  threshold:
                    metric: percent
                    must_be_less_than: 50
        """,
    )

    check_result = contract_verification_result.check_results[0]
    assert check_result.outcome == CheckOutcome.PASSED

    # RED: threshold_value should be the percent (40.0), not the raw count
    assert check_result.threshold_value == 40.0

    # RED: diagnostic_metric_values should include check_rows_tested
    assert get_diagnostic_value(check_result, "check_rows_tested") == 5
    assert get_diagnostic_value(check_result, "failed_rows_count") == 2
    assert get_diagnostic_value(check_result, "failed_rows_percent") == 40.0


def test_failed_rows_query_percent_without_rows_tested_query(data_source_test_helper: DataSourceTestHelper):
    """Verify that metric: percent without rows_tested_query emits a validation error."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    # RED: Using metric: percent in query mode without rows_tested_query should produce
    # a validation error because there is no row count to compute percent against.
    error_msg: str = data_source_test_helper.assert_contract_error(
        contract_yaml_str=f"""
            dataset: {test_table.unique_name}
            data_source: {data_source_test_helper.data_source_impl.data_source_yaml.name}
            checks:
              - failed_rows:
                  query: |
                    SELECT * FROM {test_table.qualified_name} WHERE amount < 0
                  threshold:
                    metric: percent
                    must_be_less_than: 50
        """,
    )

    assert "rows_tested_query" in error_msg.lower() or "percent" in error_msg.lower()


def test_failed_rows_query_without_rows_tested_query_backward_compat(data_source_test_helper: DataSourceTestHelper):
    """Verify backward compatibility: without rows_tested_query, checkRowsTested is null in v4 diagnostics."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT * FROM {test_table.qualified_name} WHERE amount < 0
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    v4_diagnostics = check_json["diagnostics"]["v4"]

    # RED: After the feature is implemented, the v4 diagnostics should include
    # checkRowsTested as null when rows_tested_query is not provided.
    # Currently, checkRowsTested is not present in the dict at all.
    assert "checkRowsTested" in v4_diagnostics
    assert v4_diagnostics["checkRowsTested"] is None


def test_failed_rows_query_rows_tested_query_returns_zero(data_source_test_helper: DataSourceTestHelper):
    """Verify graceful handling when rows_tested_query returns 0 (no division-by-zero)."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    empty_table = data_source_test_helper.ensure_test_table(test_table_empty_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    # rows_tested_query points to an empty table (returns 0), but the main query
    # runs against the populated table. This tests the division-by-zero edge case.
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  query: |
                    SELECT * FROM {test_table.qualified_name} WHERE amount < 0
                  rows_tested_query: |
                    SELECT COUNT(*) FROM {empty_table.qualified_name}
                  threshold:
                    metric: percent
                    must_be_less_than: 50
        """,
    )

    check_result = contract_verification_result.check_results[0]

    # RED: When rows_tested_query returns 0, percent should be 0 (not error/infinity)
    assert get_diagnostic_value(check_result, "check_rows_tested") == 0
    assert get_diagnostic_value(check_result, "failed_rows_percent") == 0


def test_failed_rows_expression_emits_check_rows_tested_in_v4(data_source_test_helper: DataSourceTestHelper):
    """Verify that expression-mode failed_rows includes checkRowsTested in cloud serialization."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    amount_quoted = data_source_test_helper.quote_column("amount")

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  expression: |
                    {amount_quoted} < 0
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    # RED: Currently the v4 diagnostics for failed_rows does NOT include checkRowsTested.
    # After the feature, expression-mode should also emit checkRowsTested in v4.
    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "datasetRowsTested": 5,
        "checkRowsTested": 5,
    }
