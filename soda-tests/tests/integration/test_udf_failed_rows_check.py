import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("failed_rows")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (0, 4),
            (10, 20),
            (10, 17),
        ]
    )
    .build()
)


def test_failed_rows_expression(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

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
                    ({end_quoted} - {start_quoted}) > 5
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "failedRowsPercent": 2 * 100 / 3,
        "datasetRowsTested": 3,
        "checkRowsTested": 3,
    }


def test_failed_rows_query(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

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
                    SELECT *
                    FROM {test_table.qualified_name}
                    WHERE ({end_quoted} - {start_quoted}) > 5
        """,
    )

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "datasetRowsTested": 3,
    }


@pytest.mark.no_snapshot  # Requires a real DB error (nested CTE rejection) to trigger the fallback path
def test_failed_rows_query_with_cte_falls_back_to_streaming(data_source_test_helper: DataSourceTestHelper):
    """A user query containing a CTE cannot be wrapped in another CTE.
    The count path should fall back to row-by-row streaming and still produce the correct count."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

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
                    WITH filtered AS (
                        SELECT *
                        FROM {test_table.qualified_name}
                        WHERE ({end_quoted} - {start_quoted}) > 5
                    )
                    SELECT * FROM filtered
        """,
    )

    # Confirm the fallback path was triggered
    logs_str = contract_verification_result.get_logs_str()
    assert (
        "falling back to row-by-row streaming" in logs_str
    ), "Expected the CTE-wrapped count to fail and trigger the streaming fallback"

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "datasetRowsTested": 3,
    }
