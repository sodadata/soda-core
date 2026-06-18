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

# (end - start) > 5 fails for (0,10), (10,20), (10,17) -> 3 failures unfiltered. With `filter: start = 0`
# only (0,4) [passes] and (0,10) [fails] are in scope, so a correctly-scoped check finds 1 failure of 2
# tested -> 50%. This narrowed-but-non-zero shape is what distinguishes "filter scopes the numerator"
# from "filter only scopes the denominator" (the 200% bug) or "filter zeroes everything".
narrowing_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("failed_rows_narrowing")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (0, 4),
            (0, 10),
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


def test_failed_rows_expression_applies_check_filter(data_source_test_helper: DataSourceTestHelper):
    """A check-level `filter:` on a failed_rows expression must scope BOTH the failing-rows count
    (numerator) and the rows-tested denominator. With `filter: start = 0`, only the row (0, 4) is in
    scope and it does NOT satisfy (end - start) > 5, so the check PASSES with failed_rows_count 0.

    Regression guard: before the fix, FailedRowsExpressionMetricImpl ignored check_filter, so the
    numerator counted the two out-of-scope failing rows (10,20) and (10,17) -> failed_rows_count = 2
    over a filtered denominator of 1 -> a 200% failed_rows_percent and a FAILED outcome."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  expression: |
                    ({end_quoted} - {start_quoted}) > 5
                  filter: |
                    {start_quoted} = 0
        """,
        publish_results=False,
    )

    check_result = contract_verification_result.check_results[0]
    assert check_result.diagnostic_metric_values == {
        "failed_rows_count": 0,
        "failed_rows_percent": 0,
        "dataset_rows_tested": 3,
        "check_rows_tested": 1,
    }


def test_failed_rows_expression_filter_narrows_count(data_source_test_helper: DataSourceTestHelper):
    """The check-level `filter:` must scope the failing-rows COUNT (numerator), not just the
    rows-tested denominator. With `filter: start = 0`, the in-scope rows are (0,4) [passes] and
    (0,10) [fails], so failed_rows_count = 1 of 2 tested -> 50%. Unfiltered the expression fails for 3
    rows, so this asserts the filter narrows to a NON-ZERO count that differs from the unfiltered one,
    and that failed_rows_percent stays <= 100 with a real numerator (the headline regression was a
    200% percent from an unscoped numerator over a scoped denominator)."""
    test_table = data_source_test_helper.ensure_test_table(narrowing_test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  expression: |
                    ({end_quoted} - {start_quoted}) > 5
                  filter: |
                    {start_quoted} = 0
        """,
        publish_results=False,
    )

    check_result = contract_verification_result.check_results[0]
    assert check_result.diagnostic_metric_values == {
        "failed_rows_count": 1,
        "failed_rows_percent": 50,
        "dataset_rows_tested": 4,
        "check_rows_tested": 2,
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


@pytest.mark.no_snapshot  # The fallback path (when triggered) requires a real DB error, not a snapshot replay
def test_failed_rows_query_with_cte_produces_correct_count(data_source_test_helper: DataSourceTestHelper):
    """A user query containing a CTE should produce the correct count regardless of whether the
    database supports nested CTEs (CTE path) or not (streaming fallback)."""
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

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]

    assert check_json["diagnostics"]["v4"] == {
        "type": "failed_rows",
        "failedRowsCount": 2,
        "datasetRowsTested": 3,
    }
