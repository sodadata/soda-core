from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import CheckOutcome

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("column_expression")
    .column_integer("id")
    .column_integer("age")
    .column_integer("age_str")
    .rows(
        rows=[
            (1, 10, "10"),
            (2, 20, "20"),
            (3, None, None),
            (None, 2, "20"),
            (-1, 30, "30"),
        ]
    )
    .build()
)


def test_column_level_column_expression_metric_checks_fail(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    result = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                column_expression: '"id"::varchar'
                checks:
                  - missing:
                        missing_format:
                            regex: '^-\d+$'
                            name: positive integers only
              - name: age_str
                column_expression: '"age_str"::integer'
                checks:
                  - aggregate:
                        function: min
                        threshold:
                            must_be: 20
        """,
    )

    for check in result.check_results:
        assert check.outcome == CheckOutcome.FAILED

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json

    # Missing check
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    assert check_json["diagnostics"]["v4"] == {
        "type": "missing",
        "failedRowsCount": 2,
        "failedRowsPercent": 40.0,
        "datasetRowsTested": 5,
        "checkRowsTested": 5,
    }

    # Aggregate check
    check_json: dict = soda_core_insert_scan_results_command["checks"][1]
    assert check_json["diagnostics"]["v4"] == {
        "type": "aggregate",
        "datasetRowsTested": 5,
        "checkRowsTested": 5,
    }


def test_check_level_column_expression_metric_checks_fail(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    result = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                column_expression: "CRASH_IF_USED"
                checks:
                  - missing:
                        column_expression: '"id"::varchar'
                        missing_format:
                            regex: '^-\d+$'
                            name: positive integers only
              - name: age_str
                column_expression: "CRASH_IF_USED"
                checks:
                  - aggregate:
                        column_expression: '"age_str"::integer'
                        function: min
                        threshold:
                            must_be: 20
        """,
    )

    for check in result.check_results:
        assert check.outcome == CheckOutcome.FAILED

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json

    # Missing check
    check_json: dict = soda_core_insert_scan_results_command["checks"][0]
    assert check_json["diagnostics"]["v4"] == {
        "type": "missing",
        "failedRowsCount": 2,
        "failedRowsPercent": 40.0,
        "datasetRowsTested": 5,
        "checkRowsTested": 5,
    }

    # Aggregate check
    check_json: dict = soda_core_insert_scan_results_command["checks"][1]
    assert check_json["diagnostics"]["v4"] == {
        "type": "aggregate",
        "datasetRowsTested": 5,
        "checkRowsTested": 5,
    }


def test_column_expression_clashing_metric(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    result = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            columns:
              - name: id
                checks:
                  - missing:
                  - missing:
                        qualifier: expr
                        column_expression: '"id"::varchar'

        """,
    )

    for check in result.check_results:
        assert check.outcome == CheckOutcome.FAILED

    # Make sure that unique metric is generated for the both missing checks due to different column expressions.
    # 3 metrics - 1 for each missing, 1 for default row count.
    assert len(result.measurements) == 3
