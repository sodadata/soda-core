from freezegun import freeze_time
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
    .column_varchar("metadata")
    .column_varchar("json_col")
    .column_varchar("country")
    .rows(
        rows=[
            (1, 10, "10", '{"created": "2000-01-01"}', '{"unique_id": 1}', '{"country_code": "NL"}'),
            (2, 20, "20", '{"created": "2000-01-02"}', '{"unique_id": 1}', '{"country_code": "BE"}'),
            (3, None, None, None, '{"unique_id": 3}', '{"country_code": "123"}'),
            (None, 2, "20", '{"created": "2000-01-04"}', '{"unique_id": 4}', '{"country_code": "0"}'),
            (-1, 30, "30", '{"created": "2000-01-05"}', '{"unique_id": null}', '{"country_code": "SK"}'),
        ]
    )
    .build()
)

reference_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("reference_for_column_expression")
    .column_varchar("country_code")
    .rows(
        rows=[
            ("NL",),
            ("BE",),
        ]
    )
    .build()
)


def test_column_level_column_expression_metric_checks_fail(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    reference_table = data_source_test_helper.ensure_test_table(reference_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    with freeze_time("2000-01-06"):
        result = data_source_test_helper.assert_contract_fail(
            test_table=test_table,
            contract_yaml_str=f"""
                variables:
                  id_col_expr:
                    default: '"id"::varchar'
                  age_str_col_expr:
                    default: '"age_str"::integer'
                  json_col_expr:
                    default: "json_col::json->>'unique_id'"
                  country_col_expr:
                    default: "country::json->>'country_code'"
                  metadata_col_expr:
                    default: "metadata::json->>'created'"
                columns:
                  - name: id
                    column_expression: '${{var.id_col_expr}}'
                    checks:
                      - missing:
                            missing_format:
                                regex: '^-\\d+$'
                                name: positive integers only
                      - invalid:
                          invalid_format:
                            regex: '^-\\d+$'
                            name: positive integers only
                  - name: age_str
                    column_expression: '${{var.age_str_col_expr}}'
                    checks:
                      - aggregate:
                            function: min
                            threshold:
                                must_be: 20
                  - name: json_col
                    checks:
                      - duplicate:
                          column_expression: '${{var.json_col_expr}}'
                  - name: country
                    valid_reference_data:
                        dataset: {data_source_test_helper.build_dqn(reference_table)}
                        column: country_code
                    missing_values: ["0"]
                    invalid_values: ["123"]
                    checks:
                        - invalid:
                            column_expression: '${{var.country_col_expr}}'
                checks:
                    - freshness:
                        column: metadata
                        column_expression: '${{var.metadata_col_expr}}'
                        threshold:
                            must_be_less_than: 1
                            unit: day

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

        # Invalid check
        check_json: dict = soda_core_insert_scan_results_command["checks"][1]
        assert check_json["diagnostics"]["v4"] == {
            "type": "invalid",
            "datasetRowsTested": 5,
            "checkRowsTested": 5,
            "failedRowsCount": 1,
            "failedRowsPercent": 20.0,
            "missingCount": 1,
        }

        # Aggregate check
        check_json: dict = soda_core_insert_scan_results_command["checks"][2]
        assert check_json["diagnostics"]["v4"] == {
            "type": "aggregate",
            "datasetRowsTested": 5,
            "checkRowsTested": 5,
        }

        # Duplicate check
        check_json: dict = soda_core_insert_scan_results_command["checks"][3]
        assert check_json["diagnostics"]["v4"] == {
            "type": "duplicate",
            "failedRowsCount": 1,
            "failedRowsPercent": 25.0,
            "datasetRowsTested": 5,
            "checkRowsTested": 5,
            "missingCount": 1,
        }

        # Invalid reference check
        check_json: dict = soda_core_insert_scan_results_command["checks"][4]
        assert check_json["diagnostics"]["v4"] == {
            "type": "invalid",
            "datasetRowsTested": 5,
            "checkRowsTested": 5,
            "failedRowsCount": 2,
            "failedRowsPercent": 40.0,
            "missingCount": 1,
        }

        # Freshness check
        check_json: dict = soda_core_insert_scan_results_command["checks"][5]
        assert check_json["diagnostics"]["v4"] == {
            "type": "freshness",
            "datasetRowsTested": 5,
            "actualTimestamp": "2000-01-05T00:00:00+00:00",
            "actualTimestampUtc": "2000-01-05T00:00:00+00:00",
            "expectedTimestamp": "2000-01-06T00:00:00+00:00",
            "expectedTimestampUtc": "2000-01-06T00:00:00+00:00",
        }


def test_check_level_column_expression_metric_checks_fail(data_source_test_helper: DataSourceTestHelper):
    # Test only a couple of check types, the override mechanism is universal.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    result = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            variables:
              id_col_expr:
                default: '"id"::varchar'
              age_str_col_expr:
                default: '"age_str"::integer'
            columns:
              - name: id
                column_expression: "CRASH_IF_USED"
                checks:
                  - missing:
                        column_expression: '${{var.id_col_expr}}'
                        missing_format:
                            regex: '^-\\d+$'
                            name: positive integers only
              - name: age_str
                column_expression: "CRASH_IF_USED"
                checks:
                  - aggregate:
                        column_expression: '${{var.age_str_col_expr}}'
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
            variables:
              id_col_expr:
                default: '"id"::varchar'
            columns:
              - name: id
                checks:
                  - missing:
                  - missing:
                        qualifier: expr
                        column_expression: '${{var.id_col_expr}}'

        """,
    )

    for check in result.check_results:
        assert check.outcome == CheckOutcome.FAILED

    # Make sure that unique metric is generated for the both missing checks due to different column expressions.
    # 3 metrics - 1 for each missing, 1 for default row count.
    assert len(result.measurements) == 3
