from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import CheckOutcome, CheckResult, ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("metric")
    .column_integer("start")
    .column_integer("end")
    .rows(
        rows=[
            (0, 10),
            (10, 20),
            (5, 15),
        ]
    )
    .build()
)

text_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("metric_text")
    .column_varchar("country", 3)
    .rows(
        rows=[
            ("BE",),
            ("USA",),
        ]
    )
    .build()
)


# Ensure this test is skipped on other data sources than
def test_metric_expression(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - metric:
                  expression: |
                    AVG({end_quoted} - {start_quoted})
                  threshold:
                    must_be_between:
                      greater_than_or_equal: 9
                      less_than_or_equal: 11
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert check_result.threshold_value == 10


# Ensure this test is skipped on other data sources than
def test_metric_query(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - metric:
                  query: |
                    SELECT AVG({end_quoted} - {start_quoted})
                    FROM {test_table.qualified_name}
                  threshold:
                    must_be_between:
                      greater_than_or_equal: 9
                      less_than_or_equal: 11
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert check_result.threshold_value == 10


def test_metric_query_returning_numeric_text_is_read_as_a_number(data_source_test_helper: DataSourceTestHelper):
    """A metric query whose number comes back as text, as from a CAST to a string type, still evaluates."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - metric:
                  query: |
                    {data_source_test_helper.select_literal_query("'12'")}
                  threshold:
                    must_be: 12
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert check_result.threshold_value == 12

    check_json: dict = data_source_test_helper.soda_cloud.requests[1].json["checks"][0]
    assert check_json["diagnostics"]["value"] == 12
    assert check_json["outcome"] == "pass"


def test_metric_query_returning_non_numeric_text_does_not_poison_the_upload(
    data_source_test_helper: DataSourceTestHelper,
):
    """A metric query returning a non-number must not take the whole scan down with it.

    diagnostics.value is a double in the Soda Cloud API, so a text value there fails the parse of
    the whole sodaCoreInsertScanResults body. The value must be dropped with an error naming the
    fix, leaving the check not evaluated, while the other checks still reach Soda Cloud.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - metric:
                  query: |
                    {data_source_test_helper.select_literal_query("'not_a_number'")}
                  threshold:
                    must_be: 0
              - row_count:
        """,
    ).contract_verification_results[0]

    metric_check_result, row_count_check_result = contract_verification_result.check_results
    assert metric_check_result.outcome == CheckOutcome.NOT_EVALUATED
    assert metric_check_result.threshold_value is None
    assert row_count_check_result.outcome == CheckOutcome.PASSED

    soda_core_insert_scan_results_command = data_source_test_helper.soda_cloud.requests[1].json
    metric_check_json, row_count_check_json = soda_core_insert_scan_results_command["checks"]
    assert metric_check_json["outcome"] == "unevaluated"
    assert row_count_check_json["outcome"] == "pass"

    error_messages = [
        log["message"] for log in soda_core_insert_scan_results_command["logs"] if log["level"] == "error"
    ]
    assert any("Metric query returned str 'not_a_number', not a number" in m for m in error_messages)


def test_metric_expression_returning_text_is_not_evaluated(data_source_test_helper: DataSourceTestHelper):
    """A metric expression returning text is skipped instead of aborting the whole verification.

    The expression's value went through float(), so text raised out of the aggregation query and
    took down every other check sharing that query.
    """
    test_table = data_source_test_helper.ensure_test_table(text_test_table_specification)

    country_quoted = data_source_test_helper.quote_column("country")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - metric:
                  expression: MAX({country_quoted})
                  threshold:
                    must_be: 0
              - row_count:
        """,
    ).contract_verification_results[0]

    metric_check_result, row_count_check_result = contract_verification_result.check_results
    assert metric_check_result.outcome == CheckOutcome.NOT_EVALUATED
    assert metric_check_result.threshold_value is None
    assert row_count_check_result.outcome == CheckOutcome.PASSED
    assert "Metric expression returned str 'USA', not a number" in contract_verification_result.get_errors_str()
