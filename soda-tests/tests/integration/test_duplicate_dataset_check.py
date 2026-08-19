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


def test_dataset_duplicate_not_evaluated_without_row_hashing(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, caplog
):
    """A source whose query language cannot express a row hash must un-evaluate, not report a number.

    The multi-column duplicate count is derived as `row_count - distinct_count`, and the distinct
    count comes from a hash. A source that cannot compute the hash leaves that metric unmeasured,
    which several `convert_db_value` implementations coerce to 0 — making `duplicate_count` equal the
    row count and reporting every row as a duplicate. `reconciliation`'s `duplicate_diff` already
    guards on this flag; the direct check needs the same guard.

    Runs on every data source: the flag defaults True, so this patches it to reproduce the condition
    a source like Salesforce reports natively.
    """
    from soda_core.common.data_source_impl import DataSourceImpl

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    monkeypatch.setattr(DataSourceImpl, "supports_row_hashing", property(lambda self: False))

    result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - duplicate:
                  columns: ['rep', 'country', 'zip']
        """,
    )
    check_results = [cr for cvr in result.contract_verification_results for cr in cvr.check_results]
    assert [cr.outcome.name for cr in check_results] == ["NOT_EVALUATED"]
    # Never a number: the false FAIL this guards against reported duplicate_count == row_count.
    assert check_results[0].diagnostic_metric_values.get("duplicate_count") is None
    # And loud. Without this the reason could be downgraded to debug and the scan would report zero
    # runtime errors — a silent NOT_EVALUATED with a clean exit, which is what the guard exists to
    # prevent. Asserting the message rather than has_errors, which other errors in a run also satisfy.
    assert "requires a row hash" in caplog.text
