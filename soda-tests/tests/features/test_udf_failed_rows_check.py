from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud, AssertFloatBetween
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)

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

    mock_soda_cloud: MockSodaCloud = data_source_test_helper.enable_soda_cloud_mock()

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
              - failed_rows:
                  expression: |
                    ({end_quoted} - {start_quoted}) > 5
        """,
    )

    mock_soda_cloud.get_request_insert_scan_results().assert_json_subdict({
        "checks": [
            {
                "diagnostics": {
                    "v4": {
                        "type": "failed_rows",
                        "failedRowsCount": 2,
                        "failedRowsPercent": AssertFloatBetween(66, 67),
                        "datasetRowsTested": 3,
                        "checkRowsTested": 3,
                    }
                }
            }
        ]
    })


def test_failed_rows_query(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    end_quoted = data_source_test_helper.quote_column("end")
    start_quoted = data_source_test_helper.quote_column("start")

    mock_soda_cloud: MockSodaCloud = data_source_test_helper.enable_soda_cloud_mock()

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

    mock_soda_cloud.get_request_insert_scan_results().assert_json_subdict({
        "checks": [
            {
                "diagnostics": {
                    "v4": {
                        "type": "failed_rows",
                        "failedRowsCount": 2,

                        # TODO remove after issue DTL-922 is fixed
                        "datasetRowsTested": 0,
                        "checkRowsTested": 0,
                        "failedRowsPercent": 0,
                    }
                }
            }
        ]
    })
