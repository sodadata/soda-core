from datetime import datetime, timezone

from freezegun import freeze_time
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("check_paths_filter")
    .column_integer("id")
    .column_varchar("name")
    .column_timestamp_tz("created_at")
    .rows(
        rows=[
            (1, "Alice", datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)),
            (2, "Bob", datetime(year=2025, month=1, day=2, hour=2, minute=0, second=0, tzinfo=timezone.utc)),
            (3, "Charlie", datetime(year=2025, month=1, day=3, hour=4, minute=0, second=0, tzinfo=timezone.utc)),
        ]
    )
    .build()
)


def test_check_paths(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
            test_table=test_table,
            check_paths=["columns.id.checks.aggregate"],
            contract_yaml_str="""
                check_attributes:
                    description: "Test description"
                columns:
                    - name: id
                      checks:
                        - aggregate:
                            function: avg
                            threshold:
                                must_be: 2
                        - invalid:
                            valid_values: [1, 2, 3]
                        - missing:
                        - duplicate:
                        - failed_rows:
                            expression: "id > 100"
                    - name: name
                    - name: created_at
                checks:
                    - schema:
                    - row_count:
                    - metric:
                        expression: "count(*)"
                        threshold:
                            must_be_greater_than: 2
                    - freshness:
                        column: created_at
                        threshold:
                            must_be_less_than: 12
            """,
        )
        assert len(contract_verification_result.check_results) == 9
        assert contract_verification_result.number_of_checks_excluded == 8

        # There should be only 2 measurements: one for the aggregate check on id, and one for the row count we collect for all contract verifications.
        assert len(contract_verification_result.measurements) == 2
