from datetime import datetime, timezone

from freezegun import freeze_time
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("attributes")
    .column_integer("id")
    .column_timestamp_tz("created_at")
    .rows(
        rows=[
            (1, datetime(year=2025, month=1, day=1, hour=0, minute=0, second=0, tzinfo=timezone.utc)),
            (2, datetime(year=2025, month=1, day=2, hour=2, minute=0, second=0, tzinfo=timezone.utc)),
            (3, datetime(year=2025, month=1, day=3, hour=4, minute=0, second=0, tzinfo=timezone.utc)),
        ]
    )
    .build()
)


def test_attributes_global_apply(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
            test_table=test_table,
            contract_yaml_str=f"""
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
                            valid_values: ['1', '2', '3']
                        - missing:
                        - duplicate:
                    - name: created_at
                checks:
                    - schema:
                    - freshness:
                        column: created_at
                        threshold:
                            must_be_less_than: 12
            """,
        )
        for check_result in contract_verification_result.check_results:
            assert check_result.check.attributes == {
                "description": "Test description",
            }


def test_attributes_individual_apply_and_override(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
        ]
    )

    with freeze_time(datetime(year=2025, month=1, day=3, hour=10, minute=0, second=0, tzinfo=timezone.utc)):
        contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
            test_table=test_table,
            contract_yaml_str=f"""
                check_attributes:
                    description_global: "Test description"
                    description: "Default description - will be overridden by column attributes"
                columns:
                    - name: id
                      checks:
                        - aggregate:
                            function: avg
                            threshold:
                                must_be: 2
                            attributes:
                                description: "Test description"
                        - invalid:
                            valid_values: ['1', '2', '3']
                            attributes:
                                description: "Test description"
                        - missing:
                            attributes:
                                description: "Test description"
                        - duplicate:
                            attributes:
                                description: "Test description"
                    - name: created_at
                checks:
                    - schema:
                        attributes:
                            description: "Test description"
                    - freshness:
                        column: created_at
                        threshold:
                            must_be_less_than: 12
                        attributes:
                            description: "Test description"
            """,
        )
        for check_result in contract_verification_result.check_results:
            assert check_result.check.attributes == {
                "description_global": "Test description",
                "description": "Test description",
            }
