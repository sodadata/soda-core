from unittest import mock
from datetime import datetime, timezone
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.common.soda_cloud_dto import DatasetConfigurationDTO, TestRowSamplerConfigurationDTO
from soda_core.contracts.contract_verification import (
    CheckResult,
    ContractVerificationResult,
)
from helpers.test_fixtures import is_sampling_supported_data_source
import pytest

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("sampling")
    .column_varchar("country", 3)
    .column_integer("age")
    .column_timestamp_tz("event_time")
    .rows(
        rows=[
            ("USA", 5, datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
            ("BE", 2, datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
            ("BE", 1, datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
            ("BE", 10, datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
            ("BE", 8, datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)),
        ]
    )
    .build()
)

country_test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("sampling_countries")
    .column_varchar("country", 3)
    .rows(
        rows=[
            ("USA",),
            ("BE",),
        ]
    )
    .build()
)


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_agent",
    new_callable=mock.PropertyMock(return_value=True),
)
@pytest.mark.skipif(not is_sampling_supported_data_source(), reason="Sampling not supported for this data source")
def test_sampling_simple_pass(mocked_env_config_helper, data_source_test_helper: DataSourceTestHelper):
    # Simple test to verify that sampling is applied to all checks when enabled. Sample of 3 rows should be used, dataset has 5 rows.
    # Verifying diagnostics value verifies that.
    # TODO: Testing the query generated would be more efficient and more strict but is more complex to implement.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    country_test_table = data_source_test_helper.ensure_test_table(country_test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock()

    data_source_test_helper.soda_cloud.set_dataset_configuration_response(
        dataset_identifier=test_table.dataset_identifier,
        dataset_configuration_dto=DatasetConfigurationDTO(
            test_row_sampler_configuration=TestRowSamplerConfigurationDTO(
                enabled=True, test_row_sampler={"type": "absoluteLimit", "limit": 3}
            )
        ),
    )
    age_quoted = data_source_test_helper.quote_column("age")
    country_quoted = data_source_test_helper.quote_column("country")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        publish_results=False,
        contract_yaml_str=f"""
            filter: '{age_quoted} IS NOT NULL AND {country_quoted} IS NOT NULL'
            columns:
              - name: age
                invalid_values: [0]
                checks:
                    - aggregate:
                        function: avg
                        threshold:
                            must_be_greater_than: 0
                    - duplicate:
                    - failed_rows:
                        qualifier: fr_expression
                        expression: |
                            {age_quoted} > 100
                    - invalid:
                    - missing:
              - name: country
                checks:
                    - invalid:
                        qualifier: reference_check
                        valid_reference_data:
                            dataset: {data_source_test_helper.build_dqn(country_test_table)}
                            column: {country_quoted}
            checks:
                - freshness:
                    column: event_time
                    threshold:
                        must_be_greater_than: 10
                - metric:
                    expression: |
                        AVG({age_quoted})
                    threshold:
                        must_be_greater_than: 0
                    filter: 1 = 1
                - row_count:
                    filter: 2 = 2

        """,
    )

    for check_result in contract_verification_result.check_results:
        if "dataset_rows_tested" in check_result.diagnostic_metric_values:
            assert check_result.diagnostic_metric_values["dataset_rows_tested"] == 3

        if "check_rows_tested" in check_result.diagnostic_metric_values:
            assert check_result.diagnostic_metric_values["check_rows_tested"] == 3


# - failed_rows:
#     qualifier: fr_query
#     query: |
#     Select * from {test_table.qualified_name} where {age_quoted} > 100

# - metric:
#     query: |
#         SELECT AVG({age_quoted})
#         FROM {test_table.qualified_name}
#     threshold:
#         must_be_greater_than: 0
#     filter: 1 = 1
