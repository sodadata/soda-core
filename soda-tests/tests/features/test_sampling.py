from datetime import datetime, timezone
from unittest import mock

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_fixtures import is_sampling_supported_data_source
from helpers.test_table import TestTableSpecification
from soda_core.common.soda_cloud_dto import (
    DatasetConfigurationDTO,
    TestRowSamplerConfigurationDTO,
)
from soda_core.contracts.contract_verification import ContractVerificationResult

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
@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_contract_test_scan_definition_type",
    new_callable=mock.PropertyMock(return_value=True),
)
@pytest.mark.skipif(not is_sampling_supported_data_source(), reason="Sampling not supported for this data source")
def test_sampling_simple_pass(
    mocked_is_running_on_agent,
    mocked_is_contract_test_scan_definition_type,
    data_source_test_helper: DataSourceTestHelper,
):
    # Simple test to verify that sampling is applied to all checks when enabled. Sample of 3 rows should be used, dataset has 5 rows.
    # Verifying diagnostics value verifies that. All tests in one test to save time.
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


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_agent",
    new_callable=mock.PropertyMock(return_value=True),
)
@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_contract_test_scan_definition_type",
    new_callable=mock.PropertyMock(return_value=True),
)
@pytest.mark.skipif(not is_sampling_supported_data_source(), reason="Sampling not supported for this data source")
def test_sampling_custom_sql_pass(
    mocked_is_running_on_agent,
    mocked_is_contract_test_scan_definition_type,
    data_source_test_helper: DataSourceTestHelper,
):
    # Checking the actual sql in logs, even though just in logs as result does not have them.
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.enable_soda_cloud_mock()

    age_quoted = data_source_test_helper.quote_column("age")
    table_full_name = test_table.qualified_name
    sampler_type = "absoluteLimit"
    sample_size = 3

    def add_sample(name: str) -> str:
        return f"{name} {data_source_test_helper.data_source_impl.sql_dialect._build_sample_sql(sampler_type, sample_size)}"

    def build_name_with_alias(name: str, alias: str) -> str:
        return f"{name} AS {alias}"

    data_source_test_helper.soda_cloud.set_dataset_configuration_response(
        dataset_identifier=test_table.dataset_identifier,
        dataset_configuration_dto=DatasetConfigurationDTO(
            test_row_sampler_configuration=TestRowSamplerConfigurationDTO(
                enabled=True, test_row_sampler={"type": sampler_type, "limit": sample_size}
            )
        ),
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
                - metric:
                    query: |
                        select avg({age_quoted}) from {build_name_with_alias(table_full_name, "metric_query")} where 1 = 1
                    threshold:
                        must_be_greater_than: 0
                - failed_rows:
                    query: |
                        select * from {build_name_with_alias(table_full_name, "fr_query")} where {age_quoted} > 100
        """,
    )

    # Simple way to verify that the sampling was applied to the custom sql queries is to check the generated sql in the logs.
    # The original query should not be there, only the modified one with sampling.
    # Use case-insensitive check to avoid issues with casing differences.
    logs = contract_verification_result.get_logs_str().lower()

    assert (
        f"from {build_name_with_alias(table_full_name, 'metric_query')} where 1 = 1".lower() not in logs
    ), "Original metric query should not be in logs"
    assert (
        f"from {add_sample(build_name_with_alias(table_full_name, 'metric_query'))}\nwhere\n  1 = 1".lower() in logs
    ), "Sampled metric query should be in logs"

    assert (
        f"from {build_name_with_alias(table_full_name, 'fr_query')} where {age_quoted} > 100".lower() not in logs
    ), "Original failed_rows query should not be in logs"
    assert (
        f"from {add_sample(build_name_with_alias(table_full_name, 'fr_query'))}\n  where\n    {age_quoted} > 100".lower()
        in logs
    ), "Sampled failed_rows query should be in logs"
