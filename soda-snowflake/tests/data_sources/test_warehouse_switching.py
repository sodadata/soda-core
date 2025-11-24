from typing import Optional
from unittest import mock

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.soda_cloud_dto import (
    ComputeWarehouseOverrideDTO,
    DatasetConfigurationDTO,
)
from soda_core.contracts.contract_verification import ContractVerificationResult
from sqlglot import logger

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("warehouse_switching")
    .column_integer("id")
    .rows(
        rows=[
            (1,),
        ]
    )
    .build()
)


def stub_get_current_warehouse() -> Optional[str]:
    logger.info("Executing SQL to get current warehouse: SELECT CURRENT_WAREHOUSE()")
    return "OLD_WAREHOUSE"


def stub_get_current_warehouse_none() -> Optional[str]:
    logger.info("Executing SQL to get current warehouse: SELECT CURRENT_WAREHOUSE()")
    return None


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_agent",
    new_callable=mock.PropertyMock(return_value=True),
)
@mock.patch(
    "soda_snowflake.common.data_sources.snowflake_data_source.SnowflakeDataSourceImpl.switch_warehouse",
    side_effect=lambda warehouse: logger.info(f"Executing SQL to switch warehouse to: USE WAREHOUSE {warehouse}"),
)
@mock.patch(
    "soda_snowflake.common.data_sources.snowflake_data_source.SnowflakeDataSourceImpl.get_current_warehouse",
    side_effect=stub_get_current_warehouse,
)
def test_warehouse_switching(
    mocked_is_running_on_agent,
    mocked_switch_warehouse,
    mocked_get_current_warehouse,
    data_source_test_helper: DataSourceTestHelper,
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    data_source_test_helper.soda_cloud.set_dataset_configuration_response(
        dataset_identifier=test_table.dataset_identifier,
        dataset_configuration_dto=DatasetConfigurationDTO(
            compute_warehouse_override=ComputeWarehouseOverrideDTO(name="NEW_WAREHOUSE")
        ),
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
                - row_count:
        """,
    )

    logs = contract_verification_result.get_logs_str().lower()

    assert "select current_warehouse()" in logs, "Expected current warehouse query not found in logs"
    assert "old_warehouse" in logs, "Expected old warehouse not found in logs"
    assert f"USE WAREHOUSE NEW_WAREHOUSE".lower() in logs, "Expected warehouse switch command not found in logs"


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_agent",
    new_callable=mock.PropertyMock(return_value=True),
)
@mock.patch(
    "soda_snowflake.common.data_sources.snowflake_data_source.SnowflakeDataSourceImpl.switch_warehouse",
    side_effect=lambda warehouse: logger.info(f"Executing SQL to switch warehouse to: USE WAREHOUSE {warehouse}"),
)
@mock.patch(
    "soda_snowflake.common.data_sources.snowflake_data_source.SnowflakeDataSourceImpl.get_current_warehouse",
    side_effect=stub_get_current_warehouse_none,
)
def test_warehouse_switching_no_current_wh(
    mocked_is_running_on_agent,
    mocked_switch_warehouse,
    mocked_get_current_warehouse,
    data_source_test_helper: DataSourceTestHelper,
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock()

    data_source_test_helper.soda_cloud.set_dataset_configuration_response(
        dataset_identifier=test_table.dataset_identifier,
        dataset_configuration_dto=DatasetConfigurationDTO(
            compute_warehouse_override=ComputeWarehouseOverrideDTO(name="NEW_WAREHOUSE")
        ),
    )

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=f"""
            checks:
                - row_count:
        """,
    )

    logs = contract_verification_result.get_logs_str().lower()

    assert "select current_warehouse()" in logs, "Expected current warehouse query not found in logs"
    assert "old_warehouse" not in logs, "Unexpected old warehouse found in logs"
    assert f"USE WAREHOUSE NEW_WAREHOUSE".lower() in logs, "Expected warehouse switch command not found in logs"
