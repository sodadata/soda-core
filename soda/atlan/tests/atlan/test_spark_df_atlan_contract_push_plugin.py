from __future__ import annotations

import os
from textwrap import dedent

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from contracts.helpers.contract_data_source_test_helper import ContractDataSourceTestHelper
from helpers.fixtures import project_root_dir
from helpers.test_table import TestTable

from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationResult,
)
from soda.execution.data_type import DataType
from spark_df_contract_data_source_test_helper import SparkDfContractDataSourceTestHelper

contracts_atlan_contract_test_table = TestTable(
    name="contracts_atlan_contract",
    # fmt: off
    columns=[
        ("id", DataType.TEXT),
        ("country", DataType.TEXT)
    ],
    values=[
        ('1', 'US'),
        ('2', 'US'),
        ('3', 'BE'),
    ]
    # fmt: on
)


class AtlanSparkDfContractDataSourceTestHelper(SparkDfContractDataSourceTestHelper):

    def _create_contract_data_source_yaml_dict(self, database_name: str | None, schema_name: str | None) -> dict:
        return {
            "atlan_qualified_name": "default/postgres/1718112025"
        }


# @pytest.mark.skip(
#     "Takes too long to be part of the local development test suite & depends on Atlan & Soda Cloud services"
# )
def test_atlan_contract_push_plugin():
    load_dotenv(f"{project_root_dir}/.env", override=True)

    contract_data_source_test_helper: AtlanSparkDfContractDataSourceTestHelper = AtlanSparkDfContractDataSourceTestHelper()
    contract_data_source_test_helper.start_test_session()
    exception: Exception | None = None
    try:
        unique_table_name: str = contract_data_source_test_helper.ensure_test_table(
            contracts_atlan_contract_test_table
        )

        spark_session: SparkSession = contract_data_source_test_helper.contract_data_source.spark_session
        df = spark_session.sql(f"SELECT * FROM {unique_table_name}")
        df.createOrReplaceTempView("students")

        contract_yaml_str: str = dedent(
            f"""
            data_source: spark_ds
            database: postgres
            schema: contracts
            dataset: students
            columns:
            - name: id
              data_type: varchar
            - name: country
              data_type: varchar
        """
        )

        soda_cloud_yaml_str: str = dedent(
            """
            api_key_id: ${DEV_SODADATA_IO_API_KEY_ID}
            api_key_secret: ${DEV_SODADATA_IO_API_KEY_SECRET}
        """
        )

        atlan_yaml_str: str = dedent(
            """
            plugin: atlan
            atlan_api_key: ${ATLAN_API_KEY}
            atlan_base_url: https://soda-partner.atlan.com
        """
        )

        contract_verification_result: ContractVerificationResult = (
            contract_data_source_test_helper
            .create_test_verification_builder()
            .with_contract_yaml_str(contract_yaml_str)
            .with_soda_cloud_yaml_str(soda_cloud_yaml_str)
            .with_plugin_yaml_str(atlan_yaml_str)
            .execute()
        )

        contract_verification_result.assert_ok()

    except Exception as e:
        exception = e
    finally:
        contract_data_source_test_helper.end_test_session(exception=exception)

    # data_source_yaml_str: str = dedent(
    #     """
    #     name: spark_ds
    #     type: spark_df
    #     atlan_qualified_name: default/postgres/1718112025
    #     connection:
    #         host: ${CONTRACTS_POSTGRES_HOST}
    #         database: ${CONTRACTS_POSTGRES_DATABASE}
    #         user: ${CONTRACTS_POSTGRES_USERNAME}
    #         password: ${CONTRACTS_POSTGRES_PASSWORD}
    #         schema: contracts
    # """
    # )
