from textwrap import dedent

from dotenv import load_dotenv
from helpers.fixtures import project_root_dir
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationResult,
)

contracts_atlan_contract_test_table = TestTable(
    name="contracts_atlan_contract",
    # fmt: off
    columns=[
        ("one", DataType.TEXT)
    ],
    values=[
        ('ID1',),
        ('XXX',),
        ('N/A',),
        (None,),
    ]
    # fmt: on
)


# @pytest.mark.skip(
#     "Takes too long to be part of the local development test suite & depends on Atlan & Soda Cloud services"
# )
def test_atlan_contract_push_plugin():
    load_dotenv(f"{project_root_dir}/.env", override=True)

    data_source_yaml_str: str = dedent(
        """
        name: postgres_ds
        type: postgres
        atlan_qualified_name: default/postgres/1718112025
        connection:
            host: ${CONTRACTS_POSTGRES_HOST}
            database: ${CONTRACTS_POSTGRES_DATABASE}
            user: ${CONTRACTS_POSTGRES_USERNAME}
            password: ${CONTRACTS_POSTGRES_PASSWORD}
            schema: contracts
    """
    )

    contract_yaml_str: str = dedent(
        f"""
        data_source: postgres_ds
        database: ${{CONTRACTS_POSTGRES_DATABASE}}
        schema: contracts
        dataset: students
        columns:
        - name: id
          data_type: varchar
        - name: name
          data_type: varchar
        - name: age
          data_type: integer
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
        ContractVerification.builder()
        .with_contract_yaml_str(contract_yaml_str)
        .with_data_source_yaml_str(data_source_yaml_str)
        .with_soda_cloud_yaml_str(soda_cloud_yaml_str)
        .with_plugin_yaml_str(atlan_yaml_str)
        .execute()
    )

    contract_verification_result.assert_ok()
