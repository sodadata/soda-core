import os
from textwrap import dedent

from dotenv import load_dotenv

from soda.contracts.contract_verification import ContractVerificationResult, ContractVerification


# @pytest.mark.skip("Takes too long to be part of the local development test suite & depends on Atlan & Soda Cloud services")
def test_atlan_contract_push_plugin():
    this_file_dir_path = os.path.dirname(os.path.realpath(__file__))
    load_dotenv(f"{this_file_dir_path}/.env", override=True)

    data_source_yaml_str: str = dedent("""
        name: postgres_ds
        type: postgres
        atlan_qualified_name: default/postgres/1718112025
        connection:
            host: ${CONTRACTS_POSTGRES_HOST}
            database: ${CONTRACTS_POSTGRES_DATABASE}
            username: ${CONTRACTS_POSTGRES_USERNAME}
            password: ${CONTRACTS_POSTGRES_PASSWORD}
            schema: contracts
    """)

    contract_yaml_str: str = dedent("""
        type: Table
        status: DRAFT
        kind: DataContract
        data_source: postgres_ds
        dataset: students
        columns:
        - name: id
          data_type: varchar
        - name: name
          data_type: varchar
        - name: age
          data_type: integer
    """)

    soda_cloud_yaml_str: str = dedent("""
        api_key_id: ${SODA_API_KEY_ID}
        api_key_secret: ${SODA_API_KEY_SECRET}
    """)

    atlan_yaml_str: str = dedent("""
        plugin: atlan
        atlan_api_key: ${ATLAN_API_KEY}
        atlan_base_url: ${ATLAN_BASE_URL}
    """)

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_str(contract_yaml_str)
        .with_data_source_yaml_str(data_source_yaml_str)
        .with_soda_cloud_yaml_str(soda_cloud_yaml_str)
        .with_plugin_yaml_str(atlan_yaml_str)
        .execute()
    )

    contract_verification_result.assert_ok()
