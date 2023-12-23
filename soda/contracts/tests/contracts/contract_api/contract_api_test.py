import os
from textwrap import dedent

from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from soda.contracts.connection import Connection
from soda.contracts.contract import Contract
from soda.contracts.contract_verification_result import ContractVerificationResult
from soda.contracts.soda_cloud import SodaCloud


def test_contract_api(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    soda_cloud: SodaCloud = SodaCloud.from_yaml_str(dedent("""
        api_key_id: ${SODA_CLOUD_API_KEY_ID}
        api_key_secret: ${SODA_CLOUD_API_KEY_SECRET}
    """))
    with Connection.from_yaml_str(dedent("""
        type: postgres
        host: localhost
        database: sodasql
        username: sodasql
        password: ${POSTGRES_PWD}
    """)) as connection:
        contract: Contract = Contract.from_yaml_str(dedent(f"""
          schema: {data_source_fixture.schema_name}
          dataset: {table_name}
          columns:
          - name: id
          - name: first_name
            data_type: varchar
            checks:
            - type: no_missing_values
        """))
        contract_verification_result: ContractVerificationResult = contract.verify(connection, soda_cloud)
        # do something with the contract verification result
