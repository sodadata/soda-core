import base64
import os
from json import dumps
from textwrap import dedent
from typing import List

import requests
from dotenv import load_dotenv
from ruamel.yaml import YAML

from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationResult,
)

contract_yaml_str: str = dedent(
    """
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
    - name: gender
"""
)


def load_environment_variables():
    this_file_dir_path = os.path.dirname(os.path.realpath(__file__))
    load_dotenv(f"{this_file_dir_path}/.env", override=True)


def atlan_save_contract():
    print()
    print("Atlan save contract")

    from datetime import datetime

    print(f"1 {datetime.now()}")
    from pyatlan.client.atlan import AtlanClient

    print(f"2 {datetime.now()}")
    from pyatlan.model.assets import DataContract

    print(f"3 {datetime.now()}")

    connection_atlan_qualified_name: str = "default/postgres/1718112025"
    database_name: str = "postgres"
    schema_name: str = "contracts"
    dataset_name: str = "students"

    yaml: YAML = YAML()
    contract_dict: dict = yaml.load(contract_yaml_str)

    client = AtlanClient(base_url="https://soda-partner.atlan.com", api_key=os.environ.get("ATLAN_API_KEY"))
    contract = DataContract.creator(  #
        asset_qualified_name=f"{connection_atlan_qualified_name}/{database_name}/{schema_name}/{dataset_name}",
        contract_json=dumps(contract_dict),
    )
    response = client.asset.save(contract)

    print(str(response))


def soda_verify_contract():
    print()
    print("Soda verify contract")

    data_source_yaml_str: str = dedent(
        """
        name: postgres_ds
        type: postgres
        atlan_qualified_name: default/postgres/1718112025
        connection:
            host: ${CONTRACTS_POSTGRES_HOST}
            database: ${CONTRACTS_POSTGRES_DATABASE}
            username: ${CONTRACTS_POSTGRES_USERNAME}
            password: ${CONTRACTS_POSTGRES_PASSWORD}
            schema: contracts
    """
    )

    soda_cloud_yaml_str: str = dedent(
        """
        api_key_id: ${SODA_API_KEY_ID}
        api_key_secret: ${SODA_API_KEY_SECRET}
    """
    )

    atlan_yaml_str: str = dedent(
        """
        plugin: atlan
        atlan_api_key: ${ATLAN_API_KEY}
        atlan_base_url: ${ATLAN_BASE_URL}
    """
    )

    contract_verification_result: ContractVerificationResult = (
        ContractVerification.builder()
        .with_contract_yaml_str(contract_yaml_str)
        .with_data_source_yaml_str(data_source_yaml_str)
        .with_database_name(os.environ[""])
        .with_schema_name("public")
        .with_soda_cloud_yaml_str(soda_cloud_yaml_str)
        .with_plugin_yaml_str(atlan_yaml_str)
        .execute()
    )

    print(str(contract_verification_result))


def soda_print_datasets():
    soda_client = ContractsSodaCloudClient()
    dataset_dicts = soda_client.get_datasets()
    from io import StringIO

    string_io = StringIO()
    YAML().dump(dataset_dicts, string_io)
    print(string_io.getvalue())


from soda.cloud.soda_cloud import SodaCloud as SodaCLSodaCloud


class ContractsSodaCloudClient(SodaCLSodaCloud):

    def __init__(self):
        from soda.common import logs as soda_core_logs
        from soda.scan import logger as scan_logger

        scan_logs = soda_core_logs.Logs(logger=scan_logger)
        super().__init__(
            host="cloud.soda.io",
            api_key_id=os.environ["SODA_API_KEY_ID"],
            api_key_secret=os.environ["SODA_API_KEY_SECRET"],
            token=None,
            port=None,
            logs=scan_logs,
        )

    def get_datasets(self) -> List[dict]:
        headers = self.headers.copy()
        token_str: str = f"{self.api_key_id}:{self.api_key_secret}"
        token_bytes = token_str.encode("ascii")
        token_base64_bytes = base64.b64encode(token_bytes)
        token_base64_ascii_str = token_base64_bytes.decode("ascii")
        headers["Authorization"] = f"Basic {token_base64_ascii_str}"
        headers["Accept"] = "application/json"
        response = requests.get(url=f"{self.api_url}/v1/datasets", headers=headers)
        print(response.json())


if __name__ == "__main__":
    load_environment_variables()
    atlan_save_contract()
    soda_verify_contract()
    soda_print_datasets()
