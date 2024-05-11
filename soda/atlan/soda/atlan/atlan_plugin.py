from json import dumps

from soda.contracts.contract import ContractResult
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.plugin import Plugin
from soda.contracts.impl.yaml_helper import YamlFile


class AtlanPlugin(Plugin):

    def __init__(self, logs: Logs, plugin_name: str, plugin_yaml_files: list[YamlFile]):
        super().__init__(logs, plugin_name, plugin_yaml_files)
        atlan_configuration_dict: dict = self.plugin_yaml_files[0].dict
        self.atlan_api_key: str = atlan_configuration_dict["atlan_api_key"]
        self.atlan_base_url: str = atlan_configuration_dict["atlan_base_url"]

    def process_contract_results(self, contract_result: ContractResult) -> None:
        from pyatlan.client.atlan import AtlanClient
        from pyatlan.model.assets import DataContract

        data_source = contract_result.contract.data_source
        atlan_qualified_name: str = data_source.data_source_yaml_dict.get("atlan_qualified_name")
        if not isinstance(atlan_qualified_name, str):
            self.logs.error("atlan_qualified_name is required in a data source configuration yaml when using the Atlan plugin.  Disabling Atlan integration.")
            return None

        database_name: str = data_source.get_database_name()
        schema_name: str = contract_result.contract.schema
        dataset_name: str = contract_result.contract.dataset
        dataset_atlan_qualified_name: str = f"{atlan_qualified_name}/{database_name}/{schema_name}/{dataset_name}"

        contract_dict: dict = contract_result.contract.contract_file.dict
        contract_json_str: str = dumps(contract_dict)

        client = AtlanClient(
            base_url=self.atlan_base_url,
            api_key=self.atlan_api_key
        )
        contract = DataContract.creator(  #
            asset_qualified_name=dataset_atlan_qualified_name,
            contract_json=contract_json_str,
        )
        response = client.asset.save(contract)
        self.logs.info(str(response))
