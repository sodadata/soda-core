from typing import Optional

from soda_core.common.logs import Emoticons, Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import YamlFileContent, YamlSource
from soda_core.contracts.contract_publication import ContractPublicationResultList
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml


class ContractPublicationImpl:
    def __init__(
        self,
        contract_yaml_sources: list[YamlSource],
        soda_cloud: Optional[SodaCloud],
        soda_cloud_yaml_source: Optional[YamlSource],
        variables: dict[str, str],
        logs: Logs = Logs(),
    ):
        self.logs: Logs = logs

        self.soda_cloud: Optional[SodaCloud] = soda_cloud
        if self.soda_cloud is None and soda_cloud_yaml_source is not None:
            soda_cloud_yaml_file_content: YamlFileContent = soda_cloud_yaml_source.parse_yaml_file_content(
                file_type="soda cloud", variables=variables, logs=logs
            )
            self.soda_cloud = SodaCloud.from_file(soda_cloud_yaml_file_content)

        self.contract_yamls: list[ContractYaml] = []
        self.contract_impls: list[ContractImpl] = []
        if contract_yaml_sources is None or len(contract_yaml_sources) == 0:
            self.logs.error(f"No contracts configured")
        else:
            for contract_yaml_source in contract_yaml_sources:
                contract_yaml: ContractYaml = ContractYaml.parse(
                    contract_yaml_source=contract_yaml_source, variables=variables, logs=logs
                )
                self.contract_yamls.append(contract_yaml)

    def execute(self) -> ContractPublicationResultList:
        if not self.soda_cloud:
            self.logs.warning("skipping publication because of missing Soda Cloud configuration")
            return ContractPublicationResultList(items=[], logs=self.logs)
        return ContractPublicationResultList(
            logs=self.logs,
            items=[self.soda_cloud.publish_contract(contract_yaml) for contract_yaml in self.contract_yamls],
        )
