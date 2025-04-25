import logging
from typing import Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource
from soda_core.contracts.contract_publication import ContractPublicationResultList
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from soda_core.contracts.impl.contract_yaml import ContractYaml

logger: logging.Logger = soda_logger


class ContractPublicationImpl:
    def __init__(
        self,
        contract_yaml_sources: list[ContractYamlSource],
        soda_cloud: Optional[SodaCloud],
        soda_cloud_yaml_source: Optional[SodaCloudYamlSource],
        variables: dict[str, str],
        logs: Logs,
    ):
        self.logs: Logs = logs

        self.soda_cloud: Optional[SodaCloud] = soda_cloud
        if self.soda_cloud is None and soda_cloud_yaml_source is not None:
            self.soda_cloud = SodaCloud.from_yaml_source(
                soda_cloud_yaml_source=soda_cloud_yaml_source, provided_variable_values=variables
            )

        self.contract_yamls: list[ContractYaml] = []
        self.contract_impls: list[ContractImpl] = []
        if contract_yaml_sources is None or len(contract_yaml_sources) == 0:
            logger.error(f"No contracts configured")
        else:
            for contract_yaml_source in contract_yaml_sources:
                contract_yaml: ContractYaml = ContractYaml.parse(
                    contract_yaml_source=contract_yaml_source, provided_variable_values=variables
                )
                self.contract_yamls.append(contract_yaml)

    def execute(self) -> ContractPublicationResultList:
        if not self.soda_cloud:
            logger.warning("skipping publication because of missing Soda Cloud configuration")
            return ContractPublicationResultList(items=[], logs=self.logs)
        return ContractPublicationResultList(
            items=[self.soda_cloud.publish_contract(contract_yaml) for contract_yaml in self.contract_yamls],
            logs=self.logs,
        )
