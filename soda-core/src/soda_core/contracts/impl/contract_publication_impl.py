import logging
from typing import Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource, SodaCloudYamlSource
from soda_core.contracts.contract_publication import ContractPublicationResult, ContractPublicationResultList
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
        self.contract_yamls: list[ContractYaml] = []
        # Positional with contract_yamls: the errors that parsing each contract logged.
        self.contract_yaml_errors: list[list[str]] = []
        self.contract_impls: list[ContractImpl] = []
        # Publication reads back the errors each parse logged, so capture them in self.logs even
        # when another Logs became active after it was created.
        with self.logs.activate():
            if self.soda_cloud is None and soda_cloud_yaml_source is not None:
                self.soda_cloud = SodaCloud.from_yaml_source(
                    soda_cloud_yaml_source=soda_cloud_yaml_source, provided_variable_values=variables
                )

            if contract_yaml_sources is None or len(contract_yaml_sources) == 0:
                logger.error(f"No contracts configured")
            else:
                for contract_yaml_source in contract_yaml_sources:
                    error_count_before_parse: int = len(self.logs.get_errors())
                    contract_yaml: ContractYaml = ContractYaml(
                        yaml_source=contract_yaml_source,
                        provided_variable_values=variables,
                        leave_variables_without_value_unresolved=True,
                    )
                    self.contract_yamls.append(contract_yaml)
                    self.contract_yaml_errors.append(self.logs.get_errors()[error_count_before_parse:])

    def execute(self) -> ContractPublicationResultList:
        with self.logs.activate():
            if not self.soda_cloud:
                logger.warning("skipping publication because of missing Soda Cloud configuration")
                return ContractPublicationResultList(items=[], logs=self.logs)
            return ContractPublicationResultList(
                items=[
                    self._publish_contract(contract_yaml, parse_errors)
                    for contract_yaml, parse_errors in zip(self.contract_yamls, self.contract_yaml_errors)
                ],
                logs=self.logs,
            )

    def _publish_contract(self, contract_yaml: ContractYaml, parse_errors: list[str]) -> ContractPublicationResult:
        if parse_errors:
            file_path: Optional[str] = contract_yaml.yaml_source.file_path
            contract_name: str = f"contract '{file_path}'" if file_path else "the contract"
            error_count: str = "1 error" if len(parse_errors) == 1 else f"{len(parse_errors)} errors"
            logger.error(
                f"Skipping publication of {contract_name} because it has {error_count}: {'; '.join(parse_errors)}"
            )
            return ContractPublicationResult(contract=None)
        return self.soda_cloud.publish_contract(contract_yaml)
