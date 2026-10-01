import logging
from typing import Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs, preserve_active_logs
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
        # Positional with contract_yamls: the errors that parsing and validating each contract logged.
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
                    # A variable without a value reads as absent until the contract is verified, so
                    # building the checks would report a bound or a configuration it names as missing.
                    # Such a contract is only checked as it parses.
                    if (
                        len(self.logs.get_errors()) == error_count_before_parse
                        and not contract_yaml_source.resolve_on_read_references_without_value
                    ):
                        self._validate_checks(contract_yaml)
                    self.contract_yamls.append(contract_yaml)
                    self.contract_yaml_errors.append(self.logs.get_errors()[error_count_before_parse:])

    def _validate_checks(self, contract_yaml: ContractYaml) -> None:
        """Build the contract's checks the way 'soda contract test' does, without a data
        source, so the errors only building them finds, like an inverted between range or a
        missing threshold, keep the contract from publishing too. Its records reach
        self.logs. The checks are built on a child of self.logs, since building them sets
        the label of their Logs."""
        with preserve_active_logs():
            validation_logs: Logs = self.logs.child()
            try:
                ContractImpl(yaml=contract_yaml, logs=validation_logs, only_validate_without_execute=True)
            except Exception as exc:
                # As 'soda contract test' reports it: the contract has an error, and the other
                # contracts of this publication still publish.
                logger.error(f"Could not validate the checks of the contract: {exc}")

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
