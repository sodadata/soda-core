from typing import Dict, Optional, Union

from soda_core.common.logging_configuration import configure_logging
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    ContractVerificationSessionResult,
)
from soda_core.telemetry.soda_telemetry import SodaTelemetry

soda_telemetry = SodaTelemetry()


def test_contracts(
    contract_file_paths: Union[str, list[str]],
    variables: Optional[Dict[str, str]] = None,
    verbose: bool = False,
) -> ContractVerificationSessionResult:
    configure_logging(verbose=verbose)

    if isinstance(contract_file_paths, str):
        contract_file_paths = [contract_file_paths]

    contract_yaml_sources = [
        ContractYamlSource.from_file_path(contract_file_path) for contract_file_path in contract_file_paths
    ]

    contract_verification_result = ContractVerificationSession.execute(
        contract_yaml_sources=contract_yaml_sources,
        variables=variables,
        only_validate_without_execute=True,
        soda_cloud_publish_results=False,
        soda_cloud_use_agent=False,
        soda_cloud_verbose=verbose,
    )

    soda_telemetry.ingest_contract_verification_session_result(
        contract_verification_session_result=contract_verification_result
    )

    return contract_verification_result
