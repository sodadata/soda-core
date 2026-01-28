from typing import Dict, Optional, Union

from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    ContractVerificationSessionResult,
)
from soda_core.telemetry.soda_telemetry import SodaTelemetry
from typing_extensions import deprecated

soda_telemetry = SodaTelemetry()


@deprecated("Use test_contract_locally instead")
def test_contracts(
    contract_file_paths: Union[str, list[str]],
    variables: Optional[Dict[str, str]] = None,
    verbose: bool = False,
) -> ContractVerificationSessionResult:
    # Limit the contract file paths to only 1. Throw an error if more than 1 is provided.
    if isinstance(contract_file_paths, list) and len(contract_file_paths) > 1:
        raise InvalidArgumentException("Only one contract is allowed at a time")

    return test_contract(
        contract_file_path=contract_file_paths[0],
        variables=variables,
        verbose=verbose,
    )


def test_contract(
    contract_file_path: str = None,
    variables: Optional[Dict[str, str]] = None,
    verbose: bool = False,
) -> ContractVerificationSessionResult:
    contract_yaml_sources = [ContractYamlSource.from_file_path(contract_file_path)]

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
