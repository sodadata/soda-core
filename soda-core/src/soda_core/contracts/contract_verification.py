from __future__ import annotations

import logging
from typing import Optional

from soda_core.check_collections.check_collection_verification import (
    Check,
    CheckCollectionResult,
    CheckCollectionSessionResult,
    CheckCollectionVerificationSession,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationStatus,
    DataSource,
    Measurement,
    PostProcessingStage,
    PostProcessingStageState,
    ScanTokenUsage,
    SodaException,
    Threshold,
    YamlFileContentInfo,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import (
    CheckCollectionYamlSource,
    ContractYamlSource,
    DataSourceYamlSource,
)

logger: logging.Logger = soda_logger


class ContractVerificationSession(CheckCollectionVerificationSession):
    """Contract-typed subtype facade.

    Callers pass contract YAML sources in and get a typed
    :class:`ContractVerificationSessionResult` back. Heterogeneous (mixed-kind)
    sessions go through :class:`CheckCollectionVerificationSession` with
    ``specs=`` instead.
    """

    @classmethod
    def execute(
        cls,
        contract_yaml_sources: Optional[list[ContractYamlSource]] = None,
        only_validate_without_execute: bool = False,
        variables: Optional[dict[str, str]] = None,
        data_timestamp: Optional[str] = None,
        data_source_impls: Optional[list["DataSourceImpl"]] = None,
        data_source_yaml_sources: Optional[list[DataSourceYamlSource]] = None,
        soda_cloud_impl: Optional["SodaCloud"] = None,
        soda_cloud_publish_results: bool = False,
        soda_cloud_use_agent: bool = False,
        soda_cloud_verbose: bool = False,
        soda_cloud_use_agent_blocking_timeout_in_minutes: int = 60,
        check_paths: Optional[list[str]] = None,
        dwh_data_source_file_path: Optional[str] = None,
        check_selectors: Optional[list["CheckSelector"]] = None,
        check_collection_yaml_sources: Optional[list[CheckCollectionYamlSource]] = None,
    ) -> ContractVerificationSessionResult:
        """Execute a contract verification session.

        Either ``contract_yaml_sources`` (legacy) or
        ``check_collection_yaml_sources`` (canonical kwarg name) — both are
        treated as contracts and produce a :class:`ContractVerificationSessionResult`.
        """
        if contract_yaml_sources is not None and check_collection_yaml_sources is not None:
            raise TypeError("Pass either contract_yaml_sources (legacy) or check_collection_yaml_sources, not both")
        contract_sources = contract_yaml_sources if contract_yaml_sources is not None else check_collection_yaml_sources
        base_result = super().execute(
            check_collection_yaml_sources=contract_sources,
            only_validate_without_execute=only_validate_without_execute,
            variables=variables,
            data_timestamp=data_timestamp,
            data_source_impls=data_source_impls,
            data_source_yaml_sources=data_source_yaml_sources,
            soda_cloud_impl=soda_cloud_impl,
            soda_cloud_publish_results=soda_cloud_publish_results,
            soda_cloud_use_agent=soda_cloud_use_agent,
            soda_cloud_verbose=soda_cloud_verbose,
            soda_cloud_use_agent_blocking_timeout_in_minutes=soda_cloud_use_agent_blocking_timeout_in_minutes,
            check_paths=check_paths,
            dwh_data_source_file_path=dwh_data_source_file_path,
            check_selectors=check_selectors,
        )
        # The universal facade returns the base ``CheckCollectionSessionResult``;
        # rewrap as ``ContractVerificationSessionResult`` so callers iterating
        # ``contract_verification_results`` (the typed BC alias) keep working.
        return ContractVerificationSessionResult(check_collection_results=base_result.check_collection_results)


class ContractVerificationSessionResult(CheckCollectionSessionResult):
    """Public per-session result type.

    Inherits ``__init__`` from :class:`CheckCollectionSessionResult`; the only addition
    is the ``contract_verification_results`` property, a stable BC alias for callers
    iterating per-contract results before the abstraction renamed it to
    ``check_collection_results``.
    """

    @property
    def contract_verification_results(self) -> list["ContractVerificationResult"]:
        return self.check_collection_results


class ContractVerificationResult(CheckCollectionResult):
    pass


# Re-exports for backwards compatibility
__all__ = [
    "ContractVerificationSession",
    "ContractVerificationSessionResult",
    "ContractVerificationResult",
    "Contract",
    "DataSource",
    "Threshold",
    "Check",
    "CheckResult",
    "Measurement",
    "YamlFileContentInfo",
    "CheckOutcome",
    "ContractVerificationStatus",
    "ScanTokenUsage",
    "PostProcessingStage",
    "PostProcessingStageState",
    "SodaException",
    "CheckCollectionVerificationSession",
    "CheckCollectionSessionResult",
    "CheckCollectionResult",
]
