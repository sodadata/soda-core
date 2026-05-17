from __future__ import annotations

import logging
from typing import Optional

from soda_core.check_collections.check_collection_verification import (
    Check,
    CheckCollectionResult,
    CheckCollectionSessionResult,
    CheckCollectionStatus,
    CheckCollectionTarget,
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

        Re-raise contract (asymmetric with the universal facade):

        * **Single-input**: re-raises ANY ``originating_exception`` carried by
          the only ERROR-status result. Covers both ``SodaCoreException``
          family caller-input errors AND programming bugs (``ValueError``,
          ``AttributeError``, ...). Legacy single-contract callers that
          ``pytest.raises(YamlParserException)`` keep working, and unexpected
          bugs surface instead of being silently masked into a result.
        * **Multi-input**: never re-raises. Returns each failed slot as an
          ERROR-status placeholder so multi-contract callers can match results
          to inputs by index.
        * **Universal facade** (``CheckCollectionVerificationSession.execute``):
          never re-raises regardless of input size.

        The ``raise exc`` below preserves ``exc.__traceback__`` (Python keeps
        the traceback on stored exceptions across 3.x), so the original
        failure site is still visible in the re-raised traceback frame chain.
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

        # Single-input legacy re-raise — see docstring above for the full
        # asymmetry contract. ``raise exc`` (not ``raise exc from None``)
        # preserves ``exc.__traceback__`` so the original failure site stays
        # visible to ``pytest.raises``' traceback inspection and to operators
        # debugging from logs.
        results = base_result.check_collection_results
        if len(results) == 1:
            exc = getattr(results[0], "originating_exception", None)
            if exc is not None:
                raise exc

        # The universal facade returns the base ``CheckCollectionSessionResult``;
        # rewrap as ``ContractVerificationSessionResult`` so callers iterating
        # ``contract_verification_results`` (the typed BC alias) keep working.
        return ContractVerificationSessionResult(check_collection_results=results)


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
    "CheckCollectionTarget",
    "DataSource",
    "Threshold",
    "Check",
    "CheckResult",
    "Measurement",
    "YamlFileContentInfo",
    "CheckOutcome",
    "CheckCollectionStatus",
    "ContractVerificationStatus",
    "ScanTokenUsage",
    "PostProcessingStage",
    "PostProcessingStageState",
    "SodaException",
    "CheckCollectionVerificationSession",
    "CheckCollectionSessionResult",
    "CheckCollectionResult",
]
