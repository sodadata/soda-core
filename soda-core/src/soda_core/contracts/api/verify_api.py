import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Union

from soda_core.common._deprecation import deprecated_kwarg, warn_deprecated
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import (
    ContractFetchFailedException,
    DatasetQueryException,
    InvalidArgumentException,
    SodaCloudAuthenticationFailedException,
    SodaCloudException,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs, preserve_active_logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource, build_data_source_yaml_sources
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    ContractVerificationResult,
    ContractVerificationSession,
    ContractVerificationSessionResult,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.diagnostics_warehouse_files import DiagnosticsWarehouseFiles
from soda_core.telemetry.soda_telemetry import SodaTelemetry
from typing_extensions import deprecated

logger: logging.Logger = soda_logger

soda_telemetry = SodaTelemetry()

__AT_LEAST_ONE_CONTRACT_OR_DATASET_REQUIRED = "At least one of -c/--contract or -d/--dataset arguments is required."


@deprecated("Use verify_contract_locally instead")
def verify_contracts_locally(
    data_source_file_path: Optional[str] = None,
    data_source_file_paths: list[str] = [],
    data_sources: Optional[Union[list[DataSourceImpl], DataSourceImpl]] = [],
    contract_file_paths: Optional[Union[str, list[str]]] = None,
    dataset_identifiers: Optional[list[str]] = None,
    soda_cloud_file_path: Optional[str] = None,
    variables: Optional[Dict[str, str]] = None,
    data_timestamp: Optional[str] = None,
    publish: bool = False,
    check_paths: Optional[list[str]] = None,
    dwh_data_source_file_path: Optional[Union[str, DiagnosticsWarehouseFiles]] = None,
    check_selectors: Optional[list[CheckSelector]] = None,
) -> ContractVerificationSessionResult:
    if not contract_file_paths and not dataset_identifiers:
        raise InvalidArgumentException(__AT_LEAST_ONE_CONTRACT_OR_DATASET_REQUIRED)

    if isinstance(contract_file_paths, str):
        contract_file_paths = [contract_file_paths]

    if (
        contract_file_paths and len(contract_file_paths) > 1
    ):  # We still need the "none" check, if there is no contract file path, we don't want to raise an error for the len()
        raise InvalidArgumentException("Only one contract is allowed at a time")

    if dataset_identifiers and len(dataset_identifiers) > 1:
        raise InvalidArgumentException("Only one dataset identifier is allowed at a time")

    if contract_file_paths and dataset_identifiers:
        logger.info(
            "Both contract file paths and dataset identifiers are provided. Only evaluating the contract file paths."
        )

    assert isinstance(dataset_identifiers, list), f"Expected a list, got {type(dataset_identifiers)}"

    contract_file_path = __attempt_pick_first_element(contract_file_paths)
    dataset_identifier = __attempt_pick_first_element(dataset_identifiers)

    return verify_contract_locally(
        data_source_file_path=data_source_file_path,
        data_source_file_paths=data_source_file_paths,
        data_sources=data_sources,
        contract_file_path=contract_file_path,
        dataset_identifier=dataset_identifier,
        soda_cloud_file_path=soda_cloud_file_path,
        variables=variables,
        data_timestamp=data_timestamp,
        publish=publish,
        check_paths=check_paths,
        dwh_data_source_file_path=dwh_data_source_file_path,
        check_selectors=check_selectors,
    )


def verify_contract_locally(
    data_source_file_path: Optional[str] = None,
    data_source_file_paths: list[str] = [],
    data_sources: Optional[Union[list[DataSourceImpl], DataSourceImpl]] = [],
    contract_file_path: Optional[str] = None,
    dataset_identifier: Optional[str] = None,
    soda_cloud_file_path: Optional[str] = None,
    variables: Optional[Dict[str, str]] = None,
    data_timestamp: Optional[str] = None,
    publish: bool = False,
    check_paths: Optional[list[str]] = None,
    dwh_data_source_file_path: Optional[Union[str, DiagnosticsWarehouseFiles]] = None,
    check_selectors: Optional[list[CheckSelector]] = None,
) -> ContractVerificationSessionResult:
    """
    Verifies the contract locally.
    """
    if not isinstance(data_sources, list):
        data_sources = [data_sources]

    return verify_contract(
        contract_file_path=contract_file_path,
        dataset_identifier=dataset_identifier,
        data_source_file_path=data_source_file_path,
        data_source_file_paths=data_source_file_paths,
        data_sources=data_sources,
        soda_cloud_file_path=soda_cloud_file_path,
        variables=variables,
        data_timestamp=data_timestamp,
        publish=publish,
        use_runner=False,
        check_paths=check_paths,
        dwh_data_source_file_path=dwh_data_source_file_path,
        check_selectors=check_selectors,
    )


@deprecated("Use verify_contract_on_runner instead")
def verify_contracts_on_runner(
    soda_cloud_file_path: str,
    contract_file_paths: Optional[Union[str, list[str]]] = None,
    dataset_identifiers: Optional[list[str]] = None,
    data_source_file_path: Optional[str] = None,
    data_source_file_paths: list[str] = [],
    variables: Optional[Dict[str, str]] = None,
    publish: bool = False,
    verbose: bool = False,
    blocking_timeout_in_minutes: int = 60,
) -> ContractVerificationSessionResult:
    if not contract_file_paths and not dataset_identifiers:
        raise InvalidArgumentException(__AT_LEAST_ONE_CONTRACT_OR_DATASET_REQUIRED)

    if isinstance(contract_file_paths, str):
        contract_file_paths = [contract_file_paths]

    if (
        contract_file_paths and len(contract_file_paths) > 1
    ):  # We still need the "none" check, if there is no contract file path, we don't want to raise an error for the len()
        raise InvalidArgumentException("Only one contract is allowed at a time")

    if dataset_identifiers and len(dataset_identifiers) > 1:
        raise InvalidArgumentException("Only one dataset identifier is allowed at a time")

    if contract_file_paths and dataset_identifiers:
        logger.info(
            "Both contract file paths and dataset identifiers are provided. Only evaluating the contract file paths."
        )

    # ``__attempt_pick_first_element`` handles None safely; no list-type
    # assertion needed (an earlier assert here tripped on the common
    # ``dataset_identifiers=None`` call shape).
    contract_file_path = __attempt_pick_first_element(contract_file_paths)
    dataset_identifier = __attempt_pick_first_element(dataset_identifiers)

    return verify_contract_on_runner(
        soda_cloud_file_path=soda_cloud_file_path,
        contract_file_path=contract_file_path,
        dataset_identifier=dataset_identifier,
        data_source_file_path=data_source_file_path,
        data_source_file_paths=data_source_file_paths,
        variables=variables,
        publish=publish,
        verbose=verbose,
        blocking_timeout_in_minutes=blocking_timeout_in_minutes,
    )


@deprecated("Use verify_contract_on_runner instead")
def verify_contracts_on_agent(
    soda_cloud_file_path: str,
    contract_file_paths: Optional[Union[str, list[str]]] = None,
    dataset_identifiers: Optional[list[str]] = None,
    data_source_file_path: Optional[str] = None,
    data_source_file_paths: list[str] = [],
    variables: Optional[Dict[str, str]] = None,
    publish: bool = False,
    verbose: bool = False,
    blocking_timeout_in_minutes: int = 60,
) -> ContractVerificationSessionResult:
    # Don't route through ``verify_contracts_on_runner`` — that path is itself deprecated and
    # would emit a second redundant warning. Apply the same first-element-pick that the plural
    # variant did and delegate straight to the canonical singular function.
    if not contract_file_paths and not dataset_identifiers:
        raise InvalidArgumentException(__AT_LEAST_ONE_CONTRACT_OR_DATASET_REQUIRED)
    if isinstance(contract_file_paths, str):
        contract_file_paths = [contract_file_paths]
    if contract_file_paths and len(contract_file_paths) > 1:
        raise InvalidArgumentException("Only one contract is allowed at a time")
    if dataset_identifiers and len(dataset_identifiers) > 1:
        raise InvalidArgumentException("Only one dataset identifier is allowed at a time")
    if contract_file_paths and dataset_identifiers:
        logger.info(
            "Both contract file paths and dataset identifiers are provided. Only evaluating the contract file paths."
        )
    contract_file_path = contract_file_paths[0] if contract_file_paths else None
    dataset_identifier = dataset_identifiers[0] if dataset_identifiers else None
    return verify_contract_on_runner(
        soda_cloud_file_path=soda_cloud_file_path,
        contract_file_path=contract_file_path,
        dataset_identifier=dataset_identifier,
        data_source_file_path=data_source_file_path,
        data_source_file_paths=data_source_file_paths,
        variables=variables,
        publish=publish,
        verbose=verbose,
        blocking_timeout_in_minutes=blocking_timeout_in_minutes,
    )


def verify_contract_on_runner(
    soda_cloud_file_path: str,
    contract_file_path: Optional[str] = None,
    dataset_identifier: Optional[str] = None,
    data_source_file_path: Optional[str] = None,
    data_source_file_paths: list[str] = [],
    variables: Optional[Dict[str, str]] = None,
    publish: bool = False,
    verbose: bool = False,
    blocking_timeout_in_minutes: int = 60,
) -> ContractVerificationSessionResult:
    """
    Verifies the contract on a Soda Runner (formerly Soda Agent).
    """
    return verify_contract(
        contract_file_path=contract_file_path,
        dataset_identifier=dataset_identifier,
        data_source_file_path=data_source_file_path,
        data_source_file_paths=data_source_file_paths,
        soda_cloud_file_path=soda_cloud_file_path,
        variables=variables,
        publish=publish,
        verbose=verbose,
        use_runner=True,
        blocking_timeout_in_minutes=blocking_timeout_in_minutes,
    )


def verify_contract_on_agent(*args, **kwargs) -> ContractVerificationSessionResult:
    """Deprecated alias for verify_contract_on_runner. Kept for backwards compatibility."""
    warn_deprecated("verify_contract_on_agent", "verify_contract_on_runner")
    return verify_contract_on_runner(*args, **kwargs)


def verify_contract(
    contract_file_path: Optional[str],
    dataset_identifier: Optional[str],
    data_source_file_path: Optional[str],
    soda_cloud_file_path: Optional[str],
    publish: bool,
    use_runner: Optional[bool] = None,
    variables: Optional[Dict[str, str]] = None,
    data_timestamp: Optional[str] = None,
    verbose: bool = False,
    blocking_timeout_in_minutes: int = 60,
    data_sources: Optional[list[DataSourceImpl]] = None,
    data_source_file_paths: Optional[list[str]] = None,
    check_paths: Optional[list[str]] = None,
    dwh_data_source_file_path: Optional[Union[str, DiagnosticsWarehouseFiles]] = None,
    check_selectors: Optional[list[CheckSelector]] = None,
    logs: Optional[Logs] = None,
    **kwargs,
) -> ContractVerificationSessionResult:
    use_runner = deprecated_kwarg(kwargs, "use_agent", "use_runner", use_runner)
    if kwargs:
        raise TypeError(f"Unexpected keyword arguments: {sorted(kwargs)}")
    if use_runner is None:
        use_runner = False
    if not data_source_file_paths:
        data_source_file_paths = []

    # Backward compatibility for single data source file path - append to the list of data source file paths
    if data_source_file_path and data_source_file_path not in data_source_file_paths:
        data_source_file_paths.append(data_source_file_path)

    # Backward compatibility for single dataset identifier - convert to list
    if dataset_identifier is not None:
        dataset_identifiers = [dataset_identifier]
    else:
        dataset_identifiers = None

    # TODO: change this when we fully deprecate a list of contract file paths
    # This is only here to make sure the rest of the code still works.
    if isinstance(contract_file_path, str):
        contract_file_paths = [contract_file_path]
    else:
        contract_file_paths = None

    # Failures propagate raw, without touching Cloud: the CLI failure boundary
    # (``scan.run_scan``) owns the single mark-scan-failed, and a
    # mark here would duplicate it (double backend scan-ended events).
    # Programmatic callers get the exception and own the reporting decision.
    soda_cloud_client: Optional[SodaCloud] = None
    if soda_cloud_file_path:
        soda_cloud_client = SodaCloud.from_config(soda_cloud_file_path, variables)

    # TODO: verify the path where connection is provided
    validate_verify_arguments(
        contract_file_paths,
        dataset_identifiers,
        data_source_file_paths,
        data_sources,
        publish,
        use_runner,
        soda_cloud_client,
    )

    contract_yaml_sources, fetch_error_results = _create_contract_yamls(
        contract_file_paths, dataset_identifiers, soda_cloud_client
    )

    # A contract that could not be fetched fails the run. An empty result has no errors, so the
    # CLI would exit 0 on it. This stops the whole run on any one failed fetch. That is only right
    # while a run has one dataset: verify_contract takes a single dataset_identifier, and the
    # public API entry points refuse more than one. A run over several datasets would have to
    # verify the ones it could fetch.
    if fetch_error_results:
        return ContractVerificationSessionResult(contract_verification_results=fetch_error_results)

    if len(contract_yaml_sources) == 0:
        soda_logger.debug("No contracts given. Exiting.")
        return ContractVerificationSessionResult(contract_verification_results=[])

    data_source_yaml_sources: list[DataSourceYamlSource] = []
    if data_source_file_paths:
        data_source_yaml_sources.extend(build_data_source_yaml_sources(data_source_file_paths, use_runner=use_runner))

    contract_verification_result = ContractVerificationSession.execute(
        contract_yaml_sources=contract_yaml_sources,
        data_source_yaml_sources=data_source_yaml_sources,
        data_source_impls=data_sources,
        soda_cloud_impl=soda_cloud_client,
        variables=variables,
        data_timestamp=data_timestamp,
        only_validate_without_execute=False,
        soda_cloud_publish_results=publish,
        soda_cloud_use_runner=use_runner,
        soda_cloud_verbose=verbose,
        soda_cloud_use_runner_blocking_timeout_in_minutes=blocking_timeout_in_minutes,
        check_paths=check_paths,
        check_selectors=check_selectors,
        dwh_data_source_file_path=dwh_data_source_file_path,
        logs=logs,
    )

    soda_telemetry.ingest_contract_verification_session_result(
        contract_verification_session_result=contract_verification_result
    )

    return contract_verification_result


def validate_verify_arguments(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    data_source_file_paths: Optional[list[str]],
    data_sources: Optional[list[DataSourceImpl]],
    publish: bool,
    use_runner: bool,
    soda_cloud_client: Optional[SodaCloud],
) -> None:
    if publish and not soda_cloud_client:
        raise InvalidArgumentException(
            "A Soda Cloud configuration file is required to use the -p/--publish argument. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )

    if use_runner and not soda_cloud_client:
        raise InvalidArgumentException(
            "A Soda Cloud configuration file is required to use the -r/--runner argument. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )

    if all_none_or_empty(contract_file_paths, dataset_identifiers):
        raise InvalidArgumentException(__AT_LEAST_ONE_CONTRACT_OR_DATASET_REQUIRED)

    if dataset_identifiers and not soda_cloud_client:
        raise InvalidArgumentException(
            "A Soda Cloud configuration file is required to use the -d/--dataset argument."
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )

    if not data_source_file_paths and not use_runner and not data_sources:
        raise InvalidArgumentException("At least one of -ds/--data-source or -d/--dataset value is required.")


def all_none_or_empty(*args: list | None) -> bool:
    return all(x is None or len(x) == 0 for x in args)


def is_using_remote_contract(
    contract_file_paths: Optional[list[str]], dataset_identifiers: Optional[list[str]]
) -> bool:
    return (contract_file_paths is None or len(contract_file_paths) == 0) and dataset_identifiers is not None


def contract_verification_is_not_sent_to_cloud(
    contract_verification_session_result: ContractVerificationSessionResult,
) -> bool:
    return any(
        cr.sending_results_to_soda_cloud_failed
        for cr in contract_verification_session_result.contract_verification_results
    )


def _create_contract_yamls(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_client: SodaCloud,
) -> tuple[list[ContractYamlSource], list[ContractVerificationResult]]:
    """Returns the contract YAML sources to verify, and an ERROR result for each dataset whose
    contract could not be fetched from Soda Cloud."""
    contract_yaml_sources: list[ContractYamlSource] = []
    fetch_error_results: list[ContractVerificationResult] = []

    if contract_file_paths:
        contract_yaml_sources += [ContractYamlSource.from_file_path(p) for p in contract_file_paths]

    if is_using_remote_contract(contract_file_paths, dataset_identifiers) and soda_cloud_client:
        for dataset_identifier in dataset_identifiers:
            try:
                contract: Optional[str] = soda_cloud_client.fetch_contract_for_dataset(dataset_identifier)
            except (SodaCloudException, SodaCloudAuthenticationFailedException) as exc:
                # A dataset query failure gives its reason without the dataset, so the line names
                # the dataset once. Any other Soda Cloud failure, such as a rejected API key at
                # login, falls back to its message.
                reason: str = exc.reason if isinstance(exc, DatasetQueryException) else str(exc)
                fetch_failure = ContractFetchFailedException(dataset_identifier, reason)
                fetch_failure.__cause__ = exc
                fetch_error_results.append(_build_fetch_error_result(fetch_failure))
                continue
            # Whitespace-only contents count as no contract: the YAML parser would reject them
            # with an error that does not name the dataset.
            if not contract or not contract.strip():
                fetch_error_results.append(
                    _build_fetch_error_result(
                        ContractFetchFailedException(dataset_identifier, "Soda Cloud returned no contract")
                    )
                )
                continue
            contract_yaml_sources.append(ContractYamlSource.from_str(contract))

    return contract_yaml_sources, fetch_error_results


def _build_fetch_error_result(fetch_failure: ContractFetchFailedException) -> ContractVerificationResult:
    """An ERROR result for a dataset whose contract could not be fetched from Soda Cloud.

    Nothing was verified, so it has no checks and nothing is sent to Soda Cloud. The ERROR status
    makes the session result report errors, and the log records carry the message.
    """
    now = datetime.now(tz=timezone.utc)
    # Captured into the result's own Logs, the way build_error_result does for a check
    # collection that fails before producing output, so the error travels with the result.
    # The console still shows it.
    with preserve_active_logs():
        error_logs = Logs()
        soda_logger.error(str(fetch_failure))
    return ContractVerificationResult(
        check_collection=Contract(
            data_source_name=None,
            dataset_prefix=[],
            dataset_name="",
            soda_qualified_dataset_name=fetch_failure.dataset_identifier,
            source=YamlFileContentInfo(source_content_str=None, local_file_path=None),
        ),
        data_source=None,
        data_timestamp=None,
        started_timestamp=now,
        ended_timestamp=now,
        status=CheckCollectionStatus.ERROR,
        measurements=[],
        check_results=[],
        sending_results_to_soda_cloud_failed=False,
        log_records=error_logs.get_log_records(),
        post_processing_stages=[],
        error=fetch_failure,
    )


def __attempt_pick_first_element(my_list: Optional[list[str]]) -> Optional[str]:
    if my_list is None:
        return None
    if isinstance(my_list, list) and len(my_list) >= 1:
        return my_list[0]
    raise InvalidArgumentException(
        "Expected a list with at least one element, got an empty list or another type of object."
    )
