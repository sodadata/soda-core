from typing import Dict, Optional, Union

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.exceptions import (
    InvalidArgumentException,
    InvalidDataSourceConfigurationException,
    SodaCloudException,
)
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    ContractVerificationSessionResult,
)
from soda_core.telemetry.soda_telemetry import SodaTelemetry

soda_telemetry = SodaTelemetry()


def verify_contracts_locally(
    data_source_file_path: Optional[str] = None,
    data_sources: Optional[Union[list[DataSourceImpl], DataSourceImpl]] = [],
    contract_file_paths: Optional[Union[str, list[str]]] = None,
    dataset_identifiers: Optional[list[str]] = None,
    soda_cloud_file_path: Optional[str] = None,
    variables: Optional[Dict[str, str]] = None,
    data_timestamp: Optional[str] = None,
    publish: bool = False,
) -> ContractVerificationSessionResult:
    """
    Verifies the contract locally.
    """
    if not isinstance(data_sources, list):
        data_sources = [data_sources]

    return verify_contracts(
        contract_file_paths=contract_file_paths,
        dataset_identifiers=dataset_identifiers,
        data_source_file_path=data_source_file_path,
        data_sources=data_sources,
        soda_cloud_file_path=soda_cloud_file_path,
        variables=variables,
        data_timestamp=data_timestamp,
        publish=publish,
        use_agent=False,
    )


def verify_contracts_on_agent(
    soda_cloud_file_path: str,
    contract_file_paths: Optional[Union[str, list[str]]] = None,
    dataset_identifiers: Optional[list[str]] = None,
    data_source_file_path: Optional[str] = None,
    variables: Optional[Dict[str, str]] = None,
    publish: bool = False,
    verbose: bool = False,
    blocking_timeout_in_minutes: int = 60,
) -> ContractVerificationSessionResult:
    """
    Verifies the contract on an agent.
    """
    return verify_contracts(
        contract_file_paths=contract_file_paths,
        dataset_identifiers=dataset_identifiers,
        data_source_file_path=data_source_file_path,
        soda_cloud_file_path=soda_cloud_file_path,
        variables=variables,
        publish=publish,
        verbose=verbose,
        use_agent=True,
        blocking_timeout_in_minutes=blocking_timeout_in_minutes,
    )


def verify_contracts(
    contract_file_paths: Optional[Union[str, list[str]]],
    dataset_identifiers: Optional[list[str]],
    data_source_file_path: Optional[str],
    soda_cloud_file_path: Optional[str],
    publish: bool,
    use_agent: bool,
    variables: Optional[Dict[str, str]] = None,
    data_timestamp: Optional[str] = None,
    verbose: bool = False,
    blocking_timeout_in_minutes: int = 60,
    data_sources: Optional[list[DataSourceImpl]] = None,
) -> ContractVerificationSessionResult:
    soda_cloud_client: Optional[SodaCloud] = None
    try:
        if soda_cloud_file_path:
            soda_cloud_client = SodaCloud.from_config(soda_cloud_file_path, variables)

        if isinstance(contract_file_paths, str):
            contract_file_paths = [contract_file_paths]

        # TODO: verify the path where connection is provided
        validate_verify_arguments(
            contract_file_paths,
            dataset_identifiers,
            data_source_file_path,
            data_sources,
            publish,
            use_agent,
            soda_cloud_client,
        )

        contract_yaml_sources = _create_contract_yamls(contract_file_paths, dataset_identifiers, soda_cloud_client)

        if len(contract_yaml_sources) == 0:
            soda_logger.debug("No contracts given. Exiting.")
            return ContractVerificationSessionResult(contract_verification_results=[])

        # TODO: decide on implications of this
        if dataset_identifiers and len(dataset_identifiers) > 1:
            raise InvalidArgumentException(
                "We currently only support a single data source configuration. Please pass a single dataset identifier."
            )

        data_source_yaml_sources: list[DataSourceYamlSource] = []
        if data_source_file_path or dataset_identifiers:
            data_source_yaml_sources.append(
                _create_datasource_yamls(
                    data_source_file_path,
                    dataset_identifiers,
                    soda_cloud_client,
                    use_agent,
                )
            )

        contract_verification_result = ContractVerificationSession.execute(
            contract_yaml_sources=contract_yaml_sources,
            data_source_yaml_sources=data_source_yaml_sources,
            data_source_impls=data_sources,
            soda_cloud_impl=soda_cloud_client,
            variables=variables,
            data_timestamp=data_timestamp,
            only_validate_without_execute=False,
            soda_cloud_publish_results=publish,
            soda_cloud_use_agent=use_agent,
            soda_cloud_verbose=verbose,
            soda_cloud_use_agent_blocking_timeout_in_minutes=blocking_timeout_in_minutes,
        )

        soda_telemetry.ingest_contract_verification_session_result(
            contract_verification_session_result=contract_verification_result
        )

        return contract_verification_result
    except Exception as exc:
        soda_logger.error(exc)
        if soda_cloud_client:
            soda_cloud_client.mark_scan_as_failed(exc=exc)
        raise exc


def validate_verify_arguments(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    data_source_file_path: Optional[str],
    data_sources: Optional[list[DataSourceImpl]],
    publish: bool,
    use_agent: bool,
    soda_cloud_client: Optional[SodaCloud],
) -> None:
    if publish and not soda_cloud_client:
        raise InvalidArgumentException(
            "A Soda Cloud configuration file is required to use the -p/--publish argument. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )

    if use_agent and not soda_cloud_client:
        raise InvalidArgumentException(
            "A Soda Cloud configuration file is required to use the -a/--agent argument. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )

    if all_none_or_empty(contract_file_paths, dataset_identifiers):
        raise InvalidArgumentException("At least one of -c/--contract or -d/--dataset arguments is required.")

    if dataset_identifiers and not soda_cloud_client:
        raise InvalidArgumentException(
            "A Soda Cloud configuration file is required to use the -d/--dataset argument."
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )

    if all_none_or_empty(dataset_identifiers) and not data_source_file_path and not use_agent and not data_sources:
        raise InvalidArgumentException("At least one of -ds/--data-source or -d/--dataset value is required.")


def all_none_or_empty(*args: list | None) -> bool:
    return all(x is None or len(x) == 0 for x in args)


def is_using_remote_contract(
    contract_file_paths: Optional[list[str]], dataset_identifiers: Optional[list[str]]
) -> bool:
    return (contract_file_paths is None or len(contract_file_paths) == 0) and dataset_identifiers is not None


def is_using_remote_datasource(dataset_identifiers: Optional[list[str]], data_source_file_path: Optional[str]) -> bool:
    return not data_source_file_path and not all_none_or_empty(dataset_identifiers)


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
) -> list[ContractYamlSource]:
    contract_yaml_sources = []

    if contract_file_paths:
        contract_yaml_sources += [ContractYamlSource.from_file_path(p) for p in contract_file_paths]

    if is_using_remote_contract(contract_file_paths, dataset_identifiers) and soda_cloud_client:
        for dataset_identifier in dataset_identifiers:
            try:
                contract = soda_cloud_client.fetch_contract_for_dataset(dataset_identifier)
                contract_yaml_sources.append(ContractYamlSource.from_str(contract))
            except SodaCloudException as exc:
                soda_logger.error(f"Could not fetch contract for dataset '{dataset_identifier}': skipping verification")

    if not contract_yaml_sources:
        return []

    return contract_yaml_sources


def _create_datasource_yamls(
    data_source_file_path: Optional[str],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_client: SodaCloud,
    use_agent: bool,
) -> Optional[DataSourceYamlSource]:
    if data_source_file_path:
        return DataSourceYamlSource.from_file_path(data_source_file_path)

    if is_using_remote_datasource(dataset_identifiers, data_source_file_path) and soda_cloud_client:
        if len(dataset_identifiers) > 1:
            raise InvalidDataSourceConfigurationException(
                f"{Emoticons.EXPLODING_HEAD} We currently only support a single data source configuration. "
                f"Please pass a single dataset identifier."
            )

        dataset_identifier = dataset_identifiers[0]
        soda_logger.debug("No local data source config, trying to fetch data source config from cloud")
        data_source_config = soda_cloud_client.fetch_data_source_configuration_for_dataset(dataset_identifier)

        return DataSourceYamlSource.from_str(data_source_config)

    # By this point, we can only progress if we are using an agent.
    # Then the agent will provide the data source config.
    if use_agent:
        return None

    raise InvalidDataSourceConfigurationException(
        "No data source configuration provided and no dataset identifiers given. "
        "Please provide a data source configuration file or a dataset identifier."
    )
