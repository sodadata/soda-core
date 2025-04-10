from typing import Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import (
    ContractYamlSource,
    DataSourceYamlSource,
    SodaCloudYamlSource,
)
from soda_core.contracts.contract_publication import ContractPublication
from soda_core.contracts.contract_verification import (
    ContractVerificationSession,
    ContractVerificationSessionResult,
)


def handle_verify_contract(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    data_source_file_path: Optional[str],
    soda_cloud_file_path: Optional[str],
    publish: bool,
    use_agent: bool,
    blocking_timeout_in_minutes: int,
) -> ExitCode:
    if exitcode := validate_verify_arguments(
        contract_file_paths, dataset_identifiers, data_source_file_path, soda_cloud_file_path, publish, use_agent
    ):
        return exitcode

    soda_cloud_client: Optional[SodaCloud] = None
    if soda_cloud_file_path:
        soda_cloud_client = SodaCloud.from_yaml_source(
            SodaCloudYamlSource.from_file_path(soda_cloud_file_path), variables=None
        )

    contract_yaml_sources = _create_contract_yamls(contract_file_paths, dataset_identifiers, soda_cloud_client)
    if len(contract_yaml_sources) == 0:
        soda_logger.debug("No contracts given. Exiting.")
        return ExitCode.OK

    # TODO: decide on implications of this
    if dataset_identifiers and len(dataset_identifiers) > 1:
        soda_logger.error(
            f"{Emoticons.EXPLODING_HEAD} We currently only support a single data source configuration. "
            f"Please pass a single dataset identifier."
        )
        return ExitCode.LOG_ERRORS
    data_source_yaml_source = _create_datasource_yamls(data_source_file_path, dataset_identifiers, soda_cloud_client)
    if not data_source_yaml_source:
        return ExitCode.LOG_ERRORS

    # TODO: pass the Soda Cloud client directly into subsequent methods
    contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
        contract_yaml_sources=contract_yaml_sources,
        data_source_yaml_sources=[data_source_yaml_source],
        soda_cloud_yaml_source=(
            SodaCloudYamlSource.from_file_path(soda_cloud_file_path) if soda_cloud_file_path else None
        ),
        soda_cloud_publish_results=publish,
        soda_cloud_use_agent=use_agent,
        soda_cloud_use_agent_blocking_timeout_in_minutes=blocking_timeout_in_minutes,
    )

    return interpret_contract_verification_result(contract_verification_session_result)


def _create_contract_yamls(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_client: SodaCloud,
) -> list[ContractYamlSource]:
    contract_yaml_sources: list[ContractYamlSource] = []

    contract_yaml_sources += [
        ContractYamlSource.from_file_path(contract_file_path) for contract_file_path in contract_file_paths or []
    ]

    if is_using_remote_contract(contract_file_paths, dataset_identifiers) and soda_cloud_client:
        for dataset_identifier in dataset_identifiers:
            contract = soda_cloud_client.fetch_contract_for_dataset(dataset_identifier)
            if not contract:
                soda_logger.error(f"Could not fetch contract for dataset '{dataset_identifier}': skipping verification")
                continue
            contract_yaml_sources.append(ContractYamlSource.from_str(contract))

    return contract_yaml_sources


def _create_datasource_yamls(
    data_source_file_path: Optional[str],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_client: SodaCloud,
) -> Optional[DataSourceYamlSource]:
    data_source_yaml_source: Optional[DataSourceYamlSource] = None

    if data_source_file_path:
        data_source_yaml_source = DataSourceYamlSource.from_file_path(data_source_file_path)

    if is_using_remote_datasource(dataset_identifiers, data_source_file_path) and soda_cloud_client:
        dataset_identifier = dataset_identifiers[0]

        soda_logger.debug("No local data source config, trying to fetch data source config from cloud")
        data_source_config = soda_cloud_client.fetch_data_source_configuration_for_dataset(dataset_identifier)
        data_source_yaml_source = DataSourceYamlSource.from_str(data_source_config) if data_source_config else None

    return data_source_yaml_source


def validate_verify_arguments(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    data_source_file_path: Optional[str],
    soda_cloud_file_path: Optional[str],
    publish: bool,
    use_agent: bool,
) -> Optional[ExitCode]:
    if publish and not soda_cloud_file_path:
        soda_logger.error(
            "A Soda Cloud configuration file is required to use the -p/--publish argument. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )
        return ExitCode.LOG_ERRORS

    if use_agent and not soda_cloud_file_path:
        soda_logger.error(
            "A Soda Cloud configuration file is required to use the -a/--agent argument. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )
        return ExitCode.LOG_ERRORS

    if all_none_or_empty(contract_file_paths, dataset_identifiers):
        soda_logger.error("At least one of -c/--contract or -d/--dataset arguments is required.")
        return ExitCode.LOG_ERRORS

    if dataset_identifiers and not soda_cloud_file_path:
        soda_logger.error(
            "A Soda Cloud configuration file is required to use the -d/--dataset argument."
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )
        return ExitCode.LOG_ERRORS

    if all_none_or_empty(dataset_identifiers) and not data_source_file_path:
        soda_logger.error("At least one of -ds/--data-source or -d/--dataset value is required.")
        return ExitCode.LOG_ERRORS

    return None


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


def interpret_contract_verification_result(verification_result: ContractVerificationSessionResult) -> ExitCode:
    if contract_verification_is_not_sent_to_cloud(verification_result):
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    elif verification_result.has_errors():
        return ExitCode.LOG_ERRORS
    elif verification_result.is_failed():
        return ExitCode.CHECK_FAILURES
    else:
        return ExitCode.OK


def handle_publish_contract(
    contract_file_paths: Optional[list[str]],
    soda_cloud_file_path: Optional[str],
) -> ExitCode:
    contract_publication_builder = ContractPublication.builder()

    for contract_file_path in contract_file_paths:
        contract_publication_builder.with_contract_yaml_file(contract_file_path)

    if soda_cloud_file_path:
        contract_publication_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)

    contract_publication_result = contract_publication_builder.build().execute()
    if contract_publication_result.has_errors():
        # TODO: detect/deal with exit code 4?
        return ExitCode.LOG_ERRORS
    else:
        return ExitCode.OK


def handle_test_contract(
    contract_file_paths: Optional[list[str]],
    data_source_file_path: Optional[str],
) -> ExitCode:
    for contract_file_path in contract_file_paths:
        contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_file_path(contract_file_path)],
            only_validate_without_execute=True,
        )
        if contract_verification_session_result.has_errors():
            return ExitCode.LOG_ERRORS
        else:
            soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} {contract_file_path} is valid")
            return ExitCode.OK

    return ExitCode.OK
