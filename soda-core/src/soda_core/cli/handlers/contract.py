from typing import Dict, List, Optional, Tuple

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
    variables: Optional[List[str]],
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

    contract_yaml_sources, exit_code = _create_contract_yamls(
        contract_file_paths, dataset_identifiers, soda_cloud_client
    )
    if exit_code:
        return exit_code

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
    data_source_yaml_source, exit_code = _create_datasource_yamls(
        data_source_file_path, dataset_identifiers, soda_cloud_client
    )
    if exit_code:
        return exit_code

    parsed_variables = _parse_variables(variables)
    if parsed_variables is None:
        return ExitCode.LOG_ERRORS

    # TODO: pass the Soda Cloud client directly into subsequent methods
    contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
        contract_yaml_sources=contract_yaml_sources,
        data_source_yaml_sources=[data_source_yaml_source],
        soda_cloud_yaml_source=(
            SodaCloudYamlSource.from_file_path(soda_cloud_file_path) if soda_cloud_file_path else None
        ),
        variables=parsed_variables,
        soda_cloud_publish_results=publish,
        soda_cloud_use_agent=use_agent,
        soda_cloud_use_agent_blocking_timeout_in_minutes=blocking_timeout_in_minutes,
    )

    return interpret_contract_verification_result(contract_verification_session_result)


def _parse_variables(variables: Optional[List[str]]) -> Optional[Dict[str, str]]:
    if not variables:
        return {}

    result = {}
    for variable in variables:
        for nested_variable in variable.split(","):
            nested_variable = nested_variable.strip()
            if not nested_variable:
                continue
            parsed_variable = _parse_variable(nested_variable)
            if parsed_variable is None:
                return None
            result[parsed_variable[0]] = parsed_variable[1]
    return result


def _parse_variable(variable: str) -> Optional[Tuple[str, str]]:
    if "=" not in variable:
        soda_logger.error(f"Variable {variable} is incorrectly formatted. Please use the format KEY=VALUE")
        return None
    key, value = variable.split("=", 1)
    key = key.strip()
    value = value.strip()
    if not key or not value:
        soda_logger.error(
            f"Incorrectly formatted variable '{variable}', key or value is empty. Please use the format KEY=VALUE"
        )
        return None
    return key, value


def _create_contract_yamls(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_client: SodaCloud,
) -> tuple[list[ContractYamlSource], ExitCode | None]:
    contract_yaml_sources = []

    if contract_file_paths:
        contract_yaml_sources += [ContractYamlSource.from_file_path(p) for p in contract_file_paths]

    if is_using_remote_contract(contract_file_paths, dataset_identifiers) and soda_cloud_client:
        for dataset_identifier in dataset_identifiers:
            contract = soda_cloud_client.fetch_contract_for_dataset(dataset_identifier)
            if not contract:
                soda_logger.error(f"Could not fetch contract for dataset '{dataset_identifier}': skipping verification")
                continue
            contract_yaml_sources.append(ContractYamlSource.from_str(contract))

    if not contract_yaml_sources:
        soda_logger.debug("No contracts given. Exiting.")
        return [], ExitCode.OK

    return contract_yaml_sources, None


def _create_datasource_yamls(
    data_source_file_path: Optional[str],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_client: SodaCloud,
) -> tuple[Optional[DataSourceYamlSource], ExitCode | None]:
    if data_source_file_path:
        return DataSourceYamlSource.from_file_path(data_source_file_path), None

    if is_using_remote_datasource(dataset_identifiers, data_source_file_path) and soda_cloud_client:
        if len(dataset_identifiers) > 1:
            soda_logger.error(
                f"{Emoticons.EXPLODING_HEAD} We currently only support a single data source configuration. "
                f"Please pass a single dataset identifier."
            )
            return None, ExitCode.LOG_ERRORS

        dataset_identifier = dataset_identifiers[0]
        soda_logger.debug("No local data source config, trying to fetch data source config from cloud")
        data_source_config = soda_cloud_client.fetch_data_source_configuration_for_dataset(dataset_identifier)

        if not data_source_config:
            return None, ExitCode.LOG_ERRORS

        return DataSourceYamlSource.from_str(data_source_config), None

    return None, None


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

    if all_none_or_empty(dataset_identifiers) and not data_source_file_path and not use_agent:
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
    variables: Optional[List[str]],
    data_source_file_path: Optional[str],
) -> ExitCode:
    parsed_variables = _parse_variables(variables)
    if parsed_variables is None:
        return ExitCode.LOG_ERRORS

    for contract_file_path in contract_file_paths:
        contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_file_path(contract_file_path)],
            only_validate_without_execute=True,
            variables=parsed_variables,
        )
        if contract_verification_session_result.has_errors():
            return ExitCode.LOG_ERRORS
        else:
            soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} {contract_file_path} is valid")
            return ExitCode.OK

    return ExitCode.OK
