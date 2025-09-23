from os.path import dirname, exists
from pathlib import Path
from typing import Dict, Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.exceptions import (
    ContractParserException,
    InvalidArgumentException,
    InvalidDataSourceConfigurationException,
)
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api import test_contracts, verify_contracts
from soda_core.contracts.contract_publication import ContractPublication
from soda_core.contracts.contract_verification import ContractVerificationSessionResult


def handle_verify_contract(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    data_source_file_paths: Optional[str],
    soda_cloud_file_path: Optional[str],
    variables: Optional[Dict[str, str]],
    publish: bool,
    verbose: bool,
    use_agent: bool,
    blocking_timeout_in_minutes: int,
    diagnostics_warehouse_file_path: Optional[str],
) -> ExitCode:
    try:
        contract_verification_result = verify_contracts(
            contract_file_paths=contract_file_paths,
            dataset_identifiers=dataset_identifiers,
            data_source_file_path=None,
            data_source_file_paths=data_source_file_paths,
            soda_cloud_file_path=soda_cloud_file_path,
            variables=variables,
            publish=publish,
            verbose=verbose,
            use_agent=use_agent,
            blocking_timeout_in_minutes=blocking_timeout_in_minutes,
            dwh_data_source_file_path=diagnostics_warehouse_file_path,
        )

        return interpret_contract_verification_result(contract_verification_result)

    except (InvalidArgumentException, InvalidDataSourceConfigurationException, Exception) as exc:
        soda_logger.exception(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS


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
    elif verification_result.has_errors:
        return ExitCode.LOG_ERRORS
    elif verification_result.is_failed:
        return ExitCode.CHECK_FAILURES
    elif verification_result.is_warned:
        return ExitCode.CHECK_WARNINGS
    else:
        return ExitCode.OK


def handle_publish_contract(
    contract_file_paths: Optional[list[str]],
    soda_cloud_file_path: Optional[str],
) -> ExitCode:
    try:
        contract_publication_builder = ContractPublication.builder()

        for contract_file_path in contract_file_paths:
            contract_publication_builder.with_contract_yaml_file(contract_file_path)

        if soda_cloud_file_path:
            contract_publication_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)

        contract_publication_result = contract_publication_builder.build().execute()
        if contract_publication_result.has_errors:
            # TODO: detect/deal with exit code 4?
            return ExitCode.LOG_ERRORS
        else:
            return ExitCode.OK
    except ContractParserException as exc:
        soda_logger.error(f"Failed to parse contract: {exc}")
        return ExitCode.LOG_ERRORS
    except Exception as exc:
        soda_logger.exception(f"Failed to publish contract: {exc}")
        return ExitCode.LOG_ERRORS


def handle_test_contract(
    contract_file_paths: Optional[list[str]],
    variables: Optional[Dict[str, str]],
) -> ExitCode:
    for contract_file_path in contract_file_paths:
        contract_verification_result = test_contracts(contract_file_paths=[contract_file_path], variables=variables)
        if contract_verification_result.has_errors:
            return ExitCode.LOG_ERRORS
        else:
            soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} {contract_file_path} is valid")
            return ExitCode.OK

    return ExitCode.OK


def handle_fetch_contract(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_file_path: Optional[str],
) -> ExitCode:
    from soda_core.common.soda_cloud import SodaCloud

    soda_cloud_client: Optional[SodaCloud] = None
    try:
        if soda_cloud_file_path:
            soda_cloud_client = SodaCloud.from_config(soda_cloud_file_path)

        validate_fetch_arguments(contract_file_paths, dataset_identifiers, soda_cloud_client)

        # fetch contract YAML strings
        contract_yaml_sources = []
        if soda_cloud_client:
            for dataset_identifier in dataset_identifiers:
                contract = soda_cloud_client.fetch_contract_for_dataset(dataset_identifier)
                if not contract:
                    soda_logger.error(f"Could not fetch contract for dataset '{dataset_identifier}': skipping fetch")
                    continue
                contract_yaml_sources.append(ContractYamlSource.from_str(contract))
        if not contract_yaml_sources or len(contract_yaml_sources) == 0:
            soda_logger.debug("No contracts given. Exiting.")
            return ExitCode.OK

        # write to local contract YAML files
        for contract_file_path, contract_yaml in zip(contract_file_paths, contract_yaml_sources):
            dir_name = dirname(contract_file_path)
            try:
                Path(dir_name).mkdir(parents=True, exist_ok=True)
                if exists(contract_file_path):
                    action = "Updated"
                else:
                    action = "Created"
                with open(contract_file_path, "w") as contract_file:
                    contract_file.write(contract_yaml.yaml_str)
                soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK}  {action} contract source file '{contract_file_path}'")
                return ExitCode.OK
            except Exception as exc:
                soda_logger.error(f"An unexpected exception occurred: {exc}")
                return ExitCode.LOG_ERRORS

    except (InvalidArgumentException, Exception) as exc:
        soda_logger.exception(f"Unexpected error during contract fetching: {exc}")
        return ExitCode.LOG_ERRORS


def validate_fetch_arguments(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    soda_cloud_file_path: Optional[str],
) -> None:
    if not contract_file_paths:
        raise InvalidArgumentException(
            "A Soda Data Contract file path is required to use the fetch command. "
            "Please provide the '-f/--file' argument with a valid contract file path."
        )
    if not dataset_identifiers:
        raise InvalidArgumentException(
            "A Soda Cloud dataset identifier is required to use the fetch command. "
            "Please provide the '-d/--dataset' argument with a valid dataset identifier."
        )
    if not soda_cloud_file_path:
        raise InvalidArgumentException(
            "A Soda Cloud configuration file is required to use the fetch command. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path."
        )
    if len(contract_file_paths) != len(dataset_identifiers):
        raise InvalidArgumentException(
            "The number of contract file paths must match the number of dataset identifiers. "
            "Ensure that each contract corresponds to a specific dataset."
        )
