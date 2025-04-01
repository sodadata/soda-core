from soda_core.cli.exit_codes import ExitCode
from soda_core.common.yaml import YamlSource, YamlFileContent
from soda_core.contracts.contract_publication import ContractPublication
from soda_core.contracts.contract_verification import (
    ContractVerificationSessionResult,
    ContractVerificationSession,
)
from soda_core.common.logging_constants import soda_logger, Emoticons
from soda_core.common.soda_cloud import SodaCloud
from typing import Optional, Dict


def handle_verify_contract(
    contract_file_paths: Optional[list[str]],
    dataset_identifiers: Optional[list[str]],
    data_source_file_path: Optional[str],
    soda_cloud_file_path: Optional[str],
    publish: bool,
    use_agent: bool,
    blocking_timeout_in_minutes: int,
) -> ExitCode:
    if all_none_or_empty(contract_file_paths, dataset_identifiers):
        soda_logger.error("At least one of -c/--contract or -d/--dataset value is required.")
        return ExitCode.LOG_ERRORS

    if dataset_identifiers and not soda_cloud_file_path:
        soda_logger.error("A Soda Cloud configuration is required to use the -d/--dataset switch.")
        return ExitCode.LOG_ERRORS

    contract_yaml_sources: list[YamlSource] = []
    contract_yaml_sources += [
            YamlSource.from_file_path(contract_file_path) for contract_file_path in contract_file_paths
        ]

    if soda_cloud_file_path:
        soda_cloud_client = SodaCloud.from_file(
            YamlSource.from_file_path(soda_cloud_file_path).parse_yaml_file_content(
                file_type="soda cloud", variables=None)
        )

        if is_using_remote_contract(dataset_identifiers):
            contract_yaml_sources += [
                YamlSource.from_str(soda_cloud_client.fetch_contract_for_dataset(dataset_identifier)) for
                dataset_identifier in dataset_identifiers
            ]

    # TODO: pass the Soda Cloud client directly into subsequent methods
    contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
        contract_yaml_sources=contract_yaml_sources,
        data_source_yaml_sources=(
            [YamlSource.from_file_path(data_source_file_path)] if data_source_file_path else []
        ),
        soda_cloud_yaml_source=(YamlSource.from_file_path(soda_cloud_file_path) if soda_cloud_file_path else None),
        soda_cloud_publish_results=publish,
        soda_cloud_use_agent=use_agent,
        soda_cloud_use_agent_blocking_timeout_in_minutes=blocking_timeout_in_minutes,
    )

    return interpret_contract_verification_result(contract_verification_session_result)


def all_none_or_empty(*args: list | None) -> bool:
    return all(x is None or len(x) == 0 for x in args)


def is_using_remote_contract(dataset_identifiers: Optional[list[str]]) -> bool:
    return dataset_identifiers is not None


def contract_verification_is_not_sent_to_cloud(
    contract_verification_session_result: ContractVerificationSessionResult
) -> bool:
    return any(
        cr.sending_results_to_soda_cloud_failed for cr in contract_verification_session_result.contract_verification_results
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
            contract_yaml_sources=[YamlSource.from_file_path(contract_file_path)],
            only_validate_without_execute=True,
        )
        if contract_verification_session_result.has_errors():
            return ExitCode.LOG_ERRORS
        else:
            soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} {contract_file_path} is valid")
            return ExitCode.OK

    return ExitCode.OK

