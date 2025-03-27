from soda_core.cli.exit_codes import ExitCode
from soda_core.contracts.contract_publication import ContractPublication
from soda_core.contracts.contract_verification import (
    ContractVerificationSessionResult,
    ContractVerificationSessionBuilder,
    ContractVerificationSession,
)
from soda_core.common.logging_constants import soda_logger, Emoticons
from typing import Optional


def handle_verify_contract(
    contract_file_paths: Optional[list[str]],
    data_source_file_path: Optional[str],
    soda_cloud_file_path: Optional[str],
    skip_publish: bool,
    use_agent: bool,
    blocking_timeout_in_minutes: int,
) -> ExitCode:
    contract_verification_session_builder: ContractVerificationSessionBuilder = (
        ContractVerificationSession.builder()
    )

    for contract_file_path in contract_file_paths:
        contract_verification_session_builder.with_contract_yaml_file(contract_file_path)

    if data_source_file_path:
        contract_verification_session_builder.with_data_source_yaml_file(data_source_file_path)

    if use_agent:
        contract_verification_session_builder.with_execution_on_soda_agent(
            blocking_timeout_in_minutes=blocking_timeout_in_minutes
        )

    if soda_cloud_file_path:
        contract_verification_session_builder.with_soda_cloud_yaml_file(soda_cloud_file_path)

    if skip_publish:
        contract_verification_session_builder.with_soda_cloud_skip_publish()

    contract_verification_session_result: ContractVerificationSessionResult = (
        contract_verification_session_builder.execute()
    )

    return interpret_contract_verification_result(contract_verification_session_result)


def contract_verification_is_not_sent_to_cloud(
    contract_verification_session_result: ContractVerificationSessionResult
) -> bool:
    return any(
        cr.sending_results_to_soda_cloud_failed for cr in contract_verification_session_result.contract_results
    )


def interpret_contract_verification_result(verification_result: ContractVerificationSessionResult) -> ExitCode:
    if contract_verification_is_not_sent_to_cloud(verification_result):
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    elif verification_result.has_errors():
        return ExitCode.LOG_ERRORS
    elif verification_result.has_failures():
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
    contract_verification_session_builder: ContractVerificationSessionBuilder = (
        ContractVerificationSession.builder()
    )

    for contract_file_path in contract_file_paths:
        soda_logger.info(f"Testing contract '{contract_file_path}' YAML syntax")
        contract_verification_session_builder.with_contract_yaml_file(contract_file_path)

    contract_verification_session_builder.with_data_source_yaml_file(data_source_file_path)

    contract_verification_session_builder.build()
    if contract_verification_session_builder.logs.has_errors():
        return ExitCode.LOG_ERRORS
    else:
        soda_logger.info(f"{Emoticons.WHITE_CHECK_MARK} All provided contracts are valid")
        return ExitCode.OK


