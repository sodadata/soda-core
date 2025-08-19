import os

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.soda_cloud import SodaCloud
from soda_core.contracts.contract_request import (
    RequestStatus,
    fetch_contract_proposal,
    push_contract_proposal,
    transition_contract_request,
)


def handle_fetch_proposal(
    soda_cloud_file_path: str,
    request_number: int,
    output_file_path: str,
    proposal_number: int | None = None,
) -> ExitCode:
    try:
        soda_cloud_client = SodaCloud.from_config(soda_cloud_file_path)
        proposal = fetch_contract_proposal(soda_cloud_client, request_number, proposal_number)
        directory = os.path.dirname(output_file_path)
        if directory:
            os.makedirs(directory, exist_ok=True)
        with open(output_file_path, "w") as file:
            file.write(proposal.contents)
        soda_logger.info(
            f"{Emoticons.WHITE_CHECK_MARK} Successfully fetched proposal {proposal.proposal_number} for request {proposal.request_number} and written it to file {output_file_path}."
        )
        return ExitCode.OK
    except Exception as exc:
        soda_logger.exception(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS


def handle_push_proposal(
    soda_cloud_file_path: str,
    file_path: str,
    request_number: int,
    message: str | None = None,
) -> ExitCode:
    try:
        soda_cloud_client = SodaCloud.from_config(soda_cloud_file_path)
        with open(file_path) as file:
            contents = file.read()
        proposal = push_contract_proposal(soda_cloud_client, request_number, contents, message)
        comment_msg = f" with comment: {message}" if message else ""
        soda_logger.info(
            f"{Emoticons.WHITE_CHECK_MARK} Successfully pushed proposal {proposal.proposal_number}{comment_msg} for request {request_number} to Soda Cloud."
        )
        return ExitCode.OK
    except Exception as exc:
        soda_logger.exception(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS


def handle_transition_request(
    soda_cloud_file_path: str,
    request_number: int,
    status: RequestStatus,
) -> ExitCode:
    try:
        soda_cloud_client = SodaCloud.from_config(soda_cloud_file_path)
        request = transition_contract_request(soda_cloud_client, request_number, status)
        soda_logger.info(
            f"{Emoticons.WHITE_CHECK_MARK} Successfully transitioned request {request.request_number} from {request.status.value} to {status.value} on Soda Cloud."
        )
        return ExitCode.OK
    except Exception as exc:
        soda_logger.exception(f"An unexpected exception occurred: {exc}")
        return ExitCode.LOG_ERRORS
