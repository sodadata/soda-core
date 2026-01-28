from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from soda_core.common.exceptions import SodaCloudException
from soda_core.common.soda_cloud import SodaCloud


class RequestStatus(str, Enum):
    OPEN = "open"
    WONT_DO = "wontDo"
    DONE = "done"


class ContractProposal(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)
    request_number: int
    proposal_number: int
    contents: str
    comment: str | None = None


class ContractRequest(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)
    request_number: int
    title: str
    created: datetime
    last_updated: datetime
    status: RequestStatus


def fetch_contract_proposal(
    soda_cloud: SodaCloud,
    request_number: int,
    proposal_number: int | None = None,
) -> ContractProposal:
    query = {
        "type": "sodaCoreFetchContractRequestProposal",
        "contractRequestNumber": request_number,
        "proposalNumber": proposal_number,
    }
    response = soda_cloud._execute_query(query, request_log_name="fetch_contract_proposal")
    response_json = response.json()

    if response.status_code != 200:
        error_details = response_json.get("message", response.text)
        raise SodaCloudException(f"Failed to fetch contract proposal: {error_details}")

    return ContractProposal.model_validate(response_json)


def push_contract_proposal(
    soda_cloud: SodaCloud,
    request_number: int,
    contents: str,
    message: str | None = None,
) -> ContractProposal:
    command = {
        "type": "sodaCorePushContractRequestProposal",
        "contractRequestNumber": request_number,
        "proposalContents": contents,
        "message": message,
    }
    response = soda_cloud._execute_command(command, request_log_name="push_contract_proposal")
    response_json = response.json()

    if response.status_code != 200:
        error_details = response_json.get("message", response.text)
        raise SodaCloudException(f"Failed to push contract proposal: {error_details}")

    return ContractProposal.model_validate(response_json)


def transition_contract_request(soda_cloud: SodaCloud, request_number: int, status: RequestStatus) -> ContractRequest:
    command = {
        "type": "sodaCoreTransitionContractRequest",
        "contractRequestNumber": request_number,
        "status": status.value,
    }
    response = soda_cloud._execute_command(command, request_log_name="transition_contract_request")
    response_json = response.json()

    if response.status_code != 200:
        error_details = response_json.get("message", response.text)
        raise SodaCloudException(f"Failed to transition contract request: {error_details}")

    return ContractRequest.model_validate(response_json)
