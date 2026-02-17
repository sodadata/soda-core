from unittest.mock import ANY, MagicMock, mock_open, patch

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.request import (
    handle_fetch_proposal,
    handle_push_proposal,
    handle_transition_request,
)
from soda_core.contracts.contract_request import RequestStatus


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.cli.handlers.request.fetch_contract_proposal")
def test_handle_fetch_proposal(mock_fetch_proposal, mock_cloud_client):
    mock_proposal = MagicMock()
    mock_proposal.contents = "proposal contents"
    mock_proposal.proposal_number = 456
    mock_proposal.request_number = 123
    mock_fetch_proposal.return_value = mock_proposal

    with patch("builtins.open", mock_open()) as mock_file:
        exit_code = handle_fetch_proposal(
            soda_cloud_file_path="test.yml",
            request_number=123,
            output_file_path="test.yml",
            proposal_number=456,
        )

        mock_file().write.assert_called_once_with("proposal contents")
        assert exit_code == ExitCode.OK


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.cli.handlers.request.fetch_contract_proposal")
def test_handle_fetch_proposal_fetch_error(mock_fetch_proposal, mock_cloud_client, caplog):
    mock_fetch_proposal.side_effect = Exception("Failed to fetch")

    exit_code = handle_fetch_proposal(
        soda_cloud_file_path="test.yml",
        request_number=123,
        output_file_path="test.yml",
        proposal_number=456,
    )

    assert exit_code == ExitCode.LOG_ERRORS
    assert "Failed to fetch" in str(caplog.records[0].message)


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.cli.handlers.request.push_contract_proposal")
def test_handle_push_proposal_success(mock_push_proposal, mock_cloud_client):
    mock_file_contents = "updated contents"

    with patch("builtins.open", mock_open(read_data=mock_file_contents)):
        exit_code = handle_push_proposal(
            soda_cloud_file_path="test.yml",
            file_path="test.yml",
            request_number=123,
            message="test comment",
        )

    mock_push_proposal.assert_called_once_with(ANY, 123, mock_file_contents, "test comment")
    assert exit_code == ExitCode.OK


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.cli.handlers.request.push_contract_proposal")
def test_handle_push_proposal_error(mock_push_proposal, mock_cloud_client, caplog):
    mock_push_proposal.side_effect = Exception("Failed to push")
    mock_file_contents = "test contents"

    with patch("builtins.open", mock_open(read_data=mock_file_contents)):
        exit_code = handle_push_proposal(
            soda_cloud_file_path="test.yml",
            file_path="test.yml",
            request_number=123,
            message="test comment",
        )

    assert exit_code == ExitCode.LOG_ERRORS
    assert "Failed to push" in str(caplog.records[0].message)


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.cli.handlers.request.transition_contract_request")
def test_handle_transition_request_success(mock_transition, mock_cloud_client):
    exit_code = handle_transition_request(
        soda_cloud_file_path="test.yml", request_number=123, status=RequestStatus.DONE
    )

    mock_transition.assert_called_once_with(ANY, 123, RequestStatus.DONE)
    assert exit_code == ExitCode.OK


@patch("soda_core.common.soda_cloud.SodaCloud.from_config")
@patch("soda_core.cli.handlers.request.transition_contract_request")
def test_handle_transition_request_error(mock_transition, mock_cloud_client, caplog):
    mock_transition.side_effect = Exception("Failed to transition")

    exit_code = handle_transition_request(
        soda_cloud_file_path="test.yml",
        request_number=123,
        status=RequestStatus.DONE,
    )

    assert exit_code == ExitCode.LOG_ERRORS
    assert "Failed to transition" in str(caplog.records[0].message)
