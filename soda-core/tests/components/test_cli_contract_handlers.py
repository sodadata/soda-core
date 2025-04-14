from unittest.mock import MagicMock, patch

import pytest
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import (
    ContractVerificationSession,
    all_none_or_empty,
    handle_publish_contract,
    handle_test_contract,
    handle_verify_contract,
)
from soda_core.common.logs import Logs
from soda_core.contracts.contract_publication import (
    ContractPublication,
    ContractPublicationResult,
    ContractPublicationResultList,
)


@pytest.mark.parametrize(
    "array, expected", [(["a", "b"], False), (["a", None], False), ([None, None], False), ([], True), (None, True)]
)
def test_all_none_or_empty(array, expected):
    is_all_none_or_empty = all_none_or_empty(array)
    assert is_all_none_or_empty == expected


@pytest.mark.parametrize(
    "has_errors, has_failures, cloud_failed, expected_exit_code",
    [
        (False, False, False, ExitCode.OK),
        (False, True, False, ExitCode.CHECK_FAILURES),
        (True, False, False, ExitCode.LOG_ERRORS),
        (True, True, False, ExitCode.LOG_ERRORS),
        (False, False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
        (True, True, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
    ],
)
@patch.object(ContractVerificationSession, "execute")
def test_handle_verify_contract_exit_codes(mock_execute, has_errors, has_failures, cloud_failed, expected_exit_code):
    mock_contract_result = MagicMock()
    mock_contract_result.sending_results_to_soda_cloud_failed = cloud_failed

    mock_result = MagicMock()
    mock_result.has_errors.return_value = has_errors
    mock_result.is_failed.return_value = has_failures
    mock_result.contract_verification_results = [mock_contract_result]

    mock_execute.return_value = mock_result

    exit_code = handle_verify_contract(
        contract_file_paths=["contract.yaml"],
        dataset_identifiers=None,
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        publish=True,
        use_agent=False,
        blocking_timeout_in_minutes=10,
    )

    assert exit_code == expected_exit_code


def test_handle_verify_contract_returns_exit_code_3_when_using_dataset_names_without_cloud_configuration(caplog):
    exit_code = handle_verify_contract(
        contract_file_paths=None,
        dataset_identifiers=["some_dataset"],
        data_source_file_path="ds.yaml",
        soda_cloud_file_path=None,
        publish=False,
        use_agent=False,
        blocking_timeout_in_minutes=10,
    )
    assert (
        "A Soda Cloud configuration file is required to use the -d/--dataset argument."
        "Please provide the '--soda-cloud' argument with a valid configuration file path."
    ) in caplog.messages
    assert exit_code == ExitCode.LOG_ERRORS


def test_handle_verify_contract_returns_exit_code_3_when_using_publish_without_cloud_configuration(caplog):
    exit_code = handle_verify_contract(
        contract_file_paths=None,
        dataset_identifiers=["some_dataset"],
        data_source_file_path="ds.yaml",
        soda_cloud_file_path=None,
        publish=True,
        use_agent=False,
        blocking_timeout_in_minutes=10,
    )

    assert (
        "A Soda Cloud configuration file is required to use the -p/--publish argument. "
        "Please provide the '--soda-cloud' argument with a valid configuration file path."
    ) in caplog.messages
    assert exit_code == ExitCode.LOG_ERRORS


def test_handle_verify_contract_returns_exit_code_3_when_no_contract_file_paths_or_dataset_identifiers(caplog):
    exit_code = handle_verify_contract(
        contract_file_paths=None,
        dataset_identifiers=None,
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="soda-cloud.yaml",
        publish=True,
        use_agent=False,
        blocking_timeout_in_minutes=10,
    )

    assert "At least one of -c/--contract or -d/--dataset arguments is required." in caplog.messages
    assert exit_code == ExitCode.LOG_ERRORS


def test_handle_verify_contract_returns_exit_code_3_when_no_data_source_configuration_or_dataset_identifiers(caplog):
    exit_code = handle_verify_contract(
        contract_file_paths=["contract.yaml"],
        dataset_identifiers=None,
        data_source_file_path=None,
        soda_cloud_file_path="soda-cloud.yaml",
        publish=True,
        use_agent=False,
        blocking_timeout_in_minutes=10,
    )

    assert "At least one of -ds/--data-source or -d/--dataset value is required." in caplog.messages
    assert exit_code == ExitCode.LOG_ERRORS


@pytest.mark.skip(reason="Needs mocking of Contract verification.")
def test_handle_verify_contract_returns_exit_code_0_when_no_data_source_configuration_or_dataset_identifiers_and_remote(
    caplog,
):
    exit_code = handle_verify_contract(
        contract_file_paths=["contract.yaml"],
        dataset_identifiers=None,
        data_source_file_path=None,
        soda_cloud_file_path="soda-cloud.yaml",
        publish=True,
        use_agent=True,
        blocking_timeout_in_minutes=10,
    )

    assert "At least one of -ds/--data-source or -d/--dataset value is required." not in caplog.messages
    assert exit_code == ExitCode.OK


def test_handle_verify_contract_skips_contract_when_contract_fetching_from_cloud_returns_errors(caplog):
    with patch("soda_core.cli.handlers.contract.SodaCloud.from_yaml_source") as mock_cloud_from_yaml:
        mock_cloud = MagicMock()
        mock_cloud.fetch_contract_for_dataset.return_value = None
        mock_cloud_from_yaml.return_value = mock_cloud

        _ = handle_verify_contract(
            contract_file_paths=None,
            dataset_identifiers=["my/super/awesome/identifier"],
            data_source_file_path="ds.yaml",
            soda_cloud_file_path="soda-cloud.yaml",
            publish=True,
            use_agent=False,
            blocking_timeout_in_minutes=10,
        )

        assert (
            "Could not fetch contract for dataset 'my/super/awesome/identifier': skipping verification"
            in caplog.messages
        )


def test_handle_verify_contract_returns_exit_code_0_when_no_valid_remote_contracts_left(caplog):
    with patch("soda_core.cli.handlers.contract.SodaCloud.from_yaml_source") as mock_cloud_from_yaml:
        mock_cloud = MagicMock()
        mock_cloud.fetch_contract_for_dataset.return_value = None
        mock_cloud_from_yaml.return_value = mock_cloud

        exit_code = handle_verify_contract(
            contract_file_paths=None,
            dataset_identifiers=["my/super/awesome/identifier"],
            data_source_file_path="ds.yaml",
            soda_cloud_file_path="soda-cloud.yaml",
            publish=True,
            use_agent=False,
            blocking_timeout_in_minutes=10,
        )

        assert "No contracts given. Exiting." in caplog.messages
        assert exit_code == ExitCode.OK


@pytest.mark.parametrize(
    "has_errors, cloud_failed, expected_exit_code",
    [
        (False, False, ExitCode.OK),
        (True, False, ExitCode.LOG_ERRORS),
        # (False, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),  # TODO: support exit code 4 detection
        # (True, True, ExitCode.RESULTS_NOT_SENT_TO_CLOUD),
    ],
)
@patch.object(ContractPublication, "builder")
def test_handle_publish_contract_exit_codes(mock_builder, has_errors, cloud_failed, expected_exit_code):
    mock_logs = MagicMock(spec=Logs)
    mock_logs.has_errors.return_value = has_errors

    mock_result_list = ContractPublicationResultList(
        items=[ContractPublicationResult(contract=None)],
        logs=mock_logs,
    )

    mock_builder_instance = MagicMock()
    mock_builder_instance.build.return_value.execute.return_value = mock_result_list
    mock_builder.return_value = mock_builder_instance

    exit_code = handle_publish_contract(
        contract_file_paths=["contract.yaml"],
        soda_cloud_file_path="sc.yaml",
    )

    assert exit_code == expected_exit_code


@pytest.mark.parametrize(
    "has_errors, expected_exit_code",
    [
        (False, ExitCode.OK),
        (True, ExitCode.LOG_ERRORS),
    ],
)
@patch.object(ContractVerificationSession, "execute")
def test_handle_test_contract_exit_codes(mock_execute, has_errors, expected_exit_code):
    mock_result = MagicMock()
    mock_result.has_errors.return_value = has_errors

    mock_execute.return_value = mock_result

    exit_code = handle_test_contract(contract_file_paths=["contract.yaml"], data_source_file_path="ds.yaml")

    assert exit_code == expected_exit_code
