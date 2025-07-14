from unittest.mock import MagicMock, patch

import pytest
from soda_core.common.exceptions import (
    InvalidArgumentException,
    SodaCloudException,
    YamlParserException,
)
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api.verify_api import (
    ContractVerificationSession,
    all_none_or_empty,
    verify_contracts,
)
from soda_core.contracts.contract_verification import (
    ContractVerificationSessionResult,
    SodaException,
)


def test_contract_verification_file_api():
    with pytest.raises(YamlParserException):
        contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_file_path("../soda/mydb/myschema/table.yml")],
            variables={"env": "test"},
        )

        assert (
            "Contract file '../soda/mydb/myschema/table.yml' does not exist"
            in contract_verification_session_result.get_errors_str()
        )


def test_contract_verification_file_api_exception_on_error():
    with pytest.raises(YamlParserException):
        with pytest.raises(SodaException) as e:
            ContractVerificationSession.execute(
                contract_yaml_sources=[ContractYamlSource.from_file_path("../soda/mydb/myschema/table.yml")],
                variables={"env": "test"},
            ).assert_ok()

            exception_string = str(e.value)
            assert "Contract file '../soda/mydb/myschema/table.yml' does not exist" in exception_string


def test_contract_provided_and_configured():
    """
    If there is no default data source configured and there is none provided in the contract, an error has to be logged
    """
    contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
        contract_yaml_sources=[
            ContractYamlSource.from_str(
                f"""
              dataset: abc/CUSTOMERS
              columns:
                - name: id
            """
            )
        ],
    )

    assert "Data source 'abc' not found" in contract_verification_session_result.get_errors_str()


@pytest.mark.parametrize(
    "array, expected", [(["a", "b"], False), (["a", None], False), ([None, None], False), ([], True), (None, True)]
)
def test_all_none_or_empty(array, expected):
    is_all_none_or_empty = all_none_or_empty(array)
    assert is_all_none_or_empty == expected


def test_handle_verify_contract_raises_exception_when_using_dataset_names_without_cloud_configuration():
    with pytest.raises(
        InvalidArgumentException,
        match="A Soda Cloud configuration file is required to use the -d/--dataset argument."
        "Please provide the '--soda-cloud' argument with a valid configuration file path.",
    ):
        _ = verify_contracts(
            contract_file_paths=None,
            dataset_identifiers=["some_dataset"],
            data_source_file_path="ds.yaml",
            soda_cloud_file_path=None,
            variables={},
            publish=False,
            use_agent=False,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )


def test_handle_verify_contract_returns_exit_code_3_when_using_publish_without_cloud_configuration():
    with pytest.raises(
        InvalidArgumentException,
        match="A Soda Cloud configuration file is required to use the -p/--publish argument. "
        "Please provide the '--soda-cloud' argument with a valid configuration file path.",
    ):
        _ = verify_contracts(
            contract_file_paths=None,
            dataset_identifiers=["some_dataset"],
            data_source_file_path="ds.yaml",
            soda_cloud_file_path=None,
            variables={},
            publish=True,
            use_agent=False,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_returns_exit_code_3_when_no_contract_file_paths_or_dataset_identifiers(
    mock_cloud_client,
):
    with pytest.raises(
        InvalidArgumentException, match="At least one of -c/--contract or -d/--dataset arguments is required."
    ):
        _ = verify_contracts(
            contract_file_paths=None,
            dataset_identifiers=None,
            data_source_file_path="ds.yaml",
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=True,
            use_agent=False,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_returns_exit_code_3_when_no_data_source_configuration_or_dataset_identifiers(
    mock_cloud_client,
):
    with pytest.raises(
        InvalidArgumentException, match="At least one of -ds/--data-source or -d/--dataset value is required."
    ):
        _ = verify_contracts(
            contract_file_paths=["contract.yaml"],
            dataset_identifiers=None,
            data_source_file_path=None,
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=True,
            use_agent=False,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )


@pytest.mark.skip(reason="Needs mocking of Contract verification.")
@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_returns_exit_code_0_when_no_data_source_configuration_or_dataset_identifiers_and_remote(
    mock_cloud_client,
):
    mock_cloud_client.return_value = MagicMock(spec=SodaCloud)

    try:
        _ = verify_contracts(
            contract_file_paths=["contract.yaml"],
            dataset_identifiers=None,
            data_source_file_path=None,
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=True,
            use_agent=True,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )
    except Exception as exc:
        pytest.fail(f"An unexpected exception was raised: {exc}")


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_skips_contract_when_contract_fetching_from_cloud_returns_errors(
    mock_cloud_client, caplog
):
    mock_cloud_client.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("woopsie")

    _ = verify_contracts(
        contract_file_paths=None,
        dataset_identifiers=["my/super/awesome/identifier"],
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=True,
        use_agent=False,
        verbose=False,
        blocking_timeout_in_minutes=10,
    )

    assert (
        "Could not fetch contract for dataset 'my/super/awesome/identifier': skipping verification" in caplog.messages
    )


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_returns_exit_code_0_when_no_valid_remote_contracts_left(mock_cloud_client, caplog):
    mock_cloud_client.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("woopsie")

    _ = verify_contracts(
        contract_file_paths=None,
        dataset_identifiers=["my/super/awesome/identifier"],
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=True,
        use_agent=False,
        verbose=False,
        blocking_timeout_in_minutes=10,
    )

    assert "No contracts given. Exiting." in caplog.messages
