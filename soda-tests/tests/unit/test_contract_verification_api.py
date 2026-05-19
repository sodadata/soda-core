from unittest.mock import MagicMock, patch

import pytest
from soda_core.common.exceptions import (
    InvalidArgumentException,
    InvalidDataSourceConfigurationException,
    SodaCloudException,
    YamlParserException,
)
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api.verify_api import (
    ContractVerificationSession,
    _create_datasource_yamls,
    all_none_or_empty,
    verify_contract,
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
        _ = verify_contract(
            contract_file_path=None,
            dataset_identifier="some_dataset",
            data_source_file_path="ds.yaml",
            soda_cloud_file_path=None,
            variables={},
            publish=False,
            use_runner=False,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )


def test_handle_verify_contract_raises_exception_when_using_dataset_names_without_cloud_configuration_agent_deprecated():
    """Deprecated alias: verifies the legacy ``use_agent`` kwarg still works and emits a DeprecationWarning."""
    with pytest.warns(DeprecationWarning, match="use_agent"):
        with pytest.raises(
            InvalidArgumentException,
            match="A Soda Cloud configuration file is required to use the -d/--dataset argument."
            "Please provide the '--soda-cloud' argument with a valid configuration file path.",
        ):
            _ = verify_contract(
                contract_file_path=None,
                dataset_identifier="some_dataset",
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
        _ = verify_contract(
            contract_file_path=None,
            dataset_identifier="some_dataset",
            data_source_file_path="ds.yaml",
            soda_cloud_file_path=None,
            variables={},
            publish=True,
            use_runner=False,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )


def test_handle_verify_contract_returns_exit_code_3_when_using_publish_without_cloud_configuration_agent_deprecated():
    with pytest.warns(DeprecationWarning, match="use_agent"):
        with pytest.raises(
            InvalidArgumentException,
            match="A Soda Cloud configuration file is required to use the -p/--publish argument. "
            "Please provide the '--soda-cloud' argument with a valid configuration file path.",
        ):
            _ = verify_contract(
                contract_file_path=None,
                dataset_identifier="some_dataset",
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
        _ = verify_contract(
            contract_file_path=None,
            dataset_identifier=None,
            data_source_file_path="ds.yaml",
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=True,
            use_runner=False,
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
        _ = verify_contract(
            contract_file_path="contract.yaml",
            dataset_identifier=None,
            data_source_file_path=None,
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=True,
            use_runner=False,
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
        _ = verify_contract(
            contract_file_path="contract.yaml",
            dataset_identifier=None,
            data_source_file_path=None,
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=True,
            use_runner=True,
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

    _ = verify_contract(
        contract_file_path=None,
        dataset_identifier="my/super/awesome/identifier",
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=True,
        use_runner=False,
        verbose=False,
        blocking_timeout_in_minutes=10,
    )

    assert (
        "Could not fetch contract for dataset 'my/super/awesome/identifier': skipping verification" in caplog.messages
    )


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_handle_verify_contract_returns_exit_code_0_when_no_valid_remote_contracts_left(mock_cloud_client, caplog):
    mock_cloud_client.return_value.fetch_contract_for_dataset.side_effect = SodaCloudException("woopsie")

    _ = verify_contract(
        contract_file_path=None,
        dataset_identifier="my/super/awesome/identifier",
        data_source_file_path="ds.yaml",
        soda_cloud_file_path="sc.yaml",
        variables={},
        publish=True,
        use_runner=False,
        verbose=False,
        blocking_timeout_in_minutes=10,
    )

    assert "No contracts given. Exiting." in caplog.messages


def test_local_flow_does_not_fetch_datasource_config_from_cloud():
    """
    In the local flow (use_runner=False), _create_datasource_yamls
    should NOT call fetch_data_source_configuration_for_dataset on the
    SodaCloud client. Fetching datasource configs from Cloud in the local
    flow is a security risk — it can expose host/connection info to users
    who only have contract execution permissions.

    When no local data source files are provided in local flow, it should
    raise an error instead of fetching from Cloud.
    """
    with pytest.raises(InvalidDataSourceConfigurationException):
        _create_datasource_yamls(
            data_source_file_paths=[],
            use_runner=False,
        )


def test_local_flow_with_dataset_identifier_uses_local_datasource_config(tmp_path):
    """
    When both --dataset and --data-source are provided in local flow,
    only the local data source config should be used (no Cloud fetch).
    """
    ds_file = tmp_path / "ds.yml"
    ds_file.write_text("type: duckdb\npath: test.db")

    result = _create_datasource_yamls(
        data_source_file_paths=[str(ds_file)],
        use_runner=False,
    )

    assert len(result) == 1


def test_runner_flow_without_local_datasource_returns_none():
    """
    In runner flow (use_runner=True), when no local data source files
    are provided, return None (runner provides its own config). Should NOT
    fetch from Cloud.
    """
    result = _create_datasource_yamls(
        data_source_file_paths=[],
        use_runner=True,
    )

    assert result is None


def test_agent_flow_without_local_datasource_returns_none_deprecated():
    """Deprecated alias for the runner flow test above; preserved for backwards compat."""
    # _create_datasource_yamls is a private helper with no legacy kwarg name, but the public
    # surface that calls it accepts both. This test now just mirrors the runner-named one.
    result = _create_datasource_yamls(
        data_source_file_paths=[],
        use_runner=True,
    )

    assert result is None


@patch("soda_core.contracts.api.verify_api.SodaCloud.from_config")
def test_local_flow_with_dataset_but_no_datasource_raises_error(mock_cloud_client):
    """
    Using --dataset without --data-source in local flow should raise
    an error at validation, requiring the user to provide a local data source config.
    """
    with pytest.raises(InvalidArgumentException, match="At least one of -ds/--data-source"):
        _ = verify_contract(
            contract_file_path=None,
            dataset_identifier="my_ds/my_schema/my_table",
            data_source_file_path=None,
            soda_cloud_file_path="sc.yaml",
            variables={},
            publish=False,
            use_runner=False,
            verbose=False,
            blocking_timeout_in_minutes=10,
        )


# Backwards-compat smoke tests for the deprecated public API names.


def test_verify_contract_on_agent_alias_emits_deprecation_warning(monkeypatch):
    """The legacy public ``verify_contract_on_agent`` function emits a DeprecationWarning
    and delegates to ``verify_contract_on_runner``."""
    from soda_core.contracts.api.verify_api import verify_contract_on_agent

    called = {}

    def fake_runner(**kwargs):
        called.update(kwargs)
        return "ok"

    monkeypatch.setattr(
        "soda_core.contracts.api.verify_api.verify_contract_on_runner",
        fake_runner,
    )

    with pytest.warns(DeprecationWarning, match="verify_contract_on_agent"):
        result = verify_contract_on_agent(soda_cloud_file_path="sc.yaml", contract_file_path="c.yaml")

    assert result == "ok"
    assert called["soda_cloud_file_path"] == "sc.yaml"


def test_verify_contracts_on_agent_alias_emits_deprecation_warning(monkeypatch):
    from soda_core.contracts.api.verify_api import verify_contracts_on_agent

    monkeypatch.setattr(
        "soda_core.contracts.api.verify_api.verify_contracts_on_runner",
        lambda **kwargs: "ok",
    )

    with pytest.warns(DeprecationWarning, match="verify_contracts_on_agent"):
        result = verify_contracts_on_agent(
            soda_cloud_file_path="sc.yaml", contract_file_paths=["c.yaml"], dataset_identifiers=[]
        )

    assert result == "ok"
