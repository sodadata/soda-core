"""Unit tests for Databricks ``auth_type: azure-workload-identity``.

No live database and no Entra: WorkloadIdentityCredential and sql.connect are mocked.
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ClientAuthenticationError, ServiceRequestError
from databricks.sql.exc import RequestError
from pydantic import ValidationError
from soda_core.common.exceptions import DataSourceConnectionException
from soda_databricks.common.data_sources import databricks_data_source_connection as conn_mod
from soda_databricks.common.data_sources import databricks_workload_identity as wif_mod
from soda_databricks.common.data_sources.databricks_data_source_connection import DatabricksDataSourceConnection
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksAzureServicePrincipal,
    DatabricksAzureWorkloadIdentity,
    DatabricksOAuthM2M,
    DatabricksTokenAuth,
)
from soda_databricks.model.data_source.databricks_data_source import DatabricksDataSource

AZURE_DATABRICKS_SCOPE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
POD_ENV = {
    "AZURE_TENANT_ID": "pod-tenant",
    "AZURE_CLIENT_ID": "pod-client",
    "AZURE_FEDERATED_TOKEN_FILE": "/var/run/secrets/azure/tokens/azure-identity-token",
}
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.secret-access-token"


def infer(connection: dict):
    return DatabricksDataSource(name="test", type="databricks", connection=connection).connection_properties


def wif_connection(**extra) -> dict:
    return {
        "auth_type": "azure-workload-identity",
        "host": "https://adb-123.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/abc",
        "catalog": "main",
        **extra,
    }


def _make_connection():
    return DatabricksDataSourceConnection.__new__(DatabricksDataSourceConnection)


@pytest.fixture
def pod_env(monkeypatch):
    for name in POD_ENV:
        monkeypatch.delenv(name, raising=False)
    for name, value in POD_ENV.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


@pytest.fixture
def credential_cls():
    """WorkloadIdentityCredential mocked; DefaultAzureCredential made to fail if anything constructs it."""
    with patch("azure.identity.WorkloadIdentityCredential") as cls, patch(
        "azure.identity.DefaultAzureCredential", side_effect=AssertionError("DefaultAzureCredential must not be used")
    ) as default_cls:
        cls.return_value.get_token.return_value = SimpleNamespace(token=TOKEN, expires_on=0)
        cls.default_cls = default_cls
        yield cls


def connect(props, connect_side_effect=None):
    """Runs _create_connection with sql.connect mocked; returns the kwargs sql.connect was called with."""
    with patch.object(conn_mod.sql, "connect", side_effect=connect_side_effect) as mock_connect:
        _make_connection()._create_connection(props)
    return mock_connect.call_args.kwargs


# --------------------------------------------------------------------------- parsing


@pytest.mark.parametrize(
    "extra",
    [{}, {"client_id": "yaml-client"}, {"tenant_id": "yaml-tenant"}, {"client_id": "c", "tenant_id": "t"}],
)
def test_parses_with_and_without_client_and_tenant_id(extra):
    props = infer(wif_connection(**extra))
    assert isinstance(props, DatabricksAzureWorkloadIdentity)
    assert props.client_id == extra.get("client_id")
    assert props.tenant_id == extra.get("tenant_id")


@pytest.mark.parametrize(
    "leftover",
    [
        {"azure_client_secret": "s3cr3t-value"},
        {"access_token": "dapi-s3cr3t-value"},
        {"client_secret": "m2m-s3cr3t-value"},
    ],
)
def test_leftover_secret_keys_are_rejected_without_echoing_them(leftover):
    with pytest.raises(ValidationError) as exc_info:
        infer(wif_connection(**leftover))
    assert "extra_forbidden" in str(exc_info.value) or "Extra inputs" in str(exc_info.value)
    assert list(leftover.values())[0] not in str(exc_info.value)


@pytest.mark.parametrize("field", ["client_id", "tenant_id"])
def test_empty_client_or_tenant_id_is_rejected(field):
    with pytest.raises(ValidationError, match=f"{field} on the connection is empty"):
        infer(wif_connection(**{field: "  "}))


def test_tenant_id_with_invalid_characters_is_rejected():
    with pytest.raises(ValidationError, match="not a valid Entra tenant ID"):
        infer(wif_connection(tenant_id="evil/../tenant"))


def test_other_modes_still_parse():
    base = {"host": "adb-123.azuredatabricks.net", "http_path": "/x"}
    assert isinstance(infer({**base, "access_token": "dapi"}), DatabricksTokenAuth)
    assert isinstance(
        infer({**base, "auth_type": "databricks-oauth-m2m", "client_id": "c", "client_secret": "s"}),
        DatabricksOAuthM2M,
    )
    assert isinstance(
        infer(
            {
                **base,
                "auth_type": "azure-service-principal",
                "azure_client_id": "c",
                "azure_client_secret": "s",
                "azure_tenant_id": "t",
            }
        ),
        DatabricksAzureServicePrincipal,
    )


def test_use_cloud_fetch_is_accepted_and_passed_to_the_connector():
    kwargs = infer(wif_connection(use_cloud_fetch=False)).to_connection_kwargs()
    assert kwargs["use_cloud_fetch"] is False
    assert "use_cloud_fetch" not in infer(wif_connection()).to_connection_kwargs()


def test_identity_fields_are_not_connection_kwargs():
    kwargs = infer(wif_connection(client_id="c", tenant_id="t")).to_connection_kwargs()
    assert kwargs == {
        "server_hostname": "adb-123.azuredatabricks.net",
        "http_path": "/sql/1.0/warehouses/abc",
        "catalog": "main",
    }


# --------------------------------------------------------------------------- connecting


def test_connect_passes_credentials_provider_and_no_credential_kwargs(pod_env, credential_cls):
    called_kwargs = connect(infer(wif_connection(client_id="c", tenant_id="t")))

    assert set(called_kwargs) == {
        "user_agent_entry",
        "credentials_provider",
        "server_hostname",
        "http_path",
        "catalog",
    }
    header_factory = called_kwargs["credentials_provider"]()
    assert header_factory() == {"Authorization": f"Bearer {TOKEN}"}


def test_credential_uses_pod_environment_by_default(pod_env, credential_cls):
    connect(infer(wif_connection()))

    credential_cls.assert_called_once_with(
        tenant_id="pod-tenant",
        client_id="pod-client",
        token_file_path=POD_ENV["AZURE_FEDERATED_TOKEN_FILE"],
    )
    credential_cls.default_cls.assert_not_called()


def test_yaml_client_and_tenant_id_override_pod_environment(pod_env, credential_cls):
    connect(infer(wif_connection(client_id="yaml-client", tenant_id="yaml-tenant")))

    credential_cls.assert_called_once_with(
        tenant_id="yaml-tenant",
        client_id="yaml-client",
        token_file_path=POD_ENV["AZURE_FEDERATED_TOKEN_FILE"],
    )
    credential_cls.default_cls.assert_not_called()


def test_yaml_ids_make_pod_ids_unnecessary(pod_env, credential_cls):
    pod_env.delenv("AZURE_TENANT_ID")
    pod_env.delenv("AZURE_CLIENT_ID")
    connect(infer(wif_connection(client_id="yaml-client", tenant_id="yaml-tenant")))
    credential_cls.assert_called_once()


def test_each_header_factory_call_asks_the_credential_with_the_databricks_scope_only(pod_env, credential_cls):
    """The connector calls the header factory when it opens the session and again shortly before the cached
    token expires; each call must go back to the credential (which caches and refreshes), so a token that
    expires mid-scan is replaced."""
    credential_cls.return_value.get_token.side_effect = [
        SimpleNamespace(token="first-token", expires_on=1),
        SimpleNamespace(token="refreshed-token", expires_on=2),
    ]
    called_kwargs = connect(infer(wif_connection()))
    header_factory = called_kwargs["credentials_provider"]()

    assert credential_cls.return_value.get_token.call_count == 0  # no token fetched once at connect time
    assert header_factory() == {"Authorization": "Bearer first-token"}
    assert header_factory() == {"Authorization": "Bearer refreshed-token"}
    assert [c.args for c in credential_cls.return_value.get_token.call_args_list] == [
        (AZURE_DATABRICKS_SCOPE,),
        (AZURE_DATABRICKS_SCOPE,),
    ]
    assert all(not c.kwargs for c in credential_cls.return_value.get_token.call_args_list)


# --------------------------------------------------------------------------- diagnostics


@pytest.mark.parametrize(
    "missing, expected",
    [
        ("AZURE_TENANT_ID", "AZURE_TENANT_ID (or tenant_id in the connection) is not set"),
        ("AZURE_CLIENT_ID", "AZURE_CLIENT_ID (or client_id in the connection) is not set"),
        ("AZURE_FEDERATED_TOKEN_FILE", "AZURE_FEDERATED_TOKEN_FILE is not set"),
    ],
)
def test_missing_environment_is_named_with_pod_label_hint(pod_env, credential_cls, missing, expected):
    pod_env.delenv(missing)
    with patch.object(conn_mod.sql, "connect") as mock_connect:
        with pytest.raises(DataSourceConnectionException) as exc_info:
            _make_connection()._create_connection(infer(wif_connection()))

    message = str(exc_info.value)
    assert expected in message
    assert "azure.workload.identity/use" in message
    credential_cls.assert_not_called()
    mock_connect.assert_not_called()


def _entra_error(message: str) -> ClientAuthenticationError:
    return ClientAuthenticationError(message=message)


def _connect_through_header_factory(props):
    """sql.connect mocked the way the real connector behaves: it calls the header factory while opening the
    session and wraps whatever it raises in a generic RequestError."""

    def fake_connect(credentials_provider, **_):
        header_factory = credentials_provider()
        try:
            header_factory()
        except Exception as e:
            raise RequestError("Error during request to server", {"original-exception": str(e)}) from e
        return MagicMock()

    with patch.object(conn_mod.sql, "connect", side_effect=fake_connect):
        _make_connection()._create_connection(props)


def test_entra_rejection_is_surfaced_verbatim(pod_env, credential_cls):
    aadsts = (
        "AADSTS70021: No matching federated identity record found for presented assertion subject "
        "'system:serviceaccount:soda:soda-agent'."
    )
    credential_cls.return_value.get_token.side_effect = _entra_error(aadsts)

    with pytest.raises(DataSourceConnectionException) as exc_info:
        _connect_through_header_factory(infer(wif_connection()))

    message = str(exc_info.value)
    assert aadsts in message
    assert "client ID 'pod-client' in tenant 'pod-tenant'" in message
    assert "not found in tenant" not in message


def test_aadsts700016_hints_at_tenant_id(pod_env, credential_cls):
    credential_cls.return_value.get_token.side_effect = _entra_error(
        "AADSTS700016: Application with identifier 'pod-client' was not found in the directory 'pod-tenant'."
    )
    with pytest.raises(DataSourceConnectionException) as exc_info:
        _connect_through_header_factory(infer(wif_connection()))

    message = str(exc_info.value)
    assert "AADSTS700016" in message
    assert "If it lives in another Entra tenant, set tenant_id on the connection" in message


def test_aadsts700016_with_configured_tenant_points_at_the_connection(pod_env, credential_cls):
    credential_cls.return_value.get_token.side_effect = _entra_error("AADSTS700016: Application not found.")
    with pytest.raises(DataSourceConnectionException) as exc_info:
        _connect_through_header_factory(infer(wif_connection(tenant_id="bu-tenant")))

    message = str(exc_info.value)
    assert "tenant 'bu-tenant', which is set as tenant_id on the connection" in message


@pytest.mark.parametrize(
    "failure, expected",
    [
        (
            FileNotFoundError(2, "No such file or directory", POD_ENV["AZURE_FEDERATED_TOKEN_FILE"]),
            "FileNotFoundError: [Errno 2] No such file or directory",
        ),
        (
            ServiceRequestError("Failed to resolve 'login.microsoftonline.com'"),
            "ServiceRequestError: Failed to resolve 'login.microsoftonline.com'",
        ),
    ],
)
def test_token_request_failures_other_than_entra_rejections_are_surfaced(pod_env, credential_cls, failure, expected):
    """An unreadable token file or an unreachable Entra endpoint must not end as the connector's bare
    'Error during request to server'."""
    credential_cls.return_value.get_token.side_effect = failure

    with pytest.raises(DataSourceConnectionException) as exc_info:
        _connect_through_header_factory(infer(wif_connection()))

    message = str(exc_info.value)
    assert message.startswith("Could not get an Entra access token")
    assert expected in message
    assert "client ID 'pod-client' in tenant 'pod-tenant'" in message
    assert POD_ENV["AZURE_FEDERATED_TOKEN_FILE"] in message
    assert "login.microsoftonline.com" in message


@pytest.mark.parametrize("http_code", [401, 403])
def test_databricks_rejecting_the_token_points_at_workspace_membership(pod_env, credential_cls, http_code):
    rejected = RequestError("Error during request to server", {"http-code": http_code})

    with pytest.raises(DataSourceConnectionException) as exc_info:
        connect(infer(wif_connection(client_id="uami-client")), connect_side_effect=rejected)

    message = str(exc_info.value)
    assert f"HTTP {http_code}" in message
    assert "client ID 'uami-client'" in message
    assert "added to the workspace as a service principal" in message
    assert "Unity Catalog" in message and "SQL warehouse" in message


def test_other_connect_errors_pass_through(pod_env, credential_cls):
    unreachable = RequestError("Error during request to server", {"http-code": 502})
    with pytest.raises(RequestError):
        connect(infer(wif_connection()), connect_side_effect=unreachable)


def test_the_three_failure_messages_differ(pod_env, credential_cls):
    messages = []

    pod_env.delenv("AZURE_FEDERATED_TOKEN_FILE")
    with pytest.raises(DataSourceConnectionException) as e1:
        connect(infer(wif_connection()))
    messages.append(str(e1.value))
    pod_env.setenv("AZURE_FEDERATED_TOKEN_FILE", POD_ENV["AZURE_FEDERATED_TOKEN_FILE"])

    credential_cls.return_value.get_token.side_effect = _entra_error("AADSTS70021: no match")
    with pytest.raises(DataSourceConnectionException) as e2:
        _connect_through_header_factory(infer(wif_connection()))
    messages.append(str(e2.value))

    with pytest.raises(DataSourceConnectionException) as e3:
        connect(
            infer(wif_connection()),
            connect_side_effect=RequestError("Error during request to server", {"http-code": 403}),
        )
    messages.append(str(e3.value))

    assert messages[0].startswith("Workload identity federation is not set up")
    assert messages[1].startswith("Entra ID rejected")
    assert messages[2].startswith("An Entra access token was acquired")


def test_no_token_or_assertion_in_logs(pod_env, credential_cls, caplog, tmp_path):
    assertion = "eyJhbGciOiJSUzI1NiJ9.projected-service-account-assertion"
    token_file = tmp_path / "token"
    token_file.write_text(assertion)
    pod_env.setenv("AZURE_FEDERATED_TOKEN_FILE", str(token_file))

    caplog.set_level(logging.DEBUG)
    connection = DatabricksDataSourceConnection.__new__(DatabricksDataSourceConnection)
    connection.name = "wif"
    connection.connection = None
    connection.connection_properties = infer(wif_connection())

    def fake_connect(credentials_provider, **_):
        credentials_provider()()  # a request with the token in its headers
        raise RequestError("Error during request to server", {"http-code": 403})

    with patch.object(conn_mod.sql, "connect", side_effect=fake_connect):
        connection.open_connection()  # logs connection properties at debug and the failure at error

    assert "acquired through workload identity federation" in caplog.text
    assert TOKEN not in caplog.text
    assert assertion not in caplog.text


def test_workload_identity_module_scope_constant():
    assert wif_mod.AZURE_DATABRICKS_TOKEN_SCOPE == AZURE_DATABRICKS_SCOPE
