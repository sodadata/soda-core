from __future__ import annotations

import functools
import json
import logging
from unittest.mock import MagicMock, patch

import azure.identity
import pyodbc
import pytest
from azure.core.exceptions import ClientAuthenticationError
from azure.core.pipeline.transport import HttpTransport
from soda_core.common.exceptions import DataSourceConnectionException
from soda_sqlserver.common.data_sources import sqlserver_data_source_connection as connection_module
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSourceConnection,
    SqlServerWorkloadIdentityAuthentication,
)

ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOi.access-token-payload.signature"
FEDERATED_ASSERTION = "eyJhbGciOiJSUzI1NiIsImtpZCI6.service-account-assertion.signature"
AADSTS_MESSAGE = (
    "AADSTS700213: No matching federated identity record found for presented assertion subject "
    "'system:serviceaccount:soda:soda-runner'. Check your federated identity credential Subject, Audience "
    "and Issuer against the presented assertion."
)
LOGIN_FAILED = pyodbc.InterfaceError(
    "28000",
    "[28000] [Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Login failed for user "
    "'<token-identified principal>'. (18456) (SQLDriverConnect)",
)
HOST_UNREACHABLE = pyodbc.OperationalError(
    "08001",
    "[08001] [Microsoft][ODBC Driver 18 for SQL Server]TCP Provider: Error code 0x2749 (10057) (SQLDriverConnect)",
)
LOGIN_TIMEOUT = pyodbc.OperationalError(
    "HYT00", "[HYT00] [Microsoft][ODBC Driver 18 for SQL Server]Login timeout expired (0) (SQLDriverConnect)"
)


def workload_identity_config(**kwargs) -> SqlServerWorkloadIdentityAuthentication:
    return SqlServerWorkloadIdentityAuthentication(
        host="myserver.database.windows.net", database="sales", authentication="workloadidentity", **kwargs
    )


def unopened_connection(config) -> SqlServerDataSourceConnection:
    with patch.object(SqlServerDataSourceConnection, "open_connection"):
        return SqlServerDataSourceConnection(name="ds", connection_properties=config)


@pytest.fixture(autouse=True)
def azure_environment(monkeypatch, tmp_path) -> None:
    token_file = tmp_path / "azure-identity-token"
    token_file.write_text(FEDERATED_ASSERTION)
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-from-env")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client-from-env")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", str(token_file))


@pytest.fixture
def credential_class(monkeypatch) -> MagicMock:
    credential_class = MagicMock(name="WorkloadIdentityCredential")
    credential_class.return_value.get_token.return_value.token = ACCESS_TOKEN
    monkeypatch.setattr(azure.identity, "WorkloadIdentityCredential", credential_class)
    return credential_class


class FakeEntraResponse:
    def __init__(self, request, status_code: int, body: dict):
        self.request = request
        self.status_code = status_code
        self.reason = "OK" if status_code == 200 else "Bad Request"
        self.headers = {"Content-Type": "application/json"}
        self.content_type = "application/json"
        self._body = json.dumps(body).encode()

    def body(self) -> bytes:
        return self._body

    def text(self, encoding=None) -> str:
        return self._body.decode()


class FakeEntraTransport(HttpTransport):
    """Answers the token endpoint in place of Entra, so the real WorkloadIdentityCredential reads the token
    file and sends the federated assertion through the azure-core pipeline, its logging policies included."""

    def __init__(self) -> None:
        self.status_code = 200
        self.body = {"access_token": ACCESS_TOKEN, "expires_in": 3600, "token_type": "Bearer"}
        self.request_bodies: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *args) -> None:
        pass

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    def send(self, request, **kwargs) -> FakeEntraResponse:
        self.request_bodies.append(str(request.data or request.body))
        return FakeEntraResponse(request, self.status_code, self.body)


@pytest.fixture
def entra_transport(monkeypatch) -> FakeEntraTransport:
    transport = FakeEntraTransport()
    monkeypatch.setattr(
        azure.identity,
        "WorkloadIdentityCredential",
        functools.partial(azure.identity.WorkloadIdentityCredential, transport=transport),
    )
    return transport


@pytest.fixture
def pyodbc_connect(monkeypatch) -> MagicMock:
    pyodbc_connect = MagicMock(name="pyodbc.connect")
    pyodbc_connect.return_value.getinfo.return_value = "16.00.1000"
    monkeypatch.setattr(connection_module.pyodbc, "connect", pyodbc_connect)
    return pyodbc_connect


@pytest.mark.parametrize("variable", ["AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE", "AZURE_CLIENT_ID"])
def test_missing_environment_variable_is_named(monkeypatch, variable, credential_class, pyodbc_connect) -> None:
    monkeypatch.delenv(variable)

    with pytest.raises(DataSourceConnectionException) as exc_info:
        unopened_connection(workload_identity_config())._create_connection(workload_identity_config())

    message = str(exc_info.value)
    assert f"{variable}" in message
    assert "azure.workload.identity/use" in message
    for other in {"AZURE_TENANT_ID", "AZURE_FEDERATED_TOKEN_FILE", "AZURE_CLIENT_ID"} - {variable}:
        assert other not in message.split(" not set ")[0]
    credential_class.assert_not_called()
    pyodbc_connect.assert_not_called()


def test_missing_client_id_mentions_the_connection_setting(monkeypatch, credential_class) -> None:
    monkeypatch.delenv("AZURE_CLIENT_ID")

    with pytest.raises(DataSourceConnectionException, match=r"AZURE_CLIENT_ID \(or client_id in the connection\)"):
        connection_module.acquire_workload_identity_token(workload_identity_config())


def test_yaml_client_id_satisfies_a_missing_environment_client_id(
    monkeypatch, credential_class, pyodbc_connect
) -> None:
    monkeypatch.delenv("AZURE_CLIENT_ID")
    config = workload_identity_config(client_id="client-from-yaml")

    unopened_connection(config)._create_connection(config)

    assert credential_class.call_args.kwargs["client_id"] == "client-from-yaml"
    assert credential_class.call_args.kwargs["tenant_id"] == "tenant-from-env"


def test_entra_rejection_surfaces_the_aadsts_message_verbatim(credential_class, pyodbc_connect) -> None:
    credential_class.return_value.get_token.side_effect = ClientAuthenticationError(message=AADSTS_MESSAGE)
    config = workload_identity_config()

    with pytest.raises(DataSourceConnectionException) as exc_info:
        unopened_connection(config)._create_connection(config)

    assert AADSTS_MESSAGE in str(exc_info.value)
    assert "client-from-env" in str(exc_info.value)
    pyodbc_connect.assert_not_called()


def test_login_failure_after_token_acquisition_points_at_the_database_user(credential_class, pyodbc_connect) -> None:
    pyodbc_connect.side_effect = LOGIN_FAILED
    config = workload_identity_config()

    with pytest.raises(DataSourceConnectionException) as exc_info:
        unopened_connection(config)._create_connection(config)

    message = str(exc_info.value)
    assert "access token was acquired" in message
    assert "FROM EXTERNAL PROVIDER" in message
    assert "'sales'" in message
    assert "Login failed" in message
    assert exc_info.value.__cause__ is LOGIN_FAILED


@pytest.mark.parametrize(
    "error", [pytest.param(HOST_UNREACHABLE, id="unreachable"), pytest.param(LOGIN_TIMEOUT, id="timeout")]
)
def test_connect_failure_that_is_not_a_login_failure_is_not_rewritten(error, credential_class, pyodbc_connect) -> None:
    pyodbc_connect.side_effect = error
    config = workload_identity_config()

    with pytest.raises(DataSourceConnectionException) as exc_info:
        unopened_connection(config)._create_connection(config)

    assert "access token was acquired" not in str(exc_info.value)
    assert "FROM EXTERNAL PROVIDER" not in str(exc_info.value)
    assert exc_info.value.__cause__ is error


def test_login_failure_for_other_authentication_is_not_rewritten(credential_class, pyodbc_connect) -> None:
    pyodbc_connect.side_effect = LOGIN_FAILED
    config = connection_module.SqlServerPasswordAuth(
        host="myserver.database.windows.net", database="sales", user="sa", password="Password1!"
    )

    with pytest.raises(DataSourceConnectionException) as exc_info:
        unopened_connection(config)._create_connection(config)

    assert "access token" not in str(exc_info.value)
    assert exc_info.value.__cause__ is LOGIN_FAILED


@pytest.mark.parametrize(
    "failure",
    [
        pytest.param(None, id="success"),
        pytest.param("entra", id="entra-rejection"),
        pytest.param("login", id="login-failure"),
    ],
)
def test_debug_logs_contain_neither_assertion_nor_token(caplog, failure, entra_transport, pyodbc_connect) -> None:
    if failure == "entra":
        entra_transport.status_code = 400
        entra_transport.body = {"error": "invalid_client", "error_description": AADSTS_MESSAGE}
    elif failure == "login":
        pyodbc_connect.side_effect = LOGIN_FAILED

    with caplog.at_level(logging.DEBUG):
        # The constructor runs open_connection, which logs the connection properties at debug level
        # and logs any connect error with its traceback.
        SqlServerDataSourceConnection(name="ds", connection_properties=workload_identity_config())

    # The real credential read the token file and sent the assertion, so it was there to leak.
    assert [FEDERATED_ASSERTION in body for body in entra_transport.request_bodies] == [True]
    assert "connection properties" in caplog.text
    if failure:
        assert "Could not connect to 'ds'" in caplog.text
    if failure == "entra":
        assert "AADSTS700213" in caplog.text
    assert ACCESS_TOKEN not in caplog.text
    assert FEDERATED_ASSERTION not in caplog.text
