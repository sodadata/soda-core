from __future__ import annotations

from unittest.mock import MagicMock

import azure.identity
import pytest
from pydantic import ValidationError
from soda_sqlserver.common.data_sources import sqlserver_data_source_connection as connection_module
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    AZURE_SQL_TOKEN_SCOPE,
    SQL_COPT_SS_ACCESS_TOKEN,
    SqlServerActiveDirectoryServicePrincipalAuthentication,
    SqlServerDataSource,
    SqlServerDataSourceConnection,
    SqlServerPasswordAuth,
    SqlServerWorkloadIdentityAuthentication,
    pack_access_token,
)

# Known vector for SQL_COPT_SS_ACCESS_TOKEN: UTF-16-LE token with a 4-byte little-endian length prefix.
ACCESS_TOKEN_FIXTURE = "eyJ0eXAi.payload.sig"
PACKED_ACCESS_TOKEN_FIXTURE = (
    b"\x28\x00\x00\x00"
    b"e\x00y\x00J\x000\x00e\x00X\x00A\x00i\x00.\x00"
    b"p\x00a\x00y\x00l\x00o\x00a\x00d\x00.\x00s\x00i\x00g\x00"
)

CONNECTION = {"host": "myserver.database.windows.net", "port": 1433, "database": "sales"}


def parse(connection: dict) -> object:
    return SqlServerDataSource(name="ds", type="sqlserver", connection=connection).connection_properties


@pytest.fixture
def credential_class(monkeypatch) -> MagicMock:
    credential_class = MagicMock(name="WorkloadIdentityCredential")
    credential_class.return_value.get_token.return_value.token = ACCESS_TOKEN_FIXTURE
    monkeypatch.setattr(azure.identity, "WorkloadIdentityCredential", credential_class)
    return credential_class


@pytest.fixture
def pyodbc_connect(monkeypatch) -> MagicMock:
    pyodbc_connect = MagicMock(name="pyodbc.connect")
    pyodbc_connect.return_value.getinfo.return_value = "16.00.1000"
    monkeypatch.setattr(connection_module.pyodbc, "connect", pyodbc_connect)
    return pyodbc_connect


@pytest.fixture(autouse=True)
def azure_environment(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-from-env")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client-from-env")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token")


def test_workload_identity_parses_without_client_id() -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity"})

    assert isinstance(config, SqlServerWorkloadIdentityAuthentication)
    assert config.client_id is None


def test_workload_identity_parses_with_client_id() -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity", "client_id": "client-from-yaml"})

    assert isinstance(config, SqlServerWorkloadIdentityAuthentication)
    assert config.client_id == "client-from-yaml"


def test_connection_string_has_no_credentials() -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity", "client_id": "client-from-yaml"})

    connection_string = SqlServerDataSourceConnection.build_connection_string(config)

    assert connection_string.split(";")[:3] == [
        "DRIVER={ODBC Driver 18 for SQL Server}",
        "DATABASE=sales",
        "SERVER=myserver.database.windows.net,1433",
    ]
    assert "UID=" not in connection_string
    assert "PWD=" not in connection_string
    assert "Authentication=" not in connection_string
    assert "client-from-yaml" not in connection_string


def test_pack_access_token_matches_known_vector() -> None:
    assert pack_access_token(ACCESS_TOKEN_FIXTURE) == PACKED_ACCESS_TOKEN_FIXTURE


def test_connect_passes_the_token_as_pre_connect_attribute(credential_class, pyodbc_connect) -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity"})

    SqlServerDataSourceConnection(name="ds", connection_properties=config)

    pyodbc_connect.assert_called_once()
    assert pyodbc_connect.call_args.kwargs["attrs_before"] == {SQL_COPT_SS_ACCESS_TOKEN: PACKED_ACCESS_TOKEN_FIXTURE}
    credential_class.return_value.get_token.assert_called_once_with(AZURE_SQL_TOKEN_SCOPE)
    assert AZURE_SQL_TOKEN_SCOPE == "https://database.windows.net/.default"


@pytest.mark.parametrize("leftover_key", ["client_secret", "password", "user"])
def test_workload_identity_rejects_leftover_credentials(leftover_key) -> None:
    with pytest.raises(ValidationError):
        parse({**CONNECTION, "authentication": "workloadidentity", "client_id": "client-from-yaml", leftover_key: "x"})


def test_scope_overrides_the_default_token_scope(credential_class, pyodbc_connect) -> None:
    sovereign_scope = "https://database.usgovcloudapi.net/.default"
    config = parse({**CONNECTION, "authentication": "workloadidentity", "scope": sovereign_scope})

    SqlServerDataSourceConnection(name="ds", connection_properties=config)

    credential_class.return_value.get_token.assert_called_once_with(sovereign_scope)


def test_client_id_from_environment_is_used_without_yaml_client_id(credential_class, pyodbc_connect) -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity"})

    SqlServerDataSourceConnection(name="ds", connection_properties=config)

    credential_class.assert_called_once()
    assert credential_class.call_args.kwargs["client_id"] == "client-from-env"


def test_yaml_client_id_overrides_environment(credential_class, pyodbc_connect) -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity", "client_id": "client-from-yaml"})

    SqlServerDataSourceConnection(name="ds", connection_properties=config)

    credential_class.assert_called_once()
    assert credential_class.call_args.kwargs["client_id"] == "client-from-yaml"


def test_default_azure_credential_is_never_used(monkeypatch, credential_class, pyodbc_connect) -> None:
    default_credential_class = MagicMock(name="DefaultAzureCredential")
    monkeypatch.setattr(azure.identity, "DefaultAzureCredential", default_credential_class)
    monkeypatch.setattr(connection_module, "DefaultAzureCredential", default_credential_class, raising=False)
    config = parse({**CONNECTION, "authentication": "workloadidentity"})

    SqlServerDataSourceConnection(name="ds", connection_properties=config)

    credential_class.assert_called_once()
    default_credential_class.assert_not_called()


def test_each_connection_gets_a_fresh_token(credential_class, pyodbc_connect) -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity"})

    connection = SqlServerDataSourceConnection(name="ds", connection_properties=config)
    connection.close_connection()
    connection.open_connection()

    assert credential_class.return_value.get_token.call_count == 2


@pytest.mark.parametrize(
    "connection, expected_type, expected_connection_string_suffix",
    [
        (
            {**CONNECTION, "user": "sa", "password": "Password1!"},
            SqlServerPasswordAuth,
            "ConnectRetryCount=0;UID={sa};PWD={Password1!}",
        ),
        (
            {**CONNECTION, "authentication": "sql", "user": "sa", "password": "Password1!"},
            SqlServerPasswordAuth,
            "ConnectRetryCount=0;UID={sa};PWD={Password1!}",
        ),
        (
            {
                **CONNECTION,
                "authentication": "activedirectoryserviceprincipal",
                "client_id": "spn-id",
                "client_secret": "spn-secret",
            },
            SqlServerActiveDirectoryServicePrincipalAuthentication,
            "ConnectRetryCount=0;Authentication=ActiveDirectoryServicePrincipal;UID={spn-id};PWD={spn-secret}",
        ),
    ],
)
def test_existing_authentication_variants_are_unchanged(
    connection, expected_type, expected_connection_string_suffix, credential_class, pyodbc_connect
) -> None:
    config = parse(connection)

    assert type(config) is expected_type
    connection_string = SqlServerDataSourceConnection.build_connection_string(config)
    assert expected_connection_string_suffix in connection_string

    SqlServerDataSourceConnection(name="ds", connection_properties=config)

    assert pyodbc_connect.call_args.kwargs["attrs_before"] is None
    credential_class.assert_not_called()
