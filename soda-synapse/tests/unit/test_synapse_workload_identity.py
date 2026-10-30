from __future__ import annotations

from unittest.mock import MagicMock

import azure.identity
import pytest
from pydantic import ValidationError
from soda_sqlserver.common.data_sources import sqlserver_data_source_connection as sqlserver_connection_module
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    AZURE_SQL_TOKEN_SCOPE,
    SQL_COPT_SS_ACCESS_TOKEN,
    pack_access_token,
)
from soda_synapse.common.data_sources.synapse_data_source_connection import (
    SynapseActiveDirectoryServicePrincipalAuthentication,
    SynapseDataSource,
    SynapseDataSourceConnection,
    SynapsePasswordAuth,
    SynapseWorkloadIdentityAuthentication,
)

ACCESS_TOKEN = "eyJ0eXAi.payload.sig"
CONNECTION = {"host": "myworkspace.sql.azuresynapse.net", "port": 1433, "database": "sales"}


def parse(connection: dict) -> object:
    return SynapseDataSource(name="ds", type="synapse", connection=connection).connection_properties


@pytest.fixture(autouse=True)
def azure_environment(monkeypatch) -> None:
    monkeypatch.setenv("AZURE_TENANT_ID", "tenant-from-env")
    monkeypatch.setenv("AZURE_CLIENT_ID", "client-from-env")
    monkeypatch.setenv("AZURE_FEDERATED_TOKEN_FILE", "/var/run/secrets/azure/tokens/azure-identity-token")


@pytest.fixture
def credential_class(monkeypatch) -> MagicMock:
    credential_class = MagicMock(name="WorkloadIdentityCredential")
    credential_class.return_value.get_token.return_value.token = ACCESS_TOKEN
    monkeypatch.setattr(azure.identity, "WorkloadIdentityCredential", credential_class)
    return credential_class


@pytest.fixture
def pyodbc_connect(monkeypatch) -> MagicMock:
    pyodbc_connect = MagicMock(name="pyodbc.connect")
    pyodbc_connect.return_value.getinfo.return_value = "12.00.2000"
    monkeypatch.setattr(sqlserver_connection_module.pyodbc, "connect", pyodbc_connect)
    return pyodbc_connect


@pytest.mark.parametrize("client_id", [None, "client-from-yaml"])
def test_workload_identity_parses(client_id) -> None:
    connection = {**CONNECTION, "authentication": "workloadidentity"}
    if client_id:
        connection["client_id"] = client_id

    config = parse(connection)

    assert isinstance(config, SynapseWorkloadIdentityAuthentication)
    assert config.client_id == client_id


@pytest.mark.parametrize("leftover_key", ["client_secret", "password", "user"])
def test_workload_identity_rejects_leftover_credentials(leftover_key) -> None:
    secret = "leftover-secret"
    with pytest.raises(ValidationError) as exc_info:
        parse(
            {**CONNECTION, "authentication": "workloadidentity", "client_id": "client-from-yaml", leftover_key: secret}
        )

    assert secret not in str(exc_info.value)


def test_connection_string_has_no_credentials() -> None:
    config = parse({**CONNECTION, "authentication": "workloadidentity", "client_id": "client-from-yaml"})

    connection_string = SynapseDataSourceConnection.build_connection_string(config)

    assert "SERVER=myworkspace.sql.azuresynapse.net,1433" in connection_string
    assert "UID=" not in connection_string
    assert "PWD=" not in connection_string
    assert "Authentication=" not in connection_string


@pytest.mark.parametrize(
    "yaml_client_id, expected_client_id", [(None, "client-from-env"), ("client-from-yaml", "client-from-yaml")]
)
def test_connect_passes_the_token_as_pre_connect_attribute(
    yaml_client_id, expected_client_id, credential_class, pyodbc_connect
) -> None:
    connection = {**CONNECTION, "authentication": "workloadidentity"}
    if yaml_client_id:
        connection["client_id"] = yaml_client_id

    SynapseDataSourceConnection(name="ds", connection_properties=parse(connection))

    pyodbc_connect.assert_called_once()
    assert pyodbc_connect.call_args.kwargs["attrs_before"] == {
        SQL_COPT_SS_ACCESS_TOKEN: pack_access_token(ACCESS_TOKEN)
    }
    assert pyodbc_connect.call_args.kwargs["autocommit"] is True
    assert credential_class.call_args.kwargs["client_id"] == expected_client_id
    credential_class.return_value.get_token.assert_called_once_with(AZURE_SQL_TOKEN_SCOPE)


@pytest.mark.parametrize(
    "connection, expected_type, expected_connection_string_suffix",
    [
        (
            {**CONNECTION, "user": "sa", "password": "Password1!"},
            SynapsePasswordAuth,
            "ConnectRetryCount=0;UID={sa};PWD={Password1!}",
        ),
        (
            {
                **CONNECTION,
                "authentication": "activedirectoryserviceprincipal",
                "client_id": "spn-id",
                "client_secret": "spn-secret",
            },
            SynapseActiveDirectoryServicePrincipalAuthentication,
            "ConnectRetryCount=0;Authentication=ActiveDirectoryServicePrincipal;UID={spn-id};PWD={spn-secret}",
        ),
    ],
)
def test_existing_authentication_variants_are_unchanged(
    connection, expected_type, expected_connection_string_suffix, credential_class, pyodbc_connect
) -> None:
    config = parse(connection)

    assert type(config) is expected_type
    assert expected_connection_string_suffix in SynapseDataSourceConnection.build_connection_string(config)

    SynapseDataSourceConnection(name="ds", connection_properties=config)

    assert pyodbc_connect.call_args.kwargs["attrs_before"] is None
    credential_class.assert_not_called()
