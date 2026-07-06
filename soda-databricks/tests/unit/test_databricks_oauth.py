"""Unit tests for Databricks OAuth (M2M + Azure service principal) auth.

No live database. The SDK credentials-provider factories are mocked because building a
real provider performs OIDC endpoint discovery over the network.
"""

from unittest.mock import patch

import pytest
from soda_databricks.common.data_sources import (
    databricks_data_source_connection as conn_mod,
)
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    DatabricksDataSourceConnection,
)
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksAzureServicePrincipal,
    DatabricksOAuthM2M,
    DatabricksTokenAuth,
)
from soda_databricks.model.data_source.databricks_data_source import (
    DatabricksDataSource,
)

infer = DatabricksDataSource.infer_connection_type


# --------------------------------------------------------------------------- dispatch


def test_token_auth_inferred_without_auth_type():
    """Backward compatibility: existing PAT YAML has no auth_type."""
    props = infer(
        {
            "host": "https://abc.cloud.databricks.com",
            "http_path": "/sql/1.0/endpoints/abc",
            "access_token": "dapi123",
        }
    )
    assert isinstance(props, DatabricksTokenAuth)


def test_explicit_token_auth_type():
    props = infer(
        {
            "auth_type": "personal-access-token",
            "host": "abc.cloud.databricks.com",
            "http_path": "/x",
            "access_token": "dapi123",
        }
    )
    assert isinstance(props, DatabricksTokenAuth)


def test_oauth_m2m_dispatch():
    props = infer(
        {
            "auth_type": "databricks-oauth-m2m",
            "host": "abc.cloud.databricks.com",
            "http_path": "/x",
            "client_id": "cid",
            "client_secret": "sec",
        }
    )
    assert isinstance(props, DatabricksOAuthM2M)


def test_azure_service_principal_dispatch():
    props = infer(
        {
            "auth_type": "azure-service-principal",
            "host": "adb-123.azuredatabricks.net",
            "http_path": "/x",
            "azure_client_id": "aid",
            "azure_client_secret": "asec",
            "azure_tenant_id": "tid",
        }
    )
    assert isinstance(props, DatabricksAzureServicePrincipal)


def test_unknown_auth_type_raises():
    with pytest.raises(ValueError, match="Unknown Databricks auth_type"):
        infer({"auth_type": "nope", "host": "h", "http_path": "/x"})


def test_no_auth_type_no_access_token_raises():
    with pytest.raises(ValueError, match="Could not infer"):
        infer({"host": "h", "http_path": "/x"})


def test_m2m_missing_client_id_raises():
    with pytest.raises(ValueError):
        infer({"auth_type": "databricks-oauth-m2m", "host": "h", "http_path": "/x", "client_secret": "sec"})


# ------------------------------------------------------------- credential-field stripping


def test_m2m_credentials_not_in_connection_kwargs():
    props = infer(
        {
            "auth_type": "databricks-oauth-m2m",
            "host": "abc.cloud.databricks.com",
            "http_path": "/x",
            "client_id": "cid",
            "client_secret": "sec",
        }
    )
    kwargs = props.to_connection_kwargs()
    assert "client_id" not in kwargs
    assert "client_secret" not in kwargs
    assert kwargs["server_hostname"] == "abc.cloud.databricks.com"


def test_azure_credentials_not_in_connection_kwargs():
    props = infer(
        {
            "auth_type": "azure-service-principal",
            "host": "adb-123.azuredatabricks.net",
            "http_path": "/x",
            "azure_client_id": "aid",
            "azure_client_secret": "asec",
            "azure_tenant_id": "tid",
        }
    )
    kwargs = props.to_connection_kwargs()
    assert not any(k.startswith("azure_") for k in kwargs)


# --------------------------------------------------- connection layer builds the provider


def _make_connection():
    return DatabricksDataSourceConnection.__new__(DatabricksDataSourceConnection)


def test_m2m_connection_passes_credentials_provider():
    props = infer(
        {
            "auth_type": "databricks-oauth-m2m",
            "host": "abc.cloud.databricks.com",
            "http_path": "/x",
            "client_id": "cid",
            "client_secret": "sec",
        }
    )
    headers = {"Authorization": "Bearer tok"}
    with patch(
        "databricks.sdk.credentials_provider.oauth_service_principal", return_value=lambda: headers
    ) as osp, patch("databricks.sdk.core.Config") as cfg, patch.object(conn_mod.sql, "connect") as mock_connect:
        _make_connection()._create_connection(props)

    called_kwargs = mock_connect.call_args.kwargs
    # ExternalAuthProvider calls credentials_provider() once to get the header factory, then
    # calls that factory per request. Verify both levels resolve to a headers dict — the old
    # one-level shape would make provider() return the dict and provider()() raise TypeError.
    provider = called_kwargs["credentials_provider"]
    assert callable(provider)
    header_factory = provider()
    assert callable(header_factory)
    assert header_factory() == headers
    assert "client_id" not in called_kwargs and "client_secret" not in called_kwargs
    # SDK Config got a scheme-prefixed host built from the stripped server_hostname.
    assert cfg.call_args.kwargs["host"] == "https://abc.cloud.databricks.com"
    assert cfg.call_args.kwargs["client_id"] == "cid"
    assert cfg.call_args.kwargs["client_secret"] == "sec"
    osp.assert_called_once()


def test_azure_connection_passes_credentials_provider():
    props = infer(
        {
            "auth_type": "azure-service-principal",
            "host": "adb-123.azuredatabricks.net",
            "http_path": "/x",
            "azure_client_id": "aid",
            "azure_client_secret": "asec",
            "azure_tenant_id": "tid",
        }
    )
    headers = {"Authorization": "Bearer az"}
    with patch(
        "databricks.sdk.credentials_provider.azure_service_principal", return_value=lambda: headers
    ) as asp, patch("databricks.sdk.core.Config") as cfg, patch.object(conn_mod.sql, "connect") as mock_connect:
        _make_connection()._create_connection(props)

    called_kwargs = mock_connect.call_args.kwargs
    provider = called_kwargs["credentials_provider"]
    assert callable(provider)
    assert provider()() == headers
    assert cfg.call_args.kwargs["azure_tenant_id"] == "tid"
    assert cfg.call_args.kwargs["azure_client_secret"] == "asec"
    asp.assert_called_once()


def test_m2m_provider_none_raises_clear_error():
    """If the SDK yields no header factory (OIDC discovery failure), fail loudly rather than
    silently falling through to a credential-less PAT connect."""
    props = infer(
        {
            "auth_type": "databricks-oauth-m2m",
            "host": "abc.cloud.databricks.com",
            "http_path": "/x",
            "client_id": "cid",
            "client_secret": "sec",
        }
    )
    with patch("databricks.sdk.credentials_provider.oauth_service_principal", return_value=None), patch(
        "databricks.sdk.core.Config"
    ), patch.object(conn_mod.sql, "connect") as mock_connect:
        with pytest.raises(ValueError, match="OAuth .M2M. authentication setup failed"):
            _make_connection()._create_connection(props)
    mock_connect.assert_not_called()


def test_token_connection_has_no_credentials_provider():
    props = infer(
        {
            "host": "abc.cloud.databricks.com",
            "http_path": "/x",
            "access_token": "dapi123",
        }
    )
    with patch.object(conn_mod.sql, "connect") as mock_connect:
        _make_connection()._create_connection(props)

    called_kwargs = mock_connect.call_args.kwargs
    assert "credentials_provider" not in called_kwargs
    assert called_kwargs["access_token"] == "dapi123"


def test_m2m_connection_strips_host_scheme():
    """A pasted https:// host must reach sql.connect scheme-less and the SDK Config with a
    single scheme — never a doubled https://https://."""
    props = infer(
        {
            "auth_type": "databricks-oauth-m2m",
            "host": "https://abc.cloud.databricks.com",
            "http_path": "/x",
            "client_id": "cid",
            "client_secret": "sec",
        }
    )
    with patch("databricks.sdk.credentials_provider.oauth_service_principal", return_value="M2M"), patch(
        "databricks.sdk.core.Config"
    ) as cfg, patch.object(conn_mod.sql, "connect") as mock_connect:
        _make_connection()._create_connection(props)

    assert mock_connect.call_args.kwargs["server_hostname"] == "abc.cloud.databricks.com"
    assert cfg.call_args.kwargs["host"] == "https://abc.cloud.databricks.com"
