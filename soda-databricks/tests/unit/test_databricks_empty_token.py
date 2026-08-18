"""Unit tests: an empty or absent Databricks token must fail loudly and must never reach
``databricks.sql.connect``.

Background: ``databricks-sql-connector`` treats a falsy ``access_token`` (``""``/``None``) with
no ``credentials_provider`` as "no auth configured" and falls back to its INTERACTIVE browser
OAuth (U2M) flow — it opens a browser URL and blocks on ``localhost:8020`` waiting for the
callback. In a headless run (agent pod, CI) that is an infinite hang with no warehouse
queries. Typical way to get there: ``access_token: ${env.DATABRICKS_TOKEN}`` with the env
var unset (or set to an empty value) where the verification runs.

No live database; ``databricks.sql.connect`` is mocked.
"""

from unittest.mock import patch

import pytest
from pydantic import SecretStr
from soda_databricks.common.data_sources import (
    databricks_data_source_connection as conn_mod,
)
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    DatabricksDataSourceConnection,
)
from soda_databricks.model.data_source.databricks_connection_properties import (
    DatabricksTokenAuth,
)
from soda_databricks.model.data_source.databricks_data_source import (
    DatabricksDataSource,
)

infer = DatabricksDataSource.infer_connection_type

BASE = {"host": "abc.cloud.databricks.com", "http_path": "/sql/1.0/endpoints/abc"}


def _make_connection() -> DatabricksDataSourceConnection:
    return DatabricksDataSourceConnection.__new__(DatabricksDataSourceConnection)


# --------------------------------------------------------------------------- model / dispatch


@pytest.mark.parametrize("token", ["", None])
def test_empty_access_token_rejected_at_parse_time(token):
    """``access_token`` present but empty (e.g. an unresolved variable reference) is rejected
    by the model (``min_length=1``), not accepted as an empty secret."""
    with pytest.raises(ValueError, match="access_token"):
        infer({**BASE, "access_token": token})


@pytest.mark.parametrize("token", ["", None])
def test_empty_access_token_with_explicit_auth_type_rejected(token):
    with pytest.raises(ValueError, match="access_token"):
        infer({**BASE, "auth_type": "personal-access-token", "access_token": token})


# ------------------------------------------------------------------------ connection guard


def test_connection_refuses_to_connect_with_empty_token():
    """Last line of defence: never call sql.connect() without a credentials_provider or a
    non-empty access_token — the connector's fallback is an interactive browser login."""
    props = DatabricksTokenAuth.model_construct(**BASE, access_token=SecretStr(""))
    with patch.object(conn_mod.sql, "connect") as mock_connect:
        with pytest.raises(ValueError, match="interactive"):
            _make_connection()._create_connection(props)
    mock_connect.assert_not_called()


def test_connection_refuses_to_connect_with_missing_token():
    props = DatabricksTokenAuth.model_construct(**BASE, access_token=None)
    with patch.object(conn_mod.sql, "connect") as mock_connect:
        with pytest.raises(ValueError, match="interactive"):
            _make_connection()._create_connection(props)
    mock_connect.assert_not_called()


def test_connection_still_connects_with_token():
    props = infer({**BASE, "access_token": "dapi123"})
    with patch.object(conn_mod.sql, "connect") as mock_connect:
        _make_connection()._create_connection(props)
    assert mock_connect.call_args.kwargs["access_token"] == "dapi123"
    assert "credentials_provider" not in mock_connect.call_args.kwargs
