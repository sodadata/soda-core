"""Unit coverage for the SparkDF connection-mode discriminator + active-session lookup.

These tests do not start a SparkSession against any cluster — they verify that:
  - the YAML-shaped dict routes to the right Pydantic properties class
  - ``_create_connection`` for active-session mode either returns
    ``SparkSession.getActiveSession()`` or raises a clear error when none is active.
"""
from __future__ import annotations

import pytest
from soda_sparkdf.common.data_sources.sparkdf_data_source import (
    SparkDataFrameDataSourceConnection,
)
from soda_sparkdf.common.data_sources.sparkdf_data_source_connection import (
    SparkDataFrameActiveSessionProperties,
    SparkDataFrameDataSource,
    SparkDataFrameRemoteSessionProperties,
)


def _build_model(connection_dict: dict) -> SparkDataFrameDataSource:
    return SparkDataFrameDataSource(name="x", connection=connection_dict)


def test_use_active_session_routes_to_active_properties():
    model = _build_model({"use_active_session": True, "use_catalog": True})
    assert isinstance(model.connection_properties, SparkDataFrameActiveSessionProperties)
    assert model.connection_properties.use_active_session is True
    assert model.connection_properties.use_catalog is True


def test_remote_session_config_routes_to_remote_properties():
    model = _build_model(
        {
            "host": "dbc-x.cloud.databricks.com",
            "token": "secret",
            "cluster_id": "0000-000000-aaaa",
            "use_catalog": True,
        }
    )
    assert isinstance(model.connection_properties, SparkDataFrameRemoteSessionProperties)


def test_conflicting_modes_raise_clear_error():
    # use_active_session + host used to be silently resolved to active by discriminator
    # order. Both being present is almost always a typo — we want an actionable error.
    with pytest.raises(ValueError, match="Conflicting SparkDataFrame connection config"):
        _build_model(
            {
                "use_active_session": True,
                "host": "dbc-x.cloud.databricks.com",
                "token": "secret",
                "cluster_id": "0000-000000-aaaa",
            }
        )


def test_remote_session_host_strips_scheme_and_trailing_slash():
    # Users naturally paste the workspace URL with scheme; the Spark Connect URI hard-codes
    # ``sc://...:443/`` so a scheme on host would yield ``sc://https://...:443/`` and a
    # confusing gRPC error. The model normalises the host on validation.
    props = SparkDataFrameRemoteSessionProperties(
        host="https://dbc-x.cloud.databricks.com/",
        token="secret",
        cluster_id="0000-000000-aaaa",
    )
    assert props.host == "dbc-x.cloud.databricks.com"


def test_remote_session_token_is_masked_in_repr_and_str():
    # soda-core logs connection_properties at DEBUG — if the token isn't a SecretStr,
    # it leaks verbatim into any verbose test or production log.
    props = SparkDataFrameRemoteSessionProperties(
        host="dbc-x.cloud.databricks.com",
        token="dapi_some_real_looking_secret",
        cluster_id="0000-000000-aaaa",
    )
    assert "dapi_some_real_looking_secret" not in repr(props)
    assert "dapi_some_real_looking_secret" not in str(props)
    assert "**********" in repr(props)
    # The actual value must still be retrievable inside the adapter when building URIs.
    assert props.token.get_secret_value() == "dapi_some_real_looking_secret"


def test_active_session_mode_raises_when_no_active_session(monkeypatch: pytest.MonkeyPatch):
    # Ensure SparkSession.getActiveSession() returns None for this test, regardless of
    # what other tests in the session have done.
    from pyspark.sql import SparkSession

    monkeypatch.setattr(SparkSession, "getActiveSession", staticmethod(lambda: None))

    conn = SparkDataFrameDataSourceConnection(
        name="x",
        connection_properties={"use_active_session": True},
    )
    props = SparkDataFrameActiveSessionProperties(use_active_session=True)
    with pytest.raises(ValueError, match="no active SparkSession was found"):
        conn._create_connection(props)
