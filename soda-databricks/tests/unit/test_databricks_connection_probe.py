"""Tests for the Databricks pre-flight reachability probe in soda-core v4.

Mirrors the soda-library test file. See SCS-884 / OBSL-445.
"""

import socket
import ssl
from unittest.mock import MagicMock, patch

import pytest
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    _probe_databricks_reachability,
)


def test_probe_succeeds_when_tcp_and_tls_handshake_ok():
    """Happy path: TCP+TLS succeed, probe returns None."""
    mock_sock = MagicMock()
    mock_sock.__enter__.return_value = mock_sock
    mock_ctx = MagicMock()
    mock_ctx.wrap_socket.return_value.__enter__.return_value = MagicMock()

    with patch(
        "soda_databricks.common.data_sources.databricks_data_source_connection.socket.create_connection",
        return_value=mock_sock,
    ) as mock_conn, patch(
        "soda_databricks.common.data_sources.databricks_data_source_connection.ssl.create_default_context",
        return_value=mock_ctx,
    ):
        _probe_databricks_reachability("example.cloud.databricks.com")

    mock_conn.assert_called_once_with(("example.cloud.databricks.com", 443), timeout=10.0)
    mock_ctx.wrap_socket.assert_called_once()
    assert mock_ctx.wrap_socket.call_args.kwargs["server_hostname"] == "example.cloud.databricks.com"


@pytest.mark.parametrize(
    "raised",
    [
        socket.gaierror("Name or service not known"),
        socket.timeout("timed out"),
        ConnectionRefusedError("connection refused"),
        OSError("network unreachable"),
    ],
    ids=["dns_failure", "tcp_timeout", "tcp_refused", "network_unreachable"],
)
def test_probe_fails_fast_on_connect_phase_errors(raised):
    with patch(
        "soda_databricks.common.data_sources.databricks_data_source_connection.socket.create_connection",
        side_effect=raised,
    ):
        with pytest.raises(ConnectionError):
            _probe_databricks_reachability("bad-host.example.com")


def test_probe_fails_fast_on_tls_handshake_error():
    mock_sock = MagicMock()
    mock_sock.__enter__.return_value = mock_sock
    mock_ctx = MagicMock()
    mock_ctx.wrap_socket.side_effect = ssl.SSLError("bad certificate")

    with patch(
        "soda_databricks.common.data_sources.databricks_data_source_connection.socket.create_connection",
        return_value=mock_sock,
    ), patch(
        "soda_databricks.common.data_sources.databricks_data_source_connection.ssl.create_default_context",
        return_value=mock_ctx,
    ):
        with pytest.raises(ConnectionError):
            _probe_databricks_reachability("example.cloud.databricks.com")
