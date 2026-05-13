"""Unit tests for ``SqlServerDataSourceConnection._fetch_session_timezone``.

The SQL Server adapter previously returned the raw ``pyodbc``-constructed tzinfo from
``SYSDATETIMEOFFSET()``, which is a fixed-offset ``datetime.timezone(timedelta(...))``
instance. Other adapters that report a UTC session through ``parse_session_timezone``
return ``timezone.utc`` (the singleton), so an ``is timezone.utc`` check would
disagree across vendors. After this change, SQL Server / Fabric / Synapse route
through the same parser, producing identity-equivalent UTC for zero-offset sessions
and ``timezone(timedelta(...))`` for non-zero offsets.

These tests stub the ``pyodbc`` cursor so they run without a live SQL Server.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSourceConnection,
)


def _make_connection(returned_offset_value):
    """Build a SqlServerDataSourceConnection stub whose cursor returns ``returned_offset_value``.

    Bypasses ``__init__`` (which would try to open a live ODBC connection) and wires
    up only the attributes ``_fetch_session_timezone`` reaches.
    """
    instance = SqlServerDataSourceConnection.__new__(SqlServerDataSourceConnection)
    cursor = MagicMock()
    cursor.fetchone.return_value = (returned_offset_value,) if returned_offset_value is not None else None
    cursor.execute.return_value = None
    cursor.close.return_value = None
    # The adapter uses ``with self.connection.cursor() as cursor:`` so the context
    # manager protocol must return the same cursor mock we configured above.
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)

    pyodbc_conn = MagicMock()
    pyodbc_conn.cursor.return_value = cursor
    instance.connection = pyodbc_conn
    return instance, cursor


class TestSqlServerSessionTimezoneNormalization:
    def test_zero_offset_returns_timezone_utc_identity(self) -> None:
        # SYSDATETIMEOFFSET on a UTC-clocked server returns a tz-aware datetime with
        # ``timezone(timedelta(0))`` as its tzinfo. Without normalization that is
        # ``== timezone.utc`` but not ``is timezone.utc``. This test pins the identity
        # behavior we now guarantee for parity with Postgres/Snowflake/Databricks.
        utc_dt = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone(timedelta(0)))
        instance, _ = _make_connection(utc_dt)

        result = instance._fetch_session_timezone()

        assert result is timezone.utc

    @pytest.mark.parametrize(
        "offset_hours, offset_minutes",
        [
            (-8, 0),
            (5, 30),
            (1, 0),
            (-3, 30),
        ],
    )
    def test_non_zero_offset_returns_fixed_offset(self, offset_hours: int, offset_minutes: int) -> None:
        offset = timedelta(hours=offset_hours, minutes=offset_minutes)
        if offset_hours < 0 or (offset_hours == 0 and offset_minutes < 0):
            offset = -timedelta(hours=abs(offset_hours), minutes=abs(offset_minutes))
        dt = datetime(2024, 6, 15, 12, 0, 0, tzinfo=timezone(offset))
        instance, _ = _make_connection(dt)

        result = instance._fetch_session_timezone()

        # The value-mapper layer only needs ``utcoffset(dt) == expected``; identity
        # against ``timezone.utc`` would be wrong here.
        assert result.utcoffset(dt) == offset
        assert result is not timezone.utc

    def test_no_row_falls_back_to_utc(self) -> None:
        instance, _ = _make_connection(None)

        result = instance._fetch_session_timezone()

        assert result is timezone.utc

    def test_naive_datetime_falls_back_to_utc(self) -> None:
        # Defensive: should never happen in practice (the pyodbc converter attaches
        # tzinfo for SQL_TYPE -155), but the implementation explicitly guards this.
        naive = datetime(2024, 1, 1, 12, 0, 0)
        instance, _ = _make_connection(naive)

        result = instance._fetch_session_timezone()

        assert result is timezone.utc
