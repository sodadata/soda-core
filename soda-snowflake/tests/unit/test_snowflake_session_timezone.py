"""Unit tests for ``SnowflakeDataSourceConnection._fetch_session_timezone``.

The Snowflake adapter previously fell back to a hard-coded magic index (``1``) when
``SHOW PARAMETERS`` returned a row whose columns did not include ``value``. That
worked under today's driver layout but would silently keep working — at the wrong
column — if a future driver/server upgrade reshaped the response. After this
change, the magic-index path is replaced with a logged UTC fallback so a real
schema drift surfaces in operator logs instead of being hidden behind a coincidence.
"""

from __future__ import annotations

import logging
from datetime import timezone
from unittest.mock import MagicMock

from soda_core.common.logs import Logs
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSourceConnection,
)


def _make_connection(rows, description):
    instance = SnowflakeDataSourceConnection.__new__(SnowflakeDataSourceConnection)
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.description = description
    cursor.execute.return_value = None
    cursor.close.return_value = None
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=None)

    snow_conn = MagicMock()
    snow_conn.cursor.return_value = cursor
    instance.connection = snow_conn
    return instance


class TestSnowflakeSessionTimezoneNoValueColumn:
    def test_no_rows_returns_utc_silently(self) -> None:
        instance = _make_connection(rows=[], description=[("key",), ("value",)])

        result = instance._fetch_session_timezone()

        assert result is timezone.utc

    def test_value_column_present_parses_as_normal(self) -> None:
        instance = _make_connection(
            rows=[("TIMEZONE", "America/Los_Angeles", "America/Los_Angeles", "ACCOUNT", "")],
            description=[("key",), ("value",), ("default",), ("level",), ("description",)],
        )

        result = instance._fetch_session_timezone()

        # Returned tzinfo must produce the expected offset for an LA-summer instant.
        from datetime import datetime

        ldt = datetime(2024, 7, 1, 12, 0, 0)
        assert result.utcoffset(ldt).total_seconds() == -7 * 3600  # PDT

    def test_missing_value_column_logs_warning_and_falls_back_to_utc(self) -> None:
        # Simulate a future Snowflake driver/server reshape: rows exist, but no
        # column is named "value". The pre-fix code would silently fall back to
        # ``rows[0][1]`` which would happen to be the right column today but would
        # silently break when the layout changes — defeat the purpose of having a
        # parser at all.
        instance = _make_connection(
            rows=[("TIMEZONE", "America/Los_Angeles", "ACCOUNT")],
            description=[("name",), ("setting",), ("source",)],
        )

        captured = Logs()
        try:
            result = instance._fetch_session_timezone()
        finally:
            captured.remove_from_root_logger()

        assert result is timezone.utc
        warnings = [r for r in captured.get_log_records() if r.levelno >= logging.WARNING]
        assert any(
            "SHOW PARAMETERS" in r.getMessage() and "value" in r.getMessage() for r in warnings
        ), f"Expected a warning naming SHOW PARAMETERS and 'value', got: {[r.getMessage() for r in warnings]}"
