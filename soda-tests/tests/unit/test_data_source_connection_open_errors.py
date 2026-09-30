"""Regression: a failure to open a data source connection must surface the real reason.

Previously ``DataSourceConnection.open_connection`` swallowed the connect exception (logging it and
leaving ``self.connection = None``). The failure then re-surfaced far away as an inscrutable
``AttributeError: 'NoneType' object has no attribute 'cursor'`` inside ``execute_query`` — hiding the
actual cause (bad credentials, unreachable host, a dropped role, a statement timeout, ...).

open_connection now raises ``DataSourceConnectionException`` chaining the original error, mirroring
``DataSourceImpl.open_connection`` which already raised.
"""

import pytest
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.exceptions import DataSourceConnectionException


class _FailingConnection(DataSourceConnection):
    """A DataSourceConnection whose backend connect always fails."""

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        raise OSError("connection refused")


class _OkConnection(DataSourceConnection):
    """A DataSourceConnection whose backend connect succeeds with a stub DBAPI connection."""

    class _StubDbapi:
        def cursor(self):  # pragma: no cover - only proves the happy path opens
            raise AssertionError("cursor() should not be called in this test")

        def close(self):
            pass

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        return self._StubDbapi()


def test_open_connection_raises_typed_error_carrying_the_cause():
    # __init__ auto-opens, so construction is where the connect happens.
    with pytest.raises(DataSourceConnectionException) as exc_info:
        _FailingConnection(name="pg", connection_properties={"host": "db.example", "password": "secret"})

    # the typed error names the data source and chains the original cause (so the real reason survives)
    assert "pg" in str(exc_info.value)
    assert isinstance(exc_info.value.__cause__, OSError)
    assert "connection refused" in str(exc_info.value.__cause__)


def test_no_secret_from_properties_in_the_raised_message():
    # the message must not echo the connection properties (they can hold a password)
    with pytest.raises(DataSourceConnectionException) as exc_info:
        _FailingConnection(name="pg", connection_properties={"password": "s3cr3t-pw"})
    assert "s3cr3t-pw" not in str(exc_info.value)


def test_successful_open_sets_the_connection():
    conn = _OkConnection(name="pg", connection_properties={})
    assert isinstance(conn.connection, _OkConnection._StubDbapi)
