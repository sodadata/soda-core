"""Unit test for DataSourceConnection.create_additional_dbapi_connection.

Uses a minimal in-process subclass of DataSourceConnection whose _create_connection
returns unique sentinel objects. Verifies that:
  - the method exists on the base class,
  - each invocation delegates to _create_connection with the stored connection_properties,
  - each invocation returns a fresh object distinct from self.connection.
"""

from __future__ import annotations

from soda_core.common.data_source_connection import DataSourceConnection


class _FakeDBAPIConnection:
    """A sentinel object that counts close() calls so we can verify lifecycle."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.closed = False

    def close(self) -> None:
        self.closed = True


class _FakeDataSourceConnection(DataSourceConnection):
    """Minimal concrete DataSourceConnection for unit-testing the base class.

    Each call to _create_connection produces a fresh _FakeDBAPIConnection whose
    label records (a) the connection_properties it was given, (b) a monotonically
    increasing sequence so callers can distinguish first vs. subsequent opens.
    """

    def __init__(self, connection_properties: dict) -> None:
        self._open_count = 0
        super().__init__(name="fake", connection_properties=connection_properties)

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        self._open_count += 1
        return _FakeDBAPIConnection(label=f"{connection_yaml_dict.get('tag')}#{self._open_count}")


def test_create_additional_dbapi_connection_returns_fresh_connection() -> None:
    props = {"tag": "t"}
    conn = _FakeDataSourceConnection(connection_properties=props)
    assert isinstance(conn.connection, _FakeDBAPIConnection)
    primary = conn.connection

    additional = conn.create_additional_dbapi_connection()

    assert isinstance(additional, _FakeDBAPIConnection)
    assert additional is not primary
    assert additional.label == "t#2"  # second creation (primary opened during __init__)


def test_create_additional_dbapi_connection_uses_stored_properties() -> None:
    props = {"tag": "alpha"}
    conn = _FakeDataSourceConnection(connection_properties=props)

    additional = conn.create_additional_dbapi_connection()

    assert additional.label.startswith("alpha#")


def test_create_additional_dbapi_connection_does_not_affect_primary_on_close() -> None:
    conn = _FakeDataSourceConnection(connection_properties={"tag": "t"})
    primary = conn.connection

    additional = conn.create_additional_dbapi_connection()
    additional.close()

    assert additional.closed is True
    assert primary.closed is False
    assert conn.connection is primary
