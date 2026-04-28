"""Unit test for DataSourceImpl.create_additional_connection.

The method is a thin delegation to the existing abstract _create_data_source_connection.
We verify it (a) returns whatever the abstract hook returns, (b) is independent of
self.connection, and (c) calls the hook fresh on every invocation.

Uses MagicMock(spec=DataSourceImpl) bound to the real method via descriptor protocol —
no real data source needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl


def _bind_method(mock_impl: MagicMock):
    """Bind the real DataSourceImpl.create_additional_connection method to the mock."""
    return DataSourceImpl.create_additional_connection.__get__(mock_impl)


def test_create_additional_connection_returns_value_from_hook() -> None:
    primary = MagicMock(spec=DataSourceConnection)
    extra = MagicMock(spec=DataSourceConnection)

    impl = MagicMock(spec=DataSourceImpl, data_source_connection=primary)
    impl._create_data_source_connection.return_value = extra

    result = _bind_method(impl)()

    assert result is extra
    impl._create_data_source_connection.assert_called_once_with()


def test_create_additional_connection_is_independent_from_primary() -> None:
    """Verifies the method does not return self.connection (the primary)."""
    primary = MagicMock(spec=DataSourceConnection)
    extra = MagicMock(spec=DataSourceConnection)

    impl = MagicMock(spec=DataSourceImpl, data_source_connection=primary)
    impl._create_data_source_connection.return_value = extra

    result = _bind_method(impl)()

    assert result is not primary


def test_create_additional_connection_returns_fresh_each_call() -> None:
    """Each call delegates to _create_data_source_connection, returning a new object."""
    a = MagicMock(spec=DataSourceConnection, name="conn_a")
    b = MagicMock(spec=DataSourceConnection, name="conn_b")

    impl = MagicMock(spec=DataSourceImpl)
    impl._create_data_source_connection.side_effect = [a, b]

    bound = _bind_method(impl)
    first = bound()
    second = bound()

    assert first is a
    assert second is b
    assert first is not second
    assert impl._create_data_source_connection.call_count == 2
