"""Unit tests for DataSourceTestHelper's monkey-patch of
DataSourceImpl.create_additional_connection.

The patch wraps the returned DataSourceConnection in a secondary
SnapshotDataSourceConnection (linked to the primary's recording stream) when
the primary connection is itself a SnapshotDataSourceConnection. Otherwise it
passes through to the original implementation.

These tests verify the install/uninstall lifecycle and the patched function's
branching without involving any real datasource.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.snapshot_connection import SnapshotDataSourceConnection
from helpers.snapshot_manager import SnapshotManager
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl

# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def reset_patch_state():
    """Save and restore the class-level patch state around each test so they
    can't leak monkey-patches into each other."""
    orig_refs = DataSourceTestHelper._create_additional_connection_patch_refs
    orig_func = DataSourceTestHelper._orig_create_additional_connection
    orig_method = DataSourceImpl.create_additional_connection
    yield
    DataSourceTestHelper._create_additional_connection_patch_refs = orig_refs
    DataSourceTestHelper._orig_create_additional_connection = orig_func
    DataSourceImpl.create_additional_connection = orig_method


class _FakeDbapi:
    """Minimal DBAPI conn for SnapshotDataSourceConnection's `_real`."""

    def cursor(self) -> Any:
        return MagicMock()

    def commit(self) -> None:
        pass


class _FakeReal(DataSourceConnection):
    """Minimal real DataSourceConnection — does not open a real DB connection."""

    def __init__(self) -> None:
        # Bypass DataSourceConnection.__init__ entirely; we only need a few attrs.
        self.connection = _FakeDbapi()
        self.connection_properties: dict = {"db": "fake"}

    def _create_connection(self, connection_yaml_dict: dict):  # pragma: no cover - unused
        return None


def _make_primary_snapshot(tmp_path, *, mode: str) -> SnapshotDataSourceConnection:
    manager = SnapshotManager(datasource_type="unit", snapshot_dir=str(tmp_path))
    if mode == "record":
        return SnapshotDataSourceConnection(
            real_connection=_FakeReal(),
            snapshot_manager=manager,
            mode="record",
        )
    return SnapshotDataSourceConnection(
        real_connection=None,
        snapshot_manager=manager,
        mode="replay",
    )


# ---------------------------------------------------------------------------
# Install / uninstall lifecycle
# ---------------------------------------------------------------------------


def test_install_replaces_method_and_uninstall_restores(reset_patch_state) -> None:
    original = DataSourceImpl.create_additional_connection

    DataSourceTestHelper._install_create_additional_connection_patch()
    assert DataSourceImpl.create_additional_connection is not original
    assert DataSourceTestHelper._create_additional_connection_patch_refs == 1

    DataSourceTestHelper._uninstall_create_additional_connection_patch()
    assert DataSourceImpl.create_additional_connection is original
    assert DataSourceTestHelper._create_additional_connection_patch_refs == 0


def test_install_is_idempotent_under_ref_counting(reset_patch_state) -> None:
    original = DataSourceImpl.create_additional_connection

    DataSourceTestHelper._install_create_additional_connection_patch()
    patched_after_first = DataSourceImpl.create_additional_connection
    DataSourceTestHelper._install_create_additional_connection_patch()
    # Still the same patched function — not double-wrapped.
    assert DataSourceImpl.create_additional_connection is patched_after_first
    assert DataSourceTestHelper._create_additional_connection_patch_refs == 2

    # First uninstall: still patched (one ref left).
    DataSourceTestHelper._uninstall_create_additional_connection_patch()
    assert DataSourceImpl.create_additional_connection is patched_after_first
    assert DataSourceTestHelper._create_additional_connection_patch_refs == 1

    # Second uninstall: restored.
    DataSourceTestHelper._uninstall_create_additional_connection_patch()
    assert DataSourceImpl.create_additional_connection is original
    assert DataSourceTestHelper._create_additional_connection_patch_refs == 0


def test_extra_uninstall_is_a_no_op(reset_patch_state) -> None:
    """Calling _uninstall_* when no install is active must not blow up."""
    DataSourceTestHelper._uninstall_create_additional_connection_patch()
    assert DataSourceTestHelper._create_additional_connection_patch_refs == 0


# ---------------------------------------------------------------------------
# Patched function behaviour
# ---------------------------------------------------------------------------


def _bind(patched_method, impl: MagicMock):
    """Bind the (now patched) DataSourceImpl.create_additional_connection to a mock."""
    return patched_method.__get__(impl)


def test_patched_passes_through_when_primary_is_not_a_snapshot(reset_patch_state) -> None:
    DataSourceTestHelper._install_create_additional_connection_patch()
    patched = DataSourceImpl.create_additional_connection

    extra = MagicMock(spec=DataSourceConnection, name="extra")
    plain_primary = MagicMock(spec=DataSourceConnection, name="primary")
    impl = MagicMock(spec=DataSourceImpl, data_source_connection=plain_primary)
    impl._create_data_source_connection.return_value = extra

    result = _bind(patched, impl)()

    assert result is extra
    impl._create_data_source_connection.assert_called_once_with()


def test_patched_wraps_in_record_mode_with_real_connection(reset_patch_state, tmp_path) -> None:
    DataSourceTestHelper._install_create_additional_connection_patch()
    patched = DataSourceImpl.create_additional_connection

    primary = _make_primary_snapshot(tmp_path, mode="record")
    extra_real = _FakeReal()
    impl = MagicMock(spec=DataSourceImpl, data_source_connection=primary)
    impl._create_data_source_connection.return_value = extra_real

    result = _bind(patched, impl)()

    assert isinstance(result, SnapshotDataSourceConnection)
    assert result is not primary
    assert result._primary_snapshot is primary
    assert result._mode == "record"
    assert result._real is extra_real
    assert result.connection_properties == primary.connection_properties
    impl._create_data_source_connection.assert_called_once_with()


def test_patched_wraps_in_replay_mode_without_opening_real_connection(reset_patch_state, tmp_path) -> None:
    DataSourceTestHelper._install_create_additional_connection_patch()
    patched = DataSourceImpl.create_additional_connection

    primary = _make_primary_snapshot(tmp_path, mode="replay")
    impl = MagicMock(spec=DataSourceImpl, data_source_connection=primary)

    result = _bind(patched, impl)()

    assert isinstance(result, SnapshotDataSourceConnection)
    assert result._primary_snapshot is primary
    assert result._mode == "replay"
    assert result._real is None
    # No real DBAPI connection was opened.
    impl._create_data_source_connection.assert_not_called()
    # But the lazy factory is in place for when fallback eventually fires.
    assert result._fallback_connection_factory is not None


def test_replay_mode_lazy_factory_calls_original_on_demand(reset_patch_state, tmp_path) -> None:
    DataSourceTestHelper._install_create_additional_connection_patch()
    patched = DataSourceImpl.create_additional_connection

    primary = _make_primary_snapshot(tmp_path, mode="replay")
    lazy_real = _FakeReal()
    impl = MagicMock(spec=DataSourceImpl, data_source_connection=primary)
    impl._create_data_source_connection.return_value = lazy_real

    secondary = _bind(patched, impl)()
    # Pre-condition: lazy factory hasn't fired yet.
    impl._create_data_source_connection.assert_not_called()

    produced = secondary._fallback_connection_factory()

    assert produced is lazy_real
    impl._create_data_source_connection.assert_called_once_with()
