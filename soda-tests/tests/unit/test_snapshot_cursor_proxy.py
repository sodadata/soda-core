"""Unit tests for the raw-cursor interception added to SnapshotDataSourceConnection.

Covers the _SnapshotDbapiProxy + _SnapshotCursor classes and the cursor_execute
op_type plumbing. Pure-Python, no real DB — uses a fake DBAPI connection/cursor
and tmp_path for SnapshotManager storage.
"""

from __future__ import annotations

import os
from typing import Any, Optional

import pytest
from helpers.snapshot_connection import (
    SnapshotDataSourceConnection,
    _SnapshotCursor,
    _SnapshotDbapiProxy,
)
from helpers.snapshot_manager import SnapshotManager, SnapshotMismatchError


@pytest.fixture(autouse=True)
def _disable_rerun(monkeypatch):
    """Opt these tests out of the plugin's rerun hook.

    These tests exercise the raw-cursor record/replay primitives and use
    ``pytest.raises(SnapshotMismatchError)`` to assert the wrapper's "raise
    on mismatch" contract. Strict mode short-circuits the rerun plugin so
    those assertions land cleanly.
    """
    monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "true")


# ---------------------------------------------------------------------------
# Fake DBAPI plumbing
# ---------------------------------------------------------------------------


class _FakeDbapiCursor:
    """Minimal DBAPI cursor stand-in. Returns the rows it was constructed with."""

    def __init__(self, rows: Optional[list[tuple]] = None, description: tuple = ()) -> None:
        self.rows: list[tuple] = list(rows) if rows is not None else []
        self.description = description
        self.executed_sql: list[str] = []
        self.closed: bool = False

    def execute(self, sql: str, params: Any = None) -> None:
        self.executed_sql.append(sql)

    def fetchall(self) -> list[tuple]:
        return list(self.rows)

    def close(self) -> None:
        self.closed = True


class _FakeDbapiConnection:
    """Returns a fresh _FakeDbapiCursor on every cursor() call."""

    def __init__(self, cursors: Optional[list[_FakeDbapiCursor]] = None) -> None:
        # If a list is provided, each cursor() call pops one; otherwise produce empty cursors.
        self._cursors_queue = list(cursors) if cursors else []
        self.cursors_returned: list[_FakeDbapiCursor] = []
        self.commits: int = 0

    def cursor(self) -> _FakeDbapiCursor:
        c = self._cursors_queue.pop(0) if self._cursors_queue else _FakeDbapiCursor()
        self.cursors_returned.append(c)
        return c

    def commit(self) -> None:
        self.commits += 1

    def close(self) -> None:
        """Intentional no-op: fake holds no real resources to release."""


class _FakeRealConnection:
    """Stand-in for a DataSourceConnection; the only surface _SnapshotCursor reads is .connection."""

    def __init__(self, dbapi: _FakeDbapiConnection) -> None:
        self.connection = dbapi
        self.connection_properties: dict = {}

    def close_connection(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record_snapshot(tmp_path, real_dbapi: _FakeDbapiConnection) -> SnapshotDataSourceConnection:
    manager = SnapshotManager(datasource_type="unit", snapshot_dir=str(tmp_path))
    real = _FakeRealConnection(real_dbapi)
    return SnapshotDataSourceConnection(
        real_connection=real,
        snapshot_manager=manager,
        mode="record",
    )


def _make_replay_snapshot(
    tmp_path,
    *,
    fallback_real: Optional[_FakeRealConnection] = None,
) -> SnapshotDataSourceConnection:
    manager = SnapshotManager(datasource_type="unit", snapshot_dir=str(tmp_path))
    return SnapshotDataSourceConnection(
        real_connection=None,
        snapshot_manager=manager,
        mode="replay",
        fallback_connection_factory=(lambda: fallback_real) if fallback_real is not None else None,
        allow_fallback=fallback_real is not None,
    )


# ---------------------------------------------------------------------------
# Proxy surface
# ---------------------------------------------------------------------------


def test_proxy_cursor_returns_snapshot_cursor(tmp_path) -> None:
    snap = _make_record_snapshot(tmp_path, _FakeDbapiConnection())
    proxy = snap.connection
    assert isinstance(proxy, _SnapshotDbapiProxy)
    cursor = proxy.cursor()
    assert isinstance(cursor, _SnapshotCursor)


@pytest.mark.parametrize("attr", ["commit", "rollback", "copy", "close", "autocommit"])
def test_proxy_raises_on_any_other_attribute(tmp_path, attr) -> None:
    snap = _make_record_snapshot(tmp_path, _FakeDbapiConnection())
    proxy = snap.connection
    with pytest.raises(AttributeError) as excinfo:
        getattr(proxy, attr)
    assert "_SnapshotDbapiProxy" in str(excinfo.value)
    assert attr in str(excinfo.value)


# ---------------------------------------------------------------------------
# Cursor surface — what is allowed
# ---------------------------------------------------------------------------


def test_cursor_execute_record_drains_real_and_records_one_entry(tmp_path) -> None:
    rows = [(1, "a"), (2, "b"), (3, "c")]
    real_cursor = _FakeDbapiCursor(rows=rows, description=(("id", "int"), ("name", "varchar")))
    dbapi = _FakeDbapiConnection(cursors=[real_cursor])
    snap = _make_record_snapshot(tmp_path, dbapi)

    cursor = snap.connection.cursor()
    cursor.execute("SELECT id, name FROM t")

    # Real cursor was driven exactly once and fully drained.
    assert real_cursor.executed_sql == ["SELECT id, name FROM t"]
    # One snapshot entry, op_type="cursor_execute", rows captured verbatim.
    assert len(snap._recording) == 1
    entry = snap._recording[0]
    assert entry.op_type == _SnapshotCursor._OP_TYPE
    assert entry.sql == "SELECT id, name FROM t"
    stored_rows, _stored_desc = entry.result
    assert stored_rows == rows


def test_cursor_fetchmany_serves_from_buffer_in_record_mode(tmp_path) -> None:
    rows = [(1,), (2,), (3,), (4,)]
    dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=rows)])
    snap = _make_record_snapshot(tmp_path, dbapi)

    cursor = snap.connection.cursor()
    cursor.execute("SELECT id FROM t")

    assert cursor.fetchmany(2) == [(1,), (2,)]
    assert cursor.fetchmany(2) == [(3,), (4,)]
    assert cursor.fetchmany(2) == []  # buffer exhausted


def test_cursor_fetchmany_default_arraysize_is_one(tmp_path) -> None:
    rows = [(1,), (2,)]
    dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=rows)])
    snap = _make_record_snapshot(tmp_path, dbapi)

    cursor = snap.connection.cursor()
    cursor.execute("SELECT id FROM t")

    assert cursor.fetchmany() == [(1,)]
    assert cursor.fetchmany() == [(2,)]
    assert cursor.fetchmany() == []


def test_cursor_description_exposes_normalized_description(tmp_path) -> None:
    real_cursor = _FakeDbapiCursor(rows=[(1,)], description=(("id", "int4"),))
    dbapi = _FakeDbapiConnection(cursors=[real_cursor])
    snap = _make_record_snapshot(tmp_path, dbapi)

    cursor = snap.connection.cursor()
    cursor.execute("SELECT id FROM t")

    # description is propagated from the real cursor (used by callers like rows_diff).
    assert cursor.description == real_cursor.description


def test_cursor_close_closes_real_cursor_and_is_idempotent(tmp_path) -> None:
    real_cursor = _FakeDbapiCursor()
    dbapi = _FakeDbapiConnection(cursors=[real_cursor])
    snap = _make_record_snapshot(tmp_path, dbapi)

    cursor = snap.connection.cursor()
    cursor.execute("SELECT 1")
    cursor.close()
    assert real_cursor.closed is True

    # Idempotent: second close() doesn't blow up and doesn't reopen anything.
    cursor.close()


# ---------------------------------------------------------------------------
# Cursor surface — what raises
# ---------------------------------------------------------------------------


def test_cursor_execute_rejects_params(tmp_path) -> None:
    dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor()])
    snap = _make_record_snapshot(tmp_path, dbapi)

    cursor = snap.connection.cursor()
    with pytest.raises(AttributeError) as excinfo:
        cursor.execute("SELECT 1", params=("x",))
    assert "parameterized execute" in str(excinfo.value)


@pytest.mark.parametrize(
    "attr",
    ["executemany", "fetchone", "fetchall", "rowcount", "copy", "scroll"],
)
def test_cursor_raises_on_any_other_attribute(tmp_path, attr) -> None:
    dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor()])
    snap = _make_record_snapshot(tmp_path, dbapi)

    cursor = snap.connection.cursor()
    with pytest.raises(AttributeError) as excinfo:
        getattr(cursor, attr)
    assert "_SnapshotCursor" in str(excinfo.value)
    assert attr in str(excinfo.value)


def test_cursor_does_not_support_context_manager(tmp_path) -> None:
    """`with snap.connection.cursor() as c:` must fail so existing fallback
    paths (e.g. _optimized_insert) keep tripping into the high-level API."""
    dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor()])
    snap = _make_record_snapshot(tmp_path, dbapi)
    cursor = snap.connection.cursor()

    # `with` looks up __enter__ on the type (bypasses __getattr__); _SnapshotCursor
    # doesn't define __enter__, so Python raises AttributeError or TypeError before
    # entering the body. The body is intentionally unreachable.
    with pytest.raises((AttributeError, TypeError)):
        with cursor:
            raise AssertionError("unreachable: __enter__ should have raised")


# ---------------------------------------------------------------------------
# Record → replay round-trip
# ---------------------------------------------------------------------------


def test_record_then_replay_round_trip_serves_rows_without_real_db(tmp_path) -> None:
    """Record one cursor_execute, then replay it from disk with _real=None."""
    rows = [(10, "x"), (20, "y")]
    real_cursor = _FakeDbapiCursor(rows=rows, description=(("id", "int"), ("v", "varchar")))
    dbapi = _FakeDbapiConnection(cursors=[real_cursor])

    rec = _make_record_snapshot(tmp_path, dbapi)
    cur = rec.connection.cursor()
    cur.execute("SELECT id, v FROM t ORDER BY id")
    cur.close()
    rec.finalize()  # persist the recording for the current PYTEST_CURRENT_TEST id

    # Sanity: the snapshot file exists.
    test_id = os.environ["PYTEST_CURRENT_TEST"].rsplit(" ", 1)[0]
    assert rec._snapshot_manager.has_snapshot(test_id)

    # Replay with no real connection at all.
    rep = _make_replay_snapshot(tmp_path)
    cur2 = rep.connection.cursor()
    cur2.execute("SELECT id, v FROM t ORDER BY id")  # matches the recorded entry
    assert cur2.fetchmany(2) == rows
    assert cur2.fetchmany(2) == []


def test_replay_mismatch_without_fallback_raises(tmp_path) -> None:
    """A different SQL on replay raises SnapshotMismatchError (fallback disabled)."""
    real_cursor = _FakeDbapiCursor(rows=[(1,)])
    dbapi = _FakeDbapiConnection(cursors=[real_cursor])
    rec = _make_record_snapshot(tmp_path, dbapi)
    rec.connection.cursor().execute("SELECT 1")
    rec.finalize()

    rep = _make_replay_snapshot(tmp_path)
    cur = rep.connection.cursor()
    with pytest.raises(SnapshotMismatchError):
        cur.execute("SELECT 2")  # different SQL


# ---------------------------------------------------------------------------
# primary_snapshot delegation
# ---------------------------------------------------------------------------


def _make_secondary(
    primary: SnapshotDataSourceConnection, real_dbapi: _FakeDbapiConnection
) -> SnapshotDataSourceConnection:
    return SnapshotDataSourceConnection(
        real_connection=_FakeRealConnection(real_dbapi),
        snapshot_manager=primary._snapshot_manager,
        mode=primary._mode,
        primary_snapshot=primary,
    )


def test_secondary_record_entries_land_in_primary_recording_stream(tmp_path) -> None:
    primary_dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=[(1,)])])
    secondary_dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=[(2,)])])

    primary = _make_record_snapshot(tmp_path, primary_dbapi)
    secondary = _make_secondary(primary, secondary_dbapi)

    primary.connection.cursor().execute("SELECT 1 FROM source")
    secondary.connection.cursor().execute("SELECT 2 FROM target")

    # Both entries are in the primary's single ordered stream.
    assert len(primary._recording) == 2
    assert primary._recording[0].sql == "SELECT 1 FROM source"
    assert primary._recording[1].sql == "SELECT 2 FROM target"
    # The secondary's own _recording is untouched.
    assert secondary._recording == []


def test_secondary_replay_pulls_from_primary_stream(tmp_path) -> None:
    # Record: primary + secondary executes interleaved, then persist via primary.
    primary_rows = [(1,)]
    secondary_rows = [(2,)]
    primary_dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=primary_rows)])
    secondary_dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=secondary_rows)])
    primary = _make_record_snapshot(tmp_path, primary_dbapi)
    secondary = _make_secondary(primary, secondary_dbapi)
    primary.connection.cursor().execute("SELECT 1 FROM source")
    secondary.connection.cursor().execute("SELECT 2 FROM target")
    primary.finalize()

    # Replay: both connections share the primary's loaded snapshot.
    primary_rep = _make_replay_snapshot(tmp_path)
    secondary_rep = SnapshotDataSourceConnection(
        real_connection=None,
        snapshot_manager=primary_rep._snapshot_manager,
        mode="replay",
        primary_snapshot=primary_rep,
    )
    p_cur = primary_rep.connection.cursor()
    s_cur = secondary_rep.connection.cursor()
    p_cur.execute("SELECT 1 FROM source")
    s_cur.execute("SELECT 2 FROM target")
    assert p_cur.fetchmany(1) == primary_rows
    assert s_cur.fetchmany(1) == secondary_rows
