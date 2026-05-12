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


def test_secondary_in_fallback_opens_its_own_real_connection(tmp_path) -> None:
    """Regression: when the primary falls back to real DB mid-replay, a
    cursor on a *secondary* (linked via primary_snapshot) must open the
    secondary's own _real, not just rely on the primary's. Otherwise
    self._owner._real stays None and cursor() crashes with
    'NoneType' object has no attribute 'connection'."""
    # Pre-condition: primary is in replay mode with fallback already active
    # (mimicking the auto-record-on-missing-snapshot path that CI hits when a
    # new test has no cached snapshot yet).
    primary_real_dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=[(99,)], description=(("v", "int"),))])
    primary_real = _FakeRealConnection(primary_real_dbapi)
    primary = _make_replay_snapshot(tmp_path, fallback_real=primary_real)
    primary._fallback_active = True  # simulate active fallback on the primary stream
    primary._real = primary_real  # primary already opened its real

    # Secondary is created in replay mode with _real=None and a lazy factory
    # (this is exactly what the patched create_additional_connection produces).
    secondary_real_dbapi = _FakeDbapiConnection(cursors=[_FakeDbapiCursor(rows=[(7,)], description=(("v", "int"),))])
    secondary_real = _FakeRealConnection(secondary_real_dbapi)
    secondary = SnapshotDataSourceConnection(
        real_connection=None,
        snapshot_manager=primary._snapshot_manager,
        mode="replay",
        fallback_connection_factory=lambda: secondary_real,
        allow_fallback=True,
        primary_snapshot=primary,
    )
    assert secondary._real is None

    # Act: execute via the secondary's cursor. Without the fix this crashes
    # with AttributeError("'NoneType' object has no attribute 'connection'").
    secondary.connection.cursor().execute("SELECT v FROM t")

    # The secondary opened its OWN real connection lazily.
    assert secondary._real is secondary_real
    # And drove it once.
    assert len(secondary_real_dbapi.cursors_returned) == 1
    assert secondary_real_dbapi.cursors_returned[0].executed_sql == ["SELECT v FROM t"]


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


# ---------------------------------------------------------------------------
# _activate_fallback handling for cursor_execute entries
# ---------------------------------------------------------------------------


def test_activate_fallback_preserves_cursor_execute_entries_without_re_execution(tmp_path) -> None:
    """When fallback kicks in mid-replay, already-consumed cursor_execute entries
    must be preserved as-is (they're read-only and tied to live cursor state)."""
    # Record three ops: two queries (results come from _Real._results) and a
    # cursor_execute in between (served from the real DBAPI cursor).
    real_cur = _FakeDbapiCursor(rows=[(9,)], description=(("v", "int"),))
    dbapi = _FakeDbapiConnection(cursors=[real_cur])
    # We need both execute_query (via real_connection) and a cursor: build a
    # custom _FakeRealConnection where execute_query returns a QueryResult.
    from soda_core.common.data_source_results import QueryResult

    class _Real(_FakeRealConnection):
        def __init__(self, dbapi_):
            super().__init__(dbapi_)
            self._results = iter(
                [
                    QueryResult(rows=[(7,)], columns=(("v", "int"),)),
                    QueryResult(rows=[(8,)], columns=(("v", "int"),)),
                ]
            )

        def execute_query(self, sql, log_query=True):
            return next(self._results)

    real = _Real(dbapi)
    manager = SnapshotManager(datasource_type="unit", snapshot_dir=str(tmp_path))
    rec = SnapshotDataSourceConnection(
        real_connection=real,
        snapshot_manager=manager,
        mode="record",
    )
    rec.execute_query("SELECT 7")
    rec.connection.cursor().execute("SELECT v FROM cursor_path")
    rec.execute_query("SELECT 8")
    rec.finalize()

    # Replay; trigger a mismatch on op #2 (the second execute_query) so fallback runs.
    fallback_real = _Real(dbapi)
    rep = _make_replay_snapshot(tmp_path, fallback_real=fallback_real)
    # Consume op #0 normally.
    res0 = rep.execute_query("SELECT 7")
    assert res0.rows == [(7,)]
    # Consume op #1 (cursor_execute) — replayed from snapshot.
    cur = rep.connection.cursor()
    cur.execute("SELECT v FROM cursor_path")
    assert cur.fetchmany(1) == [(9,)]
    # Now force a mismatch on op #2 → triggers _activate_fallback.
    # The fallback path re-executes prior ops to rebuild the recording; the
    # cursor_execute at index 1 must be preserved verbatim, not re-executed.
    rep.execute_query("SELECT 99")  # mismatch vs recorded "SELECT 8"
    cursor_entries = [e for e in rep._recording if e.op_type == _SnapshotCursor._OP_TYPE]
    assert len(cursor_entries) == 1
    assert cursor_entries[0].sql == "SELECT v FROM cursor_path"
