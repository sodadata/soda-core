"""Tests for the snapshot recording/replay system."""

from __future__ import annotations

import json
import os
import pickle
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from helpers.snapshot_connection import (
    FakeCursor,
    PicklableColumn,
    SnapshotDataSourceConnection,
)
from helpers.snapshot_manager import (
    SnapshotEntry,
    SnapshotManager,
    SnapshotMismatchError,
    SnapshotNotFoundError,
)
from soda_core.common.data_source_results import QueryResult, QueryResultIterator


# ---------------------------------------------------------------------------
# PicklableColumn
# ---------------------------------------------------------------------------


class TestPicklableColumn:
    def test_attribute_access(self):
        col = PicklableColumn("id", 23, None, None, None, None, None)
        assert col.name == "id"
        assert col.type_code == 23

    def test_index_access(self):
        col = PicklableColumn("id", 23, None, None, None, None, None)
        assert col[0] == "id"
        assert col[1] == 23

    def test_picklable(self):
        col = PicklableColumn("name", 1043, 10, 20, 5, 2, True)
        restored = pickle.loads(pickle.dumps(col))
        assert restored == col
        assert restored.name == "name"


# ---------------------------------------------------------------------------
# FakeCursor
# ---------------------------------------------------------------------------


class TestFakeCursor:
    def test_fetchone_returns_rows_then_none(self):
        cursor = FakeCursor([(1,), (2,), (3,)], description=None, rowcount=3)
        assert cursor.fetchone() == (1,)
        assert cursor.fetchone() == (2,)
        assert cursor.fetchone() == (3,)
        assert cursor.fetchone() is None

    def test_fetchall_returns_remaining(self):
        cursor = FakeCursor([(1,), (2,), (3,)], description=None, rowcount=3)
        cursor.fetchone()  # consume first
        assert cursor.fetchall() == [(2,), (3,)]
        assert cursor.fetchall() == []

    def test_description_and_rowcount(self):
        desc = (PicklableColumn("x", 23, None, None, None, None, None),)
        cursor = FakeCursor([(1,)], description=desc, rowcount=1)
        assert cursor.description == desc
        assert cursor.rowcount == 1

    def test_close_is_noop(self):
        cursor = FakeCursor([], description=None, rowcount=0)
        cursor.close()  # should not raise

    def test_works_with_query_result_iterator(self):
        rows = [(10,), (20,)]
        desc = (PicklableColumn("val", 23, None, None, None, None, None),)
        cursor = FakeCursor(rows, description=desc, rowcount=2)
        iterator = QueryResultIterator(cursor)
        assert list(iterator) == [(10,), (20,)]
        assert iterator.row_count == 2


# ---------------------------------------------------------------------------
# SnapshotManager
# ---------------------------------------------------------------------------


class TestSnapshotManager:
    @pytest.fixture
    def tmp_snapshot_dir(self, tmp_path):
        return str(tmp_path / "snapshots")

    @pytest.fixture
    def manager(self, tmp_snapshot_dir):
        return SnapshotManager(datasource_type="postgres", snapshot_dir=tmp_snapshot_dir)

    def test_snapshot_path_generation(self, manager):
        path = manager._snapshot_path("tests/integration/test_foo.py::test_bar", "pickle")
        assert path.endswith(os.path.join("postgres", "tests", "integration", "test_foo.py", "test_bar.pickle"))

    def test_save_and_load_roundtrip(self, manager):
        test_id = "tests/test_example.py::test_one"
        entries = [
            SnapshotEntry("query", "SELECT 1", QueryResult(rows=[(1,)], columns=None)),
            SnapshotEntry("update", "INSERT INTO t VALUES (1)", None),
        ]
        manager.save(test_id, entries)

        loaded = manager.load(test_id)
        assert loaded is not None
        assert len(loaded) == 2
        assert loaded[0].op_type == "query"
        assert loaded[0].sql == "SELECT 1"
        assert loaded[0].result.rows == [(1,)]
        assert loaded[1].op_type == "update"
        assert loaded[1].result is None

    def test_save_creates_json_sidecar(self, manager, tmp_snapshot_dir):
        test_id = "tests/test_example.py::test_json"
        entries = [SnapshotEntry("query", "SELECT 42", None)]
        manager.save(test_id, entries)

        json_path = manager._snapshot_path(test_id, "json")
        assert os.path.exists(json_path)
        with open(json_path) as f:
            data = json.load(f)
        assert data["datasource_type"] == "postgres"
        assert data["test_id"] == test_id
        assert data["operation_count"] == 1
        assert data["operations"][0]["sql"] == "SELECT 42"

    def test_load_returns_none_for_missing(self, manager):
        assert manager.load("nonexistent::test") is None

    def test_has_snapshot(self, manager):
        test_id = "tests/test_example.py::test_exists"
        assert not manager.has_snapshot(test_id)
        manager.save(test_id, [SnapshotEntry("query", "SELECT 1", None)])
        assert manager.has_snapshot(test_id)


# ---------------------------------------------------------------------------
# Normalization helpers
# ---------------------------------------------------------------------------


class TestNormalization:
    def test_normalize_description_none(self):
        assert SnapshotDataSourceConnection._normalize_description(None) is None

    def test_normalize_description_converts_to_picklable(self):
        # Simulate a DB driver description tuple
        raw = [("col1", 23, None, None, None, None, None), ("col2", 1043, 10, 20, 5, 2, True)]
        result = SnapshotDataSourceConnection._normalize_description(raw)
        assert len(result) == 2
        assert isinstance(result[0], PicklableColumn)
        assert result[0].name == "col1"
        assert result[1].null_ok is True

    def test_normalize_description_short_tuples(self):
        # Some drivers provide fewer than 7 elements
        raw = [("col1", 23)]
        result = SnapshotDataSourceConnection._normalize_description(raw)
        assert result[0].name == "col1"
        assert result[0].type_code == 23
        assert result[0].display_size is None

    def test_normalize_rows(self):
        # Simulate non-tuple row objects
        class FakeRow:
            def __init__(self, *args):
                self._data = args

            def __iter__(self):
                return iter(self._data)

        rows = [FakeRow(1, "a"), FakeRow(2, "b")]
        result = SnapshotDataSourceConnection._normalize_rows(rows)
        assert result == [(1, "a"), (2, "b")]
        assert all(type(r) is tuple for r in result)

    def test_normalize_query_result(self):
        qr = QueryResult(rows=[(1, "x"), (2, "y")], columns=[("id", 23, None, None, None, None, None)])
        result = SnapshotDataSourceConnection._normalize_query_result(qr)
        assert result.rows == [(1, "x"), (2, "y")]
        assert isinstance(result.columns[0], PicklableColumn)
        # Verify the normalized result is picklable
        restored = pickle.loads(pickle.dumps(result))
        assert restored.rows == [(1, "x"), (2, "y")]


# ---------------------------------------------------------------------------
# SnapshotDataSourceConnection — record and replay
# ---------------------------------------------------------------------------


def _make_mock_connection():
    """Create a mock DataSourceConnection with a real .connection attribute."""
    mock = MagicMock()
    mock.connection = object()  # non-None to satisfy has_open_connection
    return mock


class TestSnapshotConnectionRecord:
    @pytest.fixture
    def setup(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        return conn, real_conn, manager

    def test_execute_query_records(self, setup):
        conn, real_conn, manager = setup
        real_conn.execute_query.return_value = QueryResult(
            rows=[(42,)], columns=[("count", 23, None, None, None, None, None)]
        )

        test_id = "tests/test_x.py::test_record_query"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT COUNT(*)")

        assert result.rows == [(42,)]
        real_conn.execute_query.assert_called_once_with("SELECT COUNT(*)", log_query=True)

        # Finalize and check that snapshot was saved
        conn.finalize()
        loaded = manager.load(test_id)
        assert len(loaded) == 1
        assert loaded[0].op_type == "query"
        assert loaded[0].sql == "SELECT COUNT(*)"
        assert loaded[0].result.rows == [(42,)]

    def test_execute_update_records(self, setup):
        conn, real_conn, manager = setup
        real_conn.execute_update.return_value = None

        test_id = "tests/test_x.py::test_record_update"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_update("INSERT INTO t VALUES (1)")

        conn.finalize()
        loaded = manager.load(test_id)
        assert len(loaded) == 1
        assert loaded[0].op_type == "update"
        assert loaded[0].result is None

    def test_session_level_sql_passes_through(self, setup):
        conn, real_conn, _ = setup
        real_conn.execute_query.return_value = QueryResult(rows=[], columns=None)

        # No PYTEST_CURRENT_TEST set — session-level SQL
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            result = conn.execute_query("CREATE SCHEMA test_schema")

        assert result.rows == []
        # Should NOT have been recorded
        conn.finalize()


class TestSnapshotConnectionReplay:
    @pytest.fixture
    def setup(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_replay"

        # Pre-save a snapshot
        entries = [
            SnapshotEntry(
                "query",
                "SELECT COUNT(*)",
                QueryResult(rows=[(99,)], columns=(PicklableColumn("count", 23, None, None, None, None, None),)),
            ),
            SnapshotEntry("update", "INSERT INTO t VALUES (1)", None),
        ]
        manager.save(test_id, entries)

        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        return conn, manager, test_id

    def test_execute_query_replays(self, setup):
        conn, _, test_id = setup
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT COUNT(*)")
        assert result.rows == [(99,)]
        assert result.columns[0].name == "count"

    def test_execute_update_replays(self, setup):
        conn, _, test_id = setup
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT COUNT(*)")  # consume first entry
            result = conn.execute_update("INSERT INTO t VALUES (1)")
        assert result is None

    def test_sql_mismatch_raises_by_default(self, setup):
        conn, _, test_id = setup
        # Fallback is disabled by default → mismatch raises SnapshotMismatchError
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with pytest.raises(SnapshotMismatchError, match="fallback is disabled"):
                conn.execute_query("SELECT WRONG_SQL")

    def test_snapshot_exhaustion_raises_by_default(self, setup):
        conn, _, test_id = setup
        # Fallback is disabled by default → exhaustion raises SnapshotMismatchError
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT COUNT(*)")
            conn.execute_update("INSERT INTO t VALUES (1)")
            with pytest.raises(SnapshotMismatchError, match="fallback is disabled"):
                conn.execute_query("SELECT extra")

    def test_missing_snapshot_raises(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "tests/test_missing.py::test_gone (call)"}):
            with pytest.raises(SnapshotNotFoundError, match="No snapshot found"):
                conn.execute_query("SELECT 1")


class TestSnapshotConnectionQueryIterate:
    def test_record_and_replay_iterate(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_iterate"

        # --- Record phase ---
        real_conn = _make_mock_connection()
        desc = (PicklableColumn("val", 23, None, None, None, None, None),)
        fake_cursor = FakeCursor([(1,), (2,), (3,)], description=desc, rowcount=3)
        real_iter = QueryResultIterator(fake_cursor)

        # Mock execute_query_iterate as a context manager
        from contextlib import contextmanager

        @contextmanager
        def mock_iterate(sql, log_query=True):
            yield real_iter

        real_conn.execute_query_iterate = mock_iterate

        record_conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with record_conn.execute_query_iterate("SELECT val FROM t") as it:
                rows = list(it)
        assert rows == [(1,), (2,), (3,)]
        record_conn.finalize()

        # --- Replay phase ---
        replay_conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with replay_conn.execute_query_iterate("SELECT val FROM t") as it:
                replayed_rows = list(it)
        assert replayed_rows == [(1,), (2,), (3,)]


class TestSnapshotConnectionQueryOneByOne:
    def test_record_and_replay_one_by_one(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_one_by_one"

        # --- Record phase ---
        real_conn = _make_mock_connection()
        desc = (PicklableColumn("id", 23, None, None, None, None, None),)

        def mock_one_by_one(sql, row_callback, log_query=True, row_limit=None):
            for row in [(1,), (2,)]:
                row_callback(row, desc)
            return desc

        real_conn.execute_query_one_by_one = mock_one_by_one

        record_conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        recorded_rows = []
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            record_conn.execute_query_one_by_one("SELECT id FROM t", lambda row, desc: recorded_rows.append(row))
        assert recorded_rows == [(1,), (2,)]
        record_conn.finalize()

        # --- Replay phase ---
        replay_conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        replayed_rows = []
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            replay_conn.execute_query_one_by_one("SELECT id FROM t", lambda row, desc: replayed_rows.append(row))
        assert replayed_rows == [(1,), (2,)]


class TestSnapshotTestBoundaries:
    def test_multiple_tests_get_separate_snapshots(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()

        test_id_1 = "tests/test_a.py::test_one"
        test_id_2 = "tests/test_a.py::test_two"

        real_conn.execute_query.side_effect = [
            QueryResult(rows=[(1,)], columns=None),
            QueryResult(rows=[(2,)], columns=None),
        ]

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        # First test
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id_1} (call)"}):
            conn.execute_query("SELECT 1")

        # Second test — boundary detected automatically
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id_2} (call)"}):
            conn.execute_query("SELECT 2")

        conn.finalize()

        # Each test has its own snapshot
        loaded_1 = manager.load(test_id_1)
        loaded_2 = manager.load(test_id_2)
        assert len(loaded_1) == 1
        assert loaded_1[0].sql == "SELECT 1"
        assert len(loaded_2) == 1
        assert loaded_2[0].sql == "SELECT 2"


# ---------------------------------------------------------------------------
# Fallback: replay mismatch → transparent switch to real DB
# ---------------------------------------------------------------------------


class TestSnapshotFallback:
    """Tests that snapshot mismatches fall back to the real DB transparently.

    When replay detects a SQL mismatch, exhaustion, or missing snapshot, it
    re-executes previously replayed operations against the real DB to set up
    state, then continues in passthrough mode for the rest of that test.
    """

    def _save_snapshot(self, manager, test_id, entries):
        manager.save(test_id, entries)

    def test_sql_mismatch_falls_back_to_real_db(self, tmp_path):
        """When a query doesn't match the snapshot, fall back to real DB."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_fallback_mismatch"

        # Snapshot has: UPDATE, then "SELECT COUNT(*)"
        self._save_snapshot(
            manager,
            test_id,
            [
                SnapshotEntry("update", "CREATE TABLE t (id INT)", None),
                SnapshotEntry("update", "INSERT INTO t VALUES (1)", None),
                SnapshotEntry(
                    "query",
                    "SELECT COUNT(*) FROM t",
                    QueryResult(rows=[(1,)], columns=None),
                ),
            ],
        )

        # Real connection that will be used for fallback
        real_conn = _make_mock_connection()
        real_conn.execute_update.return_value = None
        real_conn.execute_query.return_value = QueryResult(rows=[(42,)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="replay", allow_fallback=True)

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            # First two operations match the snapshot → replayed from cache
            conn.execute_update("CREATE TABLE t (id INT)")
            conn.execute_update("INSERT INTO t VALUES (1)")

            # Third operation: SQL changed → triggers fallback
            result = conn.execute_query("SELECT COUNT(*) FROM t WHERE id > 0")

        # The fallback should have:
        # 1. Re-executed the 2 previous UPDATEs against the real DB
        # 2. Executed the mismatched query against the real DB
        assert result.rows == [(42,)]

        # Verify the 2 UPDATEs were re-executed (for DB state setup)
        update_calls = real_conn.execute_update.call_args_list
        assert len(update_calls) == 2
        assert update_calls[0].args[0] == "CREATE TABLE t (id INT)"
        assert update_calls[1].args[0] == "INSERT INTO t VALUES (1)"

        # Verify the mismatched query was executed against real DB
        real_conn.execute_query.assert_called_once_with("SELECT COUNT(*) FROM t WHERE id > 0", log_query=True)

    def test_fallback_continues_in_passthrough(self, tmp_path):
        """After fallback, all subsequent operations go to the real DB."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_fallback_passthrough"

        self._save_snapshot(
            manager,
            test_id,
            [
                SnapshotEntry("update", "CREATE TABLE t (id INT)", None),
                SnapshotEntry(
                    "query",
                    "SELECT 1",
                    QueryResult(rows=[(1,)], columns=None),
                ),
            ],
        )

        real_conn = _make_mock_connection()
        real_conn.execute_update.return_value = None
        real_conn.execute_query.side_effect = [
            # First call: the mismatched query that triggers fallback
            QueryResult(rows=[(100,)], columns=None),
            # Second call: subsequent query in passthrough mode
            QueryResult(rows=[(200,)], columns=None),
        ]

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="replay", allow_fallback=True)

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_update("CREATE TABLE t (id INT)")  # matches snapshot

            # Mismatch → triggers fallback
            r1 = conn.execute_query("SELECT CHANGED_SQL")

            # Subsequent operations go straight to real DB (passthrough)
            r2 = conn.execute_query("SELECT ANOTHER_QUERY")

        assert r1.rows == [(100,)]
        assert r2.rows == [(200,)]

    def test_snapshot_exhaustion_falls_back(self, tmp_path):
        """When the test has more operations than the snapshot, fall back."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_fallback_exhausted"

        # Snapshot has only 1 operation
        self._save_snapshot(
            manager,
            test_id,
            [
                SnapshotEntry("update", "CREATE TABLE t (id INT)", None),
            ],
        )

        real_conn = _make_mock_connection()
        real_conn.execute_update.return_value = None
        real_conn.execute_query.return_value = QueryResult(rows=[(5,)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="replay", allow_fallback=True)

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_update("CREATE TABLE t (id INT)")  # matches, replayed

            # Extra operation not in snapshot → fallback
            result = conn.execute_query("SELECT COUNT(*) FROM t")

        assert result.rows == [(5,)]

        # The 1 previous UPDATE should have been re-executed
        real_conn.execute_update.assert_called_once_with("CREATE TABLE t (id INT)", log_query=False)

    def test_missing_snapshot_falls_back(self, tmp_path):
        """When no snapshot exists for a test, fall back to real DB entirely."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        # No snapshot saved for this test

        real_conn = _make_mock_connection()
        real_conn.execute_update.return_value = None
        real_conn.execute_query.return_value = QueryResult(rows=[(7,)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="replay", allow_fallback=True)

        test_id = "tests/test_x.py::test_no_snapshot"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_update("CREATE TABLE t (id INT)")
            result = conn.execute_query("SELECT COUNT(*) FROM t")

        assert result.rows == [(7,)]
        real_conn.execute_update.assert_called_once()
        real_conn.execute_query.assert_called_once()

    def test_missing_snapshot_without_real_conn_raises(self, tmp_path):
        """Without a real connection, missing snapshot still raises."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "tests/test_x.py::test_gone (call)"}):
            with pytest.raises(SnapshotNotFoundError):
                conn.execute_query("SELECT 1")

    def test_missing_snapshot_with_real_conn_raises_by_default(self, tmp_path):
        """With a real connection but fallback disabled, missing snapshot raises."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="replay")

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "tests/test_x.py::test_gone (call)"}):
            with pytest.raises(SnapshotNotFoundError):
                conn.execute_query("SELECT 1")

    def test_mismatch_without_fallback_raises(self, tmp_path):
        """Without fallback enabled, mismatch raises SnapshotMismatchError."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_no_real"

        manager.save(
            test_id,
            [SnapshotEntry("query", "SELECT 1", QueryResult(rows=[(1,)], columns=None))],
        )

        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with pytest.raises(SnapshotMismatchError, match="fallback is disabled"):
                conn.execute_query("SELECT DIFFERENT")

    def test_fallback_resets_for_next_test(self, tmp_path):
        """Fallback in one test doesn't affect the next test's replay."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id_1 = "tests/test_x.py::test_falls_back"
        test_id_2 = "tests/test_x.py::test_replays_fine"

        # First test: snapshot will mismatch
        manager.save(
            test_id_1,
            [SnapshotEntry("query", "SELECT OLD", QueryResult(rows=[(1,)], columns=None))],
        )
        # Second test: snapshot will match
        manager.save(
            test_id_2,
            [SnapshotEntry("query", "SELECT 2", QueryResult(rows=[(2,)], columns=None))],
        )

        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[(99,)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="replay", allow_fallback=True)

        # First test: mismatch → fallback to real DB
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id_1} (call)"}):
            r1 = conn.execute_query("SELECT NEW")
        assert r1.rows == [(99,)]  # from real DB

        # Second test: should replay from snapshot (NOT still in fallback)
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id_2} (call)"}):
            r2 = conn.execute_query("SELECT 2")
        assert r2.rows == [(2,)]  # from snapshot, not real DB

    def test_fallback_re_records_snapshot(self, tmp_path):
        """Fallback overwrites the snapshot with fresh results from the real DB."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_rerecord"

        # Old snapshot with outdated SQL
        self._save_snapshot(
            manager,
            test_id,
            [
                SnapshotEntry("update", "CREATE TABLE t (id INT)", None),
                SnapshotEntry(
                    "query",
                    "SELECT OLD_SQL",
                    QueryResult(rows=[(1,)], columns=None),
                ),
            ],
        )

        real_conn = _make_mock_connection()
        real_conn.execute_update.return_value = None
        real_conn.execute_query.side_effect = [
            # Re-execution of the UPDATE's preceding query (none here)
            # Re-execution of "CREATE TABLE" is an update, handled separately
            # The mismatched query
            QueryResult(rows=[(42,)], columns=(PicklableColumn("n", 23, None, None, None, None, None),)),
            # A subsequent query
            QueryResult(rows=[(99,)], columns=(PicklableColumn("m", 23, None, None, None, None, None),)),
        ]

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="replay", allow_fallback=True)

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_update("CREATE TABLE t (id INT)")  # matches
            conn.execute_query("SELECT NEW_SQL")  # mismatch → fallback + re-record
            conn.execute_query("SELECT EXTRA")  # passthrough + record

        conn.finalize()

        # Verify the snapshot was overwritten with the new SQL and results
        loaded = manager.load(test_id)
        assert loaded is not None
        assert len(loaded) == 3  # UPDATE + 2 queries (was 2 before)
        assert loaded[0].op_type == "update"
        assert loaded[0].sql == "CREATE TABLE t (id INT)"
        assert loaded[1].op_type == "query"
        assert loaded[1].sql == "SELECT NEW_SQL"
        assert loaded[1].result.rows == [(42,)]
        assert loaded[2].op_type == "query"
        assert loaded[2].sql == "SELECT EXTRA"
        assert loaded[2].result.rows == [(99,)]

    def test_re_recorded_snapshot_replays_on_next_run(self, tmp_path):
        """After fallback re-records, the next replay run uses the updated snapshot."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_self_heal"

        # Old snapshot
        self._save_snapshot(
            manager,
            test_id,
            [SnapshotEntry("query", "SELECT OLD", QueryResult(rows=[(1,)], columns=None))],
        )

        # --- First run: fallback + re-record ---
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(
            rows=[(77,)], columns=(PicklableColumn("v", 23, None, None, None, None, None),)
        )

        conn1 = SnapshotDataSourceConnection(real_conn, manager, mode="replay", allow_fallback=True)
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn1.execute_query("SELECT NEW")  # mismatch → fallback + re-record
        conn1.finalize()

        # --- Second run: pure replay from updated snapshot ---
        conn2 = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn2.execute_query("SELECT NEW")  # should match now

        assert result.rows == [(77,)]  # from the re-recorded snapshot
        assert result.columns[0].name == "v"


# ---------------------------------------------------------------------------
# Lazy connection factory: real DB connection created only on fallback
# ---------------------------------------------------------------------------


class TestSnapshotLazyConnection:
    """Tests that the fallback_connection_factory is used lazily.

    When replay mode is started without a real connection, the factory should
    only be invoked when a fallback is actually needed (mismatch or missing
    snapshot). Successful replays should never call the factory.
    """

    def test_factory_called_on_missing_snapshot(self, tmp_path):
        """Factory is invoked lazily when no snapshot exists."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        factory_calls = []

        real_conn = _make_mock_connection()
        real_conn.execute_update.return_value = None
        real_conn.execute_query.return_value = QueryResult(rows=[(1,)], columns=None)

        def factory():
            factory_calls.append(True)
            return real_conn

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            fallback_connection_factory=factory,
            allow_fallback=True,
        )

        test_id = "tests/test_x.py::test_lazy_missing"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT 1")

        assert len(factory_calls) == 1
        assert result.rows == [(1,)]

    def test_factory_called_on_mismatch(self, tmp_path):
        """Factory is invoked lazily when SQL mismatch triggers fallback."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_lazy_mismatch"

        manager.save(
            test_id,
            [SnapshotEntry("query", "SELECT OLD", QueryResult(rows=[(1,)], columns=None))],
        )

        factory_calls = []
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[(99,)], columns=None)

        def factory():
            factory_calls.append(True)
            return real_conn

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            fallback_connection_factory=factory,
            allow_fallback=True,
        )

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT NEW")

        assert len(factory_calls) == 1
        assert result.rows == [(99,)]

    def test_factory_not_called_on_successful_replay(self, tmp_path):
        """Factory is NOT invoked when replay matches perfectly."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_lazy_match"

        manager.save(
            test_id,
            [SnapshotEntry("query", "SELECT 1", QueryResult(rows=[(1,)], columns=None))],
        )

        factory_calls = []

        def factory():
            factory_calls.append(True)
            return _make_mock_connection()

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            fallback_connection_factory=factory,
        )

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT 1")

        assert len(factory_calls) == 0
        assert result.rows == [(1,)]

    def test_factory_called_once_across_multiple_tests(self, tmp_path):
        """Factory is invoked at most once, even with multiple missing snapshots."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        factory_calls = []

        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[(1,)], columns=None)

        def factory():
            factory_calls.append(True)
            return real_conn

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            fallback_connection_factory=factory,
            allow_fallback=True,
        )

        # First test: missing snapshot → factory called
        test_id_1 = "tests/test_x.py::test_lazy_first"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id_1} (call)"}):
            conn.execute_query("SELECT 1")

        # Second test: also missing snapshot, but _real is already set
        test_id_2 = "tests/test_x.py::test_lazy_second"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id_2} (call)"}):
            conn.execute_query("SELECT 2")

        # Factory was only called once (for the first test)
        assert len(factory_calls) == 1


# ---------------------------------------------------------------------------
# Schema placeholder: portable snapshots across environments
# ---------------------------------------------------------------------------

PLACEHOLDER = "__$$__TEST_SCHEMA__$$__"


class TestSnapshotSchemaPlaceholder:
    """Tests that schema normalization makes snapshots portable.

    SQL and result data use the real schema name at runtime. When saving
    to snapshot, real schema is replaced with a placeholder. When loading,
    placeholder is replaced with the current real schema. This makes
    snapshots portable across environments.
    """

    def test_normalize_replaces_real_with_placeholder(self):
        """_normalize_for_snapshot replaces real schema in SQL and results."""
        manager = SnapshotManager("postgres", "/tmp/unused")
        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="record",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="dev_niels",
        )
        entry = SnapshotEntry(
            "query",
            'SELECT * FROM "soda"."dev_niels"."t"',
            QueryResult(rows=[("dev_niels",)], columns=None),
        )
        normalized = conn._normalize_for_snapshot(entry)
        assert normalized.sql == f'SELECT * FROM "soda"."{PLACEHOLDER}"."t"'
        assert normalized.result.rows == [(PLACEHOLDER,)]

    def test_denormalize_replaces_placeholder_with_real(self):
        """_denormalize_from_snapshot replaces placeholder with real schema."""
        manager = SnapshotManager("postgres", "/tmp/unused")
        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="ci_main_2026",
        )
        entry = SnapshotEntry(
            "query",
            f'SELECT * FROM "soda"."{PLACEHOLDER}"."t"',
            QueryResult(rows=[(PLACEHOLDER,)], columns=None),
        )
        denormalized = conn._denormalize_from_snapshot(entry)
        assert denormalized.sql == 'SELECT * FROM "soda"."ci_main_2026"."t"'
        assert denormalized.result.rows == [("ci_main_2026",)]

    def test_noop_without_placeholder(self):
        """Normalize/denormalize are no-ops when placeholder is not set."""
        manager = SnapshotManager("postgres", "/tmp/unused")
        conn = SnapshotDataSourceConnection(
            real_connection=None, snapshot_manager=manager, mode="replay"
        )
        entry = SnapshotEntry("query", "SELECT 1", QueryResult(rows=[(1,)], columns=None))
        assert conn._normalize_for_snapshot(entry) is entry
        assert conn._denormalize_from_snapshot(entry) is entry

    def test_record_stores_placeholder_in_snapshot(self, tmp_path):
        """Record mode stores SQL with placeholder, executes with real schema."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_placeholder_record"

        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[("dev_niels",)], columns=None)

        conn = SnapshotDataSourceConnection(
            real_connection=real_conn,
            snapshot_manager=manager,
            mode="record",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="dev_niels",
        )

        sql = 'SELECT schema FROM "soda"."dev_niels"."t"'

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query(sql)
        conn.finalize()

        # Real DB received SQL with real schema (unchanged)
        real_conn.execute_query.assert_called_once_with(sql, log_query=True)
        assert result.rows == [("dev_niels",)]

        # Snapshot stores SQL and results with placeholder
        loaded = manager.load(test_id)
        assert loaded[0].sql == f'SELECT schema FROM "soda"."{PLACEHOLDER}"."t"'
        assert loaded[0].result.rows == [(PLACEHOLDER,)]

    def test_replay_across_environments(self, tmp_path):
        """Snapshot recorded on env A replays on env B (different schema name)."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_cross_env"

        # Snapshot was saved with placeholder (as if recorded on env A)
        manager.save(
            test_id,
            [SnapshotEntry(
                "query",
                f'SELECT schema FROM "soda"."{PLACEHOLDER}"."t"',
                QueryResult(rows=[(PLACEHOLDER,)], columns=None),
            )],
        )

        # Replay on env B with a DIFFERENT real schema name
        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="ci_main_2026",
        )

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            # SQL uses env B's schema name — denormalized snapshot SQL matches
            result = conn.execute_query('SELECT schema FROM "soda"."ci_main_2026"."t"')

        # Result has env B's schema name (denormalized from placeholder)
        assert result.rows == [("ci_main_2026",)]

    def test_lowercase_schema_in_metadata_queries(self, tmp_path):
        """Handles LOWER() comparisons when schema has mixed case."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_lowercase"

        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[("Dev_Schema",)], columns=None)

        conn = SnapshotDataSourceConnection(
            real_connection=real_conn,
            snapshot_manager=manager,
            mode="record",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="Dev_Schema",
        )

        # SQL has both quoted (original case) and LOWER() (lowered) schema name
        sql = """WHERE "schema" = 'Dev_Schema' AND LOWER(n) = 'dev_schema'"""

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query(sql)
        conn.finalize()

        # Snapshot stores both original and lowercased placeholder forms
        loaded = manager.load(test_id)
        assert PLACEHOLDER in loaded[0].sql
        assert PLACEHOLDER.lower() in loaded[0].sql
