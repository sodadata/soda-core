"""Tests for the snapshot recording/replay system."""

from __future__ import annotations

import json
import os
import pickle
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
        iterator = QueryResultIterator(cursor, format_row=tuple)
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


class TestSnapshotManagerSessionTimezone:
    """Session-TZ sidecar is per-datasource (one file shared across tests)
    because session timezone is connection-scoped, not test-scoped.
    """

    @pytest.fixture
    def tmp_snapshot_dir(self, tmp_path):
        return str(tmp_path / "snapshots")

    @pytest.fixture
    def manager(self, tmp_snapshot_dir):
        return SnapshotManager(datasource_type="postgres", snapshot_dir=tmp_snapshot_dir)

    def test_load_returns_none_when_sidecar_missing(self, manager):
        assert manager.load_session_timezone() is None

    def test_save_then_load_iana_zone_roundtrip(self, manager):
        from zoneinfo import ZoneInfo

        manager.save_session_timezone(ZoneInfo("America/Los_Angeles"))
        recorded = manager.load_session_timezone()
        assert recorded == "America/Los_Angeles"

        # Round-trip: parse the recorded string back and confirm it produces a
        # tzinfo that gives the right offset. (We can't compare ZoneInfo objects
        # by identity because the parser returns a fresh ZoneInfo instance.)
        from soda_core.common.data_source_connection import parse_session_timezone

        parsed = parse_session_timezone(recorded)
        from datetime import datetime

        assert parsed.utcoffset(datetime(2024, 1, 15)).total_seconds() == -8 * 3600  # PST winter
        assert parsed.utcoffset(datetime(2024, 7, 15)).total_seconds() == -7 * 3600  # PDT summer

    def test_save_then_load_utc_roundtrip(self, manager):
        from datetime import timezone as _timezone

        manager.save_session_timezone(_timezone.utc)
        recorded = manager.load_session_timezone()
        assert recorded == "UTC"

    def test_save_then_load_fixed_offset_roundtrip(self, manager):
        from datetime import timedelta
        from datetime import timezone as _timezone

        manager.save_session_timezone(_timezone(timedelta(hours=-8)))
        recorded = manager.load_session_timezone()
        assert recorded == "-08:00"

    def test_save_then_load_zero_offset_collapses_to_utc(self, manager):
        from datetime import timedelta
        from datetime import timezone as _timezone

        # ``timezone(timedelta(0))`` is functionally UTC; we serialize as "UTC"
        # so the recorded value round-trips cleanly through parse_session_timezone
        # back to the singleton.
        manager.save_session_timezone(_timezone(timedelta(0)))
        recorded = manager.load_session_timezone()
        assert recorded == "UTC"

    def test_save_overwrites_existing_sidecar(self, manager):
        # Re-recording updates the sidecar in place rather than appending.
        from datetime import timezone as _timezone
        from zoneinfo import ZoneInfo

        manager.save_session_timezone(_timezone.utc)
        assert manager.load_session_timezone() == "UTC"

        manager.save_session_timezone(ZoneInfo("America/Los_Angeles"))
        assert manager.load_session_timezone() == "America/Los_Angeles"

    def test_sidecar_is_per_datasource(self, tmp_snapshot_dir):
        # Two managers for two datasources see independent sidecar files.
        from datetime import timezone as _timezone
        from zoneinfo import ZoneInfo

        pg = SnapshotManager(datasource_type="postgres", snapshot_dir=tmp_snapshot_dir)
        sf = SnapshotManager(datasource_type="snowflake", snapshot_dir=tmp_snapshot_dir)

        pg.save_session_timezone(_timezone.utc)
        sf.save_session_timezone(ZoneInfo("America/Los_Angeles"))

        assert pg.load_session_timezone() == "UTC"
        assert sf.load_session_timezone() == "America/Los_Angeles"


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
        assert result == 0

    def test_sql_mismatch_raises_by_default(self, setup):
        conn, _, test_id = setup
        # Under the rerun model (default), mismatch raises SnapshotMismatchError
        # straight up so the pytest plugin can re-run the test.
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with pytest.raises(SnapshotMismatchError, match="Snapshot mismatch"):
                conn.execute_query("SELECT WRONG_SQL")

    def test_snapshot_exhaustion_raises_by_default(self, setup):
        conn, _, test_id = setup
        # Under the rerun model (default), running past the end of a recorded
        # snapshot raises SnapshotMismatchError.
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT COUNT(*)")
            conn.execute_update("INSERT INTO t VALUES (1)")
            with pytest.raises(SnapshotMismatchError, match="Snapshot exhausted"):
                conn.execute_query("SELECT extra")

    def test_missing_snapshot_raises(self, tmp_path):
        from helpers.snapshot_connection import reset_pending_rerun_record

        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": "tests/test_missing.py::test_gone (call)"}):
            with pytest.raises(SnapshotNotFoundError, match="No snapshot found"):
                conn.execute_query("SELECT 1")
        # The wrapper queues a rerun signal so the plugin can let the rerun
        # decide the outcome; clean it up so it doesn't leak into other tests.
        reset_pending_rerun_record()


class TestSnapshotConnectionQueryIterate:
    def test_record_and_replay_iterate(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_iterate"

        # --- Record phase ---
        real_conn = _make_mock_connection()
        desc = (PicklableColumn("val", 23, None, None, None, None, None),)
        fake_cursor = FakeCursor([(1,), (2,), (3,)], description=desc, rowcount=3)
        real_iter = QueryResultIterator(fake_cursor, format_row=tuple)

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
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
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
        # _normalize_value lowercases all placeholders for uniform storage
        lc = PLACEHOLDER.lower()
        assert normalized.sql == f'SELECT * FROM "soda"."{lc}"."t"'
        assert normalized.result.rows == [(lc,)]

    def test_denormalize_replaces_placeholder_with_real(self):
        """_denormalize_from_snapshot replaces placeholder with real schema."""
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="ci_main_2026",
        )
        # Snapshots always store lowercase placeholders (as produced by _normalize_value)
        lc = PLACEHOLDER.lower()
        entry = SnapshotEntry(
            "query",
            f'SELECT * FROM "soda"."{lc}"."t"',
            QueryResult(rows=[(lc,)], columns=None),
        )
        denormalized = conn._denormalize_from_snapshot(entry)
        assert denormalized.sql == 'SELECT * FROM "soda"."ci_main_2026"."t"'
        assert denormalized.result.rows == [("ci_main_2026",)]

    def test_noop_without_placeholder(self):
        """Normalize/denormalize are no-ops when placeholder is not set."""
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
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

        # Snapshot stores SQL and results with lowercase placeholder
        lc = PLACEHOLDER.lower()
        loaded = manager.load(test_id)
        assert loaded[0].sql == f'SELECT schema FROM "soda"."{lc}"."t"'
        assert loaded[0].result.rows == [(lc,)]

    def test_replay_across_environments(self, tmp_path):
        """Snapshot recorded on env A replays on env B (different schema name)."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_cross_env"

        # Snapshot was saved with lowercase placeholder (as produced by _normalize_value)
        lc = PLACEHOLDER.lower()
        manager.save(
            test_id,
            [
                SnapshotEntry(
                    "query",
                    f'SELECT schema FROM "soda"."{lc}"."t"',
                    QueryResult(rows=[(lc,)], columns=None),
                )
            ],
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

        # Snapshot stores lowercased placeholder forms (uniform lowercase storage)
        loaded = manager.load(test_id)
        assert PLACEHOLDER.lower() in loaded[0].sql


# ---------------------------------------------------------------------------
# Passthrough queries: bypass snapshot entirely
# ---------------------------------------------------------------------------


class TestPassthroughQueries:
    """Tests for passthrough_queries that bypass snapshot recording/replay."""

    def test_passthrough_bypasses_snapshot_in_record_mode(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        conn.passthrough_queries = {"SELECT @@location": QueryResult(rows=[("US",)], columns=None)}

        test_id = "tests/test_x.py::test_passthrough_record"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT @@location")

        assert result.rows == [("US",)]
        # Real connection should NOT have been called for the passthrough query
        real_conn.execute_query.assert_not_called()

    def test_passthrough_bypasses_snapshot_in_replay_mode(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_passthrough_replay"

        # Save a snapshot with a real query (not the passthrough one)
        manager.save(test_id, [SnapshotEntry("query", "SELECT 1", QueryResult(rows=[(1,)], columns=None))])

        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.passthrough_queries = {"SELECT @@location": QueryResult(rows=[("EU",)], columns=None)}

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            # Passthrough returns mock result without consuming snapshot entries
            pt_result = conn.execute_query("SELECT @@location")
            # Real snapshot entry is still available
            real_result = conn.execute_query("SELECT 1")

        assert pt_result.rows == [("EU",)]
        assert real_result.rows == [(1,)]

    def test_passthrough_not_recorded_in_snapshot(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[(42,)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        conn.passthrough_queries = {"SELECT @@location": QueryResult(rows=[("US",)], columns=None)}

        test_id = "tests/test_x.py::test_passthrough_not_stored"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT @@location")  # passthrough
            conn.execute_query("SELECT 42")  # real query

        conn.finalize()
        loaded = manager.load(test_id)
        # Only the real query should be in the snapshot
        assert len(loaded) == 1
        assert loaded[0].sql == "SELECT 42"

    def test_passthrough_strips_whitespace(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        conn.passthrough_queries = {"SELECT @@location": QueryResult(rows=[("US",)], columns=None)}

        test_id = "tests/test_x.py::test_passthrough_strip"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("  SELECT @@location  ")

        assert result.rows == [("US",)]


# ---------------------------------------------------------------------------
# Record-mode cached queries: return cached result AND record in snapshot
# ---------------------------------------------------------------------------


class TestRecordModeCachedQueries:
    """Tests for record_mode_cached_queries that cache expensive queries."""

    def test_cached_query_returns_result_without_db_hit(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        cached_result = QueryResult(
            rows=[("public", "test_table", "BASE TABLE")],
            columns=(PicklableColumn("schema", 1043, None, None, None, None, None),),
        )
        conn.record_mode_cached_queries = {"SELECT * FROM metadata": cached_result}

        test_id = "tests/test_x.py::test_cached_no_db"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT * FROM metadata")

        assert result.rows == [("public", "test_table", "BASE TABLE")]
        # Real connection should NOT have been called
        real_conn.execute_query.assert_not_called()

    def test_cached_query_still_recorded_in_snapshot(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        cached_result = QueryResult(rows=[("t1",)], columns=None)
        conn.record_mode_cached_queries = {"SELECT tables": cached_result}

        test_id = "tests/test_x.py::test_cached_recorded"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT tables")

        conn.finalize()
        loaded = manager.load(test_id)
        assert len(loaded) == 1
        assert loaded[0].sql == "SELECT tables"
        assert loaded[0].result.rows == [("t1",)]

    def test_non_cached_query_hits_db_normally(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[(99,)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        conn.record_mode_cached_queries = {"SELECT cached": QueryResult(rows=[(1,)], columns=None)}

        test_id = "tests/test_x.py::test_non_cached"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = conn.execute_query("SELECT not_cached")

        assert result.rows == [(99,)]
        real_conn.execute_query.assert_called_once()


# ---------------------------------------------------------------------------
# Extra replacements: additional placeholder→real_value normalization
# ---------------------------------------------------------------------------


class TestExtraReplacements:
    """Tests for extra_replacements dict used by DWH interceptor."""

    def test_normalize_with_extra_replacements(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="record")
        conn.extra_replacements = {"__$$__SCAN_ID__$$__": "abc123"}

        entry = SnapshotEntry("query", "SELECT * FROM scan_abc123", QueryResult(rows=[("abc123",)], columns=None))
        normalized = conn._normalize_for_snapshot(entry)
        assert "abc123" not in normalized.sql
        assert "__$$__scan_id__$$__" in normalized.sql
        assert normalized.result.rows == [("__$$__scan_id__$$__",)]

    def test_denormalize_with_extra_replacements(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.extra_replacements = {"__$$__SCAN_ID__$$__": "xyz789"}

        entry = SnapshotEntry(
            "query",
            "SELECT * FROM scan___$$__scan_id__$$__",
            QueryResult(rows=[("__$$__scan_id__$$__",)], columns=None),
        )
        denormalized = conn._denormalize_from_snapshot(entry)
        assert denormalized.sql == "SELECT * FROM scan_xyz789"
        assert denormalized.result.rows == [("xyz789",)]

    def test_extra_replacements_combined_with_schema_placeholder(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[("dev_schema", "scan_001")], columns=None)

        conn = SnapshotDataSourceConnection(
            real_connection=real_conn,
            snapshot_manager=manager,
            mode="record",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="dev_schema",
        )
        conn.extra_replacements = {"__$$__SCAN__$$__": "scan_001"}

        test_id = "tests/test_x.py::test_combined_replacements"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT dev_schema, scan_001")

        conn.finalize()
        loaded = manager.load(test_id)
        # Both schema and scan ID should be replaced
        assert "dev_schema" not in loaded[0].sql
        assert "scan_001" not in loaded[0].sql

    def test_extra_replacements_in_record_replay_roundtrip(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_extra_roundtrip"

        # --- Record with scan_id=aaa ---
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[("aaa",)], columns=None)

        record_conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        record_conn.extra_replacements = {"__$$__SCAN__$$__": "aaa"}

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            record_conn.execute_query("SELECT aaa FROM t")
        record_conn.finalize()

        # --- Replay with scan_id=bbb (different run) ---
        replay_conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        replay_conn.extra_replacements = {"__$$__SCAN__$$__": "bbb"}

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            result = replay_conn.execute_query("SELECT bbb FROM t")

        assert result.rows == [("bbb",)]


# ---------------------------------------------------------------------------
# Timestamp and dynamic value normalization
# ---------------------------------------------------------------------------


class TestTimestampNormalization:
    """Tests for normalize_timestamps and _normalize_dynamic_values."""

    def test_normalize_iso_timestamp(self):
        sql = "INSERT INTO t VALUES ('2026-03-16T18:24:35.151223')"
        result = SnapshotDataSourceConnection._normalize_dynamic_values(sql)
        assert "'__$$__SODA_TIMESTAMP__$$__'" in result
        assert "2026-03-16" not in result

    def test_normalize_oracle_timestamp(self):
        sql = "INSERT INTO t VALUES (TIMESTAMP '2026-03-18 14:09:40')"
        result = SnapshotDataSourceConnection._normalize_dynamic_values(sql)
        assert "'__$$__SODA_TIMESTAMP__$$__'" in result
        assert "2026-03-18" not in result

    def test_normalize_timestamp_with_timezone(self):
        sql = "INSERT INTO t VALUES ('2026-03-16T17:24:35+00:00')"
        result = SnapshotDataSourceConnection._normalize_dynamic_values(sql)
        assert "'__$$__SODA_TIMESTAMP__$$__'" in result

    def test_normalize_soda_temp_uuid(self):
        sql = "CREATE TABLE __soda_temp_abcdef01234567890abcdef012345678 AS SELECT 1"
        result = SnapshotDataSourceConnection._normalize_dynamic_values(sql)
        assert "__soda_temp___$$__SODA_UUID__$$__" in result
        assert "abcdef01234567890abcdef012345678" not in result

    def test_normalize_soda_temp_uuid_case_insensitive(self):
        sql = "DROP TABLE __SODA_TEMP_ABCDEF01234567890ABCDEF012345678"
        result = SnapshotDataSourceConnection._normalize_dynamic_values(sql)
        assert "__soda_temp___$$__SODA_UUID__$$__" in result

    def test_sql_matches_with_timestamps_enabled(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.normalize_timestamps = True

        stored = "INSERT INTO t VALUES ('2026-03-16T18:24:35.151223')"
        incoming = "INSERT INTO t VALUES ('2026-03-20T10:00:00.000000')"
        assert conn._sql_matches(stored, incoming)

    def test_sql_matches_without_timestamps_disabled(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.normalize_timestamps = False

        stored = "INSERT INTO t VALUES ('2026-03-16T18:24:35.151223')"
        incoming = "INSERT INTO t VALUES ('2026-03-20T10:00:00.000000')"
        assert not conn._sql_matches(stored, incoming)

    def test_replay_matches_despite_different_timestamps(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_ts_replay"

        # Save snapshot with one timestamp
        manager.save(
            test_id,
            [
                SnapshotEntry(
                    "update",
                    "INSERT INTO t (ts) VALUES ('2026-03-16T18:24:35.151223')",
                    None,
                )
            ],
        )

        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.normalize_timestamps = True

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            # Different timestamp — should still match because normalize_timestamps=True
            conn.execute_update("INSERT INTO t (ts) VALUES ('2026-03-20T10:00:00.000000')")
        # No exception = success

    def test_replay_with_different_soda_temp_uuid(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_uuid_replay"

        manager.save(
            test_id,
            [
                SnapshotEntry(
                    "update",
                    "CREATE TABLE __soda_temp_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1 AS SELECT 1",
                    None,
                )
            ],
        )

        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.normalize_timestamps = True

        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_update("CREATE TABLE __soda_temp_bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2 AS SELECT 1")


# ---------------------------------------------------------------------------
# Unconsumed entries detection
# ---------------------------------------------------------------------------


class TestGetattr:
    """Tests for __getattr__ attribute proxying to real connection."""

    def test_proxies_attribute_to_real_connection(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        real_conn = _make_mock_connection()
        real_conn.athena_staging_dir = "/some/s3/path"

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        assert conn.athena_staging_dir == "/some/s3/path"

    def test_triggers_factory_when_real_is_none(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        real_conn = _make_mock_connection()
        real_conn.custom_attr = "hello"

        factory_calls = []

        def factory():
            factory_calls.append(True)
            return real_conn

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            fallback_connection_factory=factory,
        )

        result = conn.custom_attr
        assert result == "hello"
        assert len(factory_calls) == 1

    def test_raises_attribute_error_for_missing(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")

        # Use a real object (not MagicMock) so hasattr() returns False for missing attrs
        class FakeConn:
            connection = object()
            connection_properties = {}
            custom_attr = "exists"

        conn = SnapshotDataSourceConnection(FakeConn(), manager, mode="record")

        with pytest.raises(AttributeError, match="no_such_attribute"):
            _ = conn.no_such_attribute

    def test_raises_attribute_error_when_no_real_and_no_factory(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        with pytest.raises(AttributeError):
            _ = conn.some_missing_attr


# ---------------------------------------------------------------------------
# _safe_pickle_value
# ---------------------------------------------------------------------------


class TestSafePickleValue:
    """Tests for _safe_pickle_value fallback for unpicklable types."""

    def test_picklable_value_unchanged(self):
        assert SnapshotDataSourceConnection._safe_pickle_value(42) == 42
        assert SnapshotDataSourceConnection._safe_pickle_value("hello") == "hello"
        assert SnapshotDataSourceConnection._safe_pickle_value(None) is None

    def test_unpicklable_value_converted_to_string(self):
        # Create a value that raises TypeError when pickled
        class Unpicklable:
            def __reduce__(self):
                raise TypeError("cannot pickle this")

            def __str__(self):
                return "unpicklable_value"

        result = SnapshotDataSourceConnection._safe_pickle_value(Unpicklable())
        assert result == "unpicklable_value"


# ---------------------------------------------------------------------------
# Dot-separated path normalization (Dremio-style)
# ---------------------------------------------------------------------------


class TestDotSeparatedNormalization:
    """Tests for normalization of dot-separated identifiers (e.g. Dremio)."""

    def test_to_quoted_helper(self):
        assert SnapshotDataSourceConnection._to_quoted("a.b.c") == '"a"."b"."c"'
        assert SnapshotDataSourceConnection._to_quoted("schema") == '"schema"'

    def test_normalize_dot_separated_path(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="record")
        conn.extra_replacements = {"__$$__DWH__$$__": "catalog.schema.prefix"}

        entry = SnapshotEntry(
            "query",
            'SELECT * FROM "catalog"."schema"."prefix"."table"',
            None,
        )
        normalized = conn._normalize_for_snapshot(entry)
        assert '"catalog"."schema"."prefix"' not in normalized.sql.lower()

    def test_denormalize_dot_separated_path(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.extra_replacements = {"__$$__DWH__$$__": "my_cat.my_schema.my_prefix"}

        # Snapshot stored with lowercase placeholder (as normalize does)
        entry = SnapshotEntry(
            "query",
            'SELECT * FROM "__$$__dwh__$$__"."table"',
            None,
        )
        # After denormalize, placeholder should be replaced with dot-separated quoted form
        denormalized = conn._denormalize_from_snapshot(entry)
        assert "__$$__dwh__$$__" not in denormalized.sql


# ---------------------------------------------------------------------------
# Connection lifecycle: commit, rollback, close
# ---------------------------------------------------------------------------


class TestConnectionLifecycle:
    """Tests for commit(), rollback(), close_connection()."""

    def test_commit_delegates_to_real(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        conn.commit()
        real_conn.commit.assert_called_once()

    def test_commit_noop_in_replay(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        # Should not raise
        conn.commit()

    def test_rollback_delegates_to_real(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        real_conn = _make_mock_connection()
        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        conn.rollback()
        real_conn.rollback.assert_called_once()

    def test_rollback_noop_in_replay(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        conn.rollback()

    def test_close_connection_finalizes_and_closes_real(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[(1,)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        test_id = "tests/test_x.py::test_close"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT 1")

        # Remove PYTEST_CURRENT_TEST so close doesn't try to start a new test
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            conn.close_connection()

        # Snapshot should have been saved via finalize()
        loaded = manager.load(test_id)
        assert loaded is not None
        assert len(loaded) == 1

        # Real connection should have been closed
        real_conn.close_connection.assert_called_once()

        # connection should be set to None
        assert conn.connection is None


# ---------------------------------------------------------------------------
# Fallback for execute_query_iterate and execute_query_one_by_one
# ---------------------------------------------------------------------------


class TestSessionLevelOperations:
    """Tests for operations outside of a test context."""

    def test_session_level_iterate_passes_through(self):
        from contextlib import contextmanager

        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        real_conn = _make_mock_connection()
        desc = (PicklableColumn("v", 23, None, None, None, None, None),)
        fake_cursor = FakeCursor([(1,)], description=desc, rowcount=1)
        real_iter = QueryResultIterator(fake_cursor, format_row=tuple)

        @contextmanager
        def mock_iterate(sql, log_query=True):
            yield real_iter

        real_conn.execute_query_iterate = mock_iterate

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        # No PYTEST_CURRENT_TEST set
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            with conn.execute_query_iterate("SELECT 1") as it:
                rows = list(it)

        assert rows == [(1,)]

    def test_session_level_one_by_one_passes_through(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        real_conn = _make_mock_connection()
        desc = (PicklableColumn("id", 23, None, None, None, None, None),)

        def mock_one_by_one(sql, row_callback, log_query=True, row_limit=None):
            row_callback((1,), desc)
            return desc

        real_conn.execute_query_one_by_one = mock_one_by_one

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        captured = []
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            conn.execute_query_one_by_one("SELECT 1", lambda row, desc: captured.append(row))

        assert captured == [(1,)]

    def test_session_level_without_real_conn_raises_for_query(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            with pytest.raises(RuntimeError, match="No real connection"):
                conn.execute_query("SELECT 1")

    def test_session_level_without_real_conn_raises_for_update(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            with pytest.raises(RuntimeError, match="No real connection"):
                conn.execute_update("CREATE TABLE t (id INT)")

    def test_session_level_without_real_conn_raises_for_iterate(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            with pytest.raises(RuntimeError, match="No real connection"):
                with conn.execute_query_iterate("SELECT 1") as it:
                    list(it)

    def test_session_level_without_real_conn_raises_for_one_by_one(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("PYTEST_CURRENT_TEST", None)
            with pytest.raises(RuntimeError, match="No real connection"):
                conn.execute_query_one_by_one("SELECT 1", lambda row, desc: None)


# ---------------------------------------------------------------------------
# Row limit in execute_query_one_by_one replay
# ---------------------------------------------------------------------------


class TestRowLimitReplay:
    """Tests for row_limit support in execute_query_one_by_one replay."""

    def test_one_by_one_respects_row_limit_in_replay(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        test_id = "tests/test_x.py::test_row_limit"

        desc = (PicklableColumn("id", 23, None, None, None, None, None),)
        # Snapshot recorded 5 rows
        manager.save(
            test_id,
            [SnapshotEntry("query_one_by_one", "SELECT id FROM t", (desc, [(1,), (2,), (3,), (4,), (5,)]))],
        )

        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")

        captured = []
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query_one_by_one("SELECT id FROM t", lambda row, desc: captured.append(row), row_limit=3)

        # Only 3 rows should have been delivered despite 5 being in the snapshot
        assert captured == [(1,), (2,), (3,)]


# ---------------------------------------------------------------------------
# format_rows delegation
# ---------------------------------------------------------------------------


class TestFormatRows:
    """Tests for format_rows delegation."""

    def test_format_rows_delegates_to_real(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        real_conn = _make_mock_connection()
        real_conn.format_rows.return_value = [("formatted",)]

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")
        result = conn.format_rows([("raw",)])
        assert result == [("formatted",)]
        real_conn.format_rows.assert_called_once_with([("raw",)])

    def test_format_rows_identity_without_real(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        rows = [("a", 1), ("b", 2)]
        assert conn.format_rows(rows) == rows


# ---------------------------------------------------------------------------
# Normalize PicklableColumn in values
# ---------------------------------------------------------------------------


class TestNormalizePicklableColumnInValues:
    """Tests for recursive normalization of PicklableColumn objects."""

    def test_normalize_value_in_picklable_column(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="record",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="dev_schema",
        )

        col = PicklableColumn("dev_schema", 1043, None, None, None, None, None)
        # _normalize_value directly handles PicklableColumn recursion
        normalized_col = conn._normalize_value(col, "dev_schema", PLACEHOLDER)
        assert isinstance(normalized_col, PicklableColumn)
        assert normalized_col.name == PLACEHOLDER.lower()
        assert normalized_col.type_code == 1043

    def test_denormalize_value_in_picklable_column(self):
        manager = SnapshotManager("postgres", ".test_snapshots_temp")
        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            schema_placeholder=PLACEHOLDER,
            real_schema_name="prod_schema",
        )

        col = PicklableColumn(PLACEHOLDER.lower(), 1043, None, None, None, None, None)
        denormalized_col = conn._denormalize_value(col, PLACEHOLDER, "prod_schema")
        assert isinstance(denormalized_col, PicklableColumn)
        assert denormalized_col.name == "prod_schema"


# ---------------------------------------------------------------------------
# Finalize prevents re-activation
# ---------------------------------------------------------------------------


class TestFinalizedState:
    """Tests that finalize() prevents the snapshot from re-activating.

    After finalize(), teardown code may run while PYTEST_CURRENT_TEST is still set.
    The snapshot must not re-enter recording mode, otherwise cached queries may
    return stale results (e.g. missing views that were created during tests).
    """

    def test_finalize_prevents_reactivation(self, tmp_path):
        """After finalize(), queries pass through to real connection even with PYTEST_CURRENT_TEST set."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        real_conn.execute_query.return_value = QueryResult(rows=[("view_1",)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        test_id = "tests/test_x.py::test_last"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT 1")

        # Finalize (as end_test_session does)
        conn.finalize()
        assert conn._finalized is True
        assert conn._current_test_id is None

        # PYTEST_CURRENT_TEST is still set (as happens during session fixture teardown)
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (teardown)"}):
            result = conn.execute_query("SELECT view_name FROM views")

        # Should have passed through to real connection, not used cache
        assert result.rows == [("view_1",)]
        real_conn.execute_query.assert_called_with("SELECT view_name FROM views", log_query=True)
        # _current_test_id should remain None (not re-activated)
        assert conn._current_test_id is None

    def test_finalize_prevents_cached_query_usage(self, tmp_path):
        """After finalize(), record_mode_cached_queries are bypassed."""
        manager = SnapshotManager("postgres", str(tmp_path / "snaps"))
        real_conn = _make_mock_connection()
        # Real DB returns the view
        real_conn.execute_query.return_value = QueryResult(rows=[("table_1",), ("view_1",)], columns=None)

        conn = SnapshotDataSourceConnection(real_conn, manager, mode="record")

        # Set up a cached query that's stale (missing the view)
        stale_result = QueryResult(rows=[("table_1",)], columns=None)
        conn.record_mode_cached_queries["SELECT * FROM metadata"] = stale_result

        test_id = "tests/test_x.py::test_cached"
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            # During test, cache is used
            result = conn.execute_query("SELECT * FROM metadata")
            assert result.rows == [("table_1",)]  # stale cache

        conn.finalize()

        # After finalize, cache should be bypassed
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (teardown)"}):
            result = conn.execute_query("SELECT * FROM metadata")

        # Should have gotten the real result with the view
        assert result.rows == [("table_1",), ("view_1",)]


# ---------------------------------------------------------------------------
# Linked snapshot fallback: cascade from DWH to source snapshot
# ---------------------------------------------------------------------------


class TestRerunSignalling:
    """The wrapper signals a rerun by registering in ``_PENDING_RERUN`` and
    re-raising. The pytest plugin then runs the test once more against the
    real DB with the wrapper swapped out."""

    @pytest.fixture(autouse=True)
    def _clean_rerun_state(self):
        from helpers.snapshot_connection import reset_pending_rerun_record

        reset_pending_rerun_record()
        yield
        reset_pending_rerun_record()

    def _save_simple_snapshot(self, manager, test_id):
        manager.save(
            test_id,
            [SnapshotEntry("query", "SELECT old", QueryResult(rows=[(1,)], columns=None))],
        )

    def test_mismatch_raises_and_registers_rerun(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        test_id = "tests/test_rerun.py::test_mismatch"
        self._save_simple_snapshot(manager, test_id)

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
        )
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with pytest.raises(SnapshotMismatchError):
                conn.execute_query("SELECT new")

        from helpers.snapshot_connection import get_pending_rerun_record

        assert test_id in get_pending_rerun_record()

    def test_missing_snapshot_raises_and_registers_rerun(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        test_id = "tests/test_rerun.py::test_missing"

        # Even with allow_fallback and a factory, the wrapper raises so the
        # plugin can rerun rather than silently synthesising state mid-test.
        factory_called = []

        def factory():
            factory_called.append(True)
            return _make_mock_connection()

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
            fallback_connection_factory=factory,
        )
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with pytest.raises(SnapshotNotFoundError):
                conn.execute_query("SELECT 1")

        # Factory must NOT have been called — no auto-record on missing snapshot.
        assert factory_called == []
        from helpers.snapshot_connection import get_pending_rerun_record

        assert test_id in get_pending_rerun_record()

    def test_mismatch_without_fallback_raises_without_registering(self, tmp_path):
        """``allow_fallback=False`` means: hard fail on mismatch, no rerun signal."""
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        test_id = "tests/test_rerun.py::test_no_fallback"
        self._save_simple_snapshot(manager, test_id)

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=False,
        )
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with pytest.raises(SnapshotMismatchError):
                conn.execute_query("SELECT new")

        from helpers.snapshot_connection import get_pending_rerun_record

        assert test_id not in get_pending_rerun_record()

    def test_missing_snapshot_registers_rerun_even_without_fallback(self, tmp_path):
        """A missing snapshot ALWAYS registers a rerun signal, regardless of
        ``allow_fallback``. There's no recording to replay — only the real DB
        can tell us if the test passes. The plugin then decides whether to
        actually rerun (default) or hard fail (strict mode)."""
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        test_id = "tests/test_rerun.py::test_never_recorded"

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=False,  # explicitly off — mismatches would NOT signal
        )
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            with pytest.raises(SnapshotNotFoundError):
                conn.execute_query("SELECT 1")

        # Despite allow_fallback=False, the missing snapshot still queues a rerun.
        from helpers.snapshot_connection import get_pending_rerun_record

        assert test_id in get_pending_rerun_record()

    def test_live_wrapper_registry_tracks_instances(self, tmp_path):
        from helpers.snapshot_connection import get_live_snapshot_wrappers

        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        before = set(get_live_snapshot_wrappers())
        conn = SnapshotDataSourceConnection(real_connection=None, snapshot_manager=manager, mode="replay")
        assert conn in set(get_live_snapshot_wrappers())
        # Weak ref: deleting the wrapper drops it from the registry on GC.
        del conn
        import gc

        gc.collect()
        after = set(get_live_snapshot_wrappers())
        assert after == before

    def test_unconsumed_entries_in_teardown_queue_rerun_instead_of_raising(self, tmp_path):
        """If a mid-test mismatch is swallowed by user code, the per-test
        teardown still calls ``_finalize_current_test``. It must NOT raise —
        that would fail the teardown phase and block the plugin from running
        the rerun. Instead it queues another rerun signal."""
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        test_id = "tests/test_rerun.py::test_unconsumed"
        manager.save(
            test_id,
            [
                SnapshotEntry("query", "SELECT a", QueryResult(rows=[(1,)], columns=None)),
                SnapshotEntry("query", "SELECT b", QueryResult(rows=[(2,)], columns=None)),
            ],
        )
        conn = SnapshotDataSourceConnection(
            real_connection=_make_mock_connection(),
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
        )
        with patch.dict(os.environ, {"PYTEST_CURRENT_TEST": f"{test_id} (call)"}):
            conn.execute_query("SELECT a")  # consume entry 0, leave entry 1
            conn._finalize_current_test()
        from helpers.snapshot_connection import get_pending_rerun_record

        assert test_id in get_pending_rerun_record()
        assert conn._replay_data is None
        assert conn._replay_index == 0


class TestSwapBasedRerun:
    """The swap-based rerun model: between attempts, the plugin replaces
    ``data_source_impl.data_source_connection`` with the real connection so
    the rerun behaves identically to ``SODA_TEST_SNAPSHOT=off``."""

    def test_swap_detaches_wrapper_from_data_source_impl(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        real = _make_mock_connection()
        conn = SnapshotDataSourceConnection(
            real_connection=real,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
        )
        fake_ds_impl = MagicMock()
        fake_ds_impl.data_source_connection = conn
        conn._data_source_impl = fake_ds_impl

        assert conn.swap_to_real_for_rerun() is True
        # After swap, the data source's connection is the real one, not the wrapper.
        assert fake_ds_impl.data_source_connection is real

    def test_restore_reattaches_wrapper(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        real = _make_mock_connection()
        conn = SnapshotDataSourceConnection(
            real_connection=real,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
        )
        fake_ds_impl = MagicMock()
        fake_ds_impl.data_source_connection = conn
        conn._data_source_impl = fake_ds_impl

        conn.swap_to_real_for_rerun()
        assert fake_ds_impl.data_source_connection is real

        conn.restore_from_rerun()
        assert fake_ds_impl.data_source_connection is conn

    def test_swap_opens_real_via_factory_if_needed(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        factory_calls = []
        real = _make_mock_connection()

        def factory():
            factory_calls.append(True)
            return real

        conn = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
            fallback_connection_factory=factory,
        )
        fake_ds_impl = MagicMock()
        fake_ds_impl.data_source_connection = conn
        conn._data_source_impl = fake_ds_impl

        assert conn.swap_to_real_for_rerun() is True
        assert factory_calls == [True]
        assert fake_ds_impl.data_source_connection is real

    def test_swap_returns_false_for_secondary_wrapper(self, tmp_path):
        """Secondaries aren't installed on a DataSourceImpl — they're returned
        by patched create_additional_connection. The plugin's patch bypasses
        them entirely during a rerun, so the wrapper has nothing to swap."""
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        real = _make_mock_connection()
        primary = SnapshotDataSourceConnection(
            real_connection=real,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
        )
        secondary = SnapshotDataSourceConnection(
            real_connection=None,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
            primary_snapshot=primary,
        )
        assert secondary.swap_to_real_for_rerun() is False

    def test_swap_returns_false_when_no_back_ref(self, tmp_path):
        manager = SnapshotManager("postgres", str(tmp_path / "s"))
        real = _make_mock_connection()
        conn = SnapshotDataSourceConnection(
            real_connection=real,
            snapshot_manager=manager,
            mode="replay",
            allow_fallback=True,
        )
        # No _data_source_impl back-ref set — plugin can't swap.
        assert conn.swap_to_real_for_rerun() is False


class TestStrictMode:
    """``SODA_TEST_SNAPSHOT_STRICT=true`` makes snapshot mismatches fail the
    test instead of triggering a rerun. Used for nightly post-record runs
    that need to verify the freshly-recorded snapshots replay correctly."""

    def test_is_strict_mode_enabled_default_false(self, monkeypatch):
        from helpers.snapshot_connection import is_strict_mode_enabled

        monkeypatch.delenv("SODA_TEST_SNAPSHOT_STRICT", raising=False)
        assert is_strict_mode_enabled() is False

    def test_is_strict_mode_enabled_when_set(self, monkeypatch):
        from helpers.snapshot_connection import is_strict_mode_enabled

        monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "true")
        assert is_strict_mode_enabled() is True

    def test_is_strict_mode_case_insensitive(self, monkeypatch):
        from helpers.snapshot_connection import is_strict_mode_enabled

        monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "TRUE")
        assert is_strict_mode_enabled() is True
        monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "True")
        assert is_strict_mode_enabled() is True

    def test_plugin_rerun_disabled_in_strict_mode(self, monkeypatch):
        """In strict mode the plugin's pytest_runtest_protocol no-ops so
        pytest runs the test once and surfaces the SnapshotReplayError as a
        hard failure."""
        from helpers.snapshot_pytest_plugin import _rerun_mode_enabled

        monkeypatch.setenv("SODA_TEST_SNAPSHOT_RERUN", "true")
        monkeypatch.setenv("SODA_TEST_SNAPSHOT_STRICT", "true")
        assert _rerun_mode_enabled() is False

    def test_plugin_rerun_enabled_when_strict_off(self, monkeypatch):
        from helpers.snapshot_pytest_plugin import _rerun_mode_enabled

        monkeypatch.setenv("SODA_TEST_SNAPSHOT_RERUN", "true")
        monkeypatch.delenv("SODA_TEST_SNAPSHOT_STRICT", raising=False)
        assert _rerun_mode_enabled() is True
