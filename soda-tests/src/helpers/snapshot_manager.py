from __future__ import annotations

import json
import logging
import os
import pickle
from dataclasses import dataclass
from datetime import timezone, tzinfo
from typing import Any, Optional

logger = logging.getLogger(__name__)


def _serialize_tzinfo(tz: tzinfo) -> str:
    """Render a ``tzinfo`` as a string parsable by ``parse_session_timezone``.

    ZoneInfo zones serialize to their IANA key (``"America/Los_Angeles"``).
    ``timezone.utc`` and any zero-offset ``datetime.timezone(...)`` serialize as
    ``"UTC"``. Non-zero fixed offsets serialize as ``"±HH:MM"``.
    """
    if tz is timezone.utc:
        return "UTC"
    iana_key = getattr(tz, "key", None)  # ZoneInfo / pytz-style zones expose .key
    if iana_key:
        return iana_key
    offset = tz.utcoffset(None)
    if offset is None:
        return "UTC"
    total_minutes = int(offset.total_seconds() // 60)
    if total_minutes == 0:
        return "UTC"
    sign = "-" if total_minutes < 0 else "+"
    hours, minutes = divmod(abs(total_minutes), 60)
    return f"{sign}{hours:02d}:{minutes:02d}"


@dataclass
class SnapshotEntry:
    """A single recorded SQL operation with its result."""

    op_type: str  # "query", "update", "query_one_by_one", "query_iterate"
    sql: str
    result: Any = None  # QueryResult, (description, rows), etc.


class SnapshotReplayError(Exception):
    """Base class for replay-time failures that the pytest rerun plugin recognises.

    A raised SnapshotReplayError bubbling out of a test (or a test boundary
    transition) is the signal for "this test cannot be served from the
    snapshot — re-run it against the real DB". Two concrete subclasses cover
    the two ways replay can fail: a missing snapshot file, and a stored
    operation that no longer matches what the test now emits.
    """


class SnapshotNotFoundError(SnapshotReplayError):
    pass


class SnapshotMismatchError(SnapshotReplayError):
    pass


class SnapshotManager:
    """Manages snapshot file I/O for test SQL recordings.

    Each test gets two files:
    - .pickle: Full recording (SQL + results) for exact replay
    - .json: Human-readable SQL log for inspection/debugging
    """

    def __init__(self, datasource_type: str, snapshot_dir: str):
        self.datasource_type: str = datasource_type
        self.snapshot_dir: str = snapshot_dir

    def _snapshot_path(self, test_id: str, ext: str) -> str:
        """Convert a pytest node ID to a snapshot file path.

        Example: "tests/integration/test_row_count_check.py::test_row_count"
        becomes: ".test_snapshots/postgres/tests/integration/test_row_count_check.py/test_row_count.pickle"
        """
        # Split on :: to separate file path from test name
        sanitized = test_id.replace("::", os.sep)
        return os.path.join(self.snapshot_dir, self.datasource_type, f"{sanitized}.{ext}")

    def save(self, test_id: str, recording: list[SnapshotEntry]) -> None:
        pickle_path = self._snapshot_path(test_id, "pickle")
        json_path = self._snapshot_path(test_id, "json")

        os.makedirs(os.path.dirname(pickle_path), exist_ok=True)

        # Save pickle (full recording with results for exact replay)
        with open(pickle_path, "wb") as f:
            pickle.dump(recording, f, protocol=pickle.HIGHEST_PROTOCOL)

        # Save JSON (human-readable SQL log for inspection)
        json_data = {
            "datasource_type": self.datasource_type,
            "test_id": test_id,
            "operation_count": len(recording),
            "operations": [{"index": i, "type": entry.op_type, "sql": entry.sql} for i, entry in enumerate(recording)],
        }
        with open(json_path, "w") as f:
            json.dump(json_data, f, indent=2)

        logger.info(f"SNAPSHOT: Saved recording for {test_id} ({len(recording)} operations)")

    def load(self, test_id: str) -> Optional[list[SnapshotEntry]]:
        pickle_path = self._snapshot_path(test_id, "pickle")
        if not os.path.exists(pickle_path):
            return None
        with open(pickle_path, "rb") as f:
            return pickle.load(f)

    def has_snapshot(self, test_id: str) -> bool:
        return os.path.exists(self._snapshot_path(test_id, "pickle"))

    # ------------------------------------------------------------------
    # Session-TZ sidecar
    #
    # Session timezone is connection-scoped (queried once at mapper-construction
    # time, cached for the connection's lifetime), not test-scoped. We persist it
    # in a single file per datasource rather than embedding it in every per-test
    # pickle: that keeps the per-test format unchanged (no migration cost) and
    # avoids redundant per-test storage of a single value.
    #
    # The vendor-specific ``_fetch_session_timezone`` implementations call
    # ``self.connection.cursor()`` directly, bypassing the snapshot's
    # ``execute_query`` / ``execute_update`` interception. So we cannot capture
    # session-TZ replay through the same mechanism as SQL ops; the wrapper has
    # to record the result of the call explicitly via this sidecar.
    # ------------------------------------------------------------------

    def _session_timezone_path(self) -> str:
        return os.path.join(self.snapshot_dir, self.datasource_type, "_session_timezone.json")

    def save_session_timezone(self, tz: tzinfo) -> None:
        path = self._session_timezone_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        payload = {"datasource_type": self.datasource_type, "session_timezone": _serialize_tzinfo(tz)}
        with open(path, "w") as f:
            json.dump(payload, f, indent=2)
        logger.info(f"SNAPSHOT: Saved session timezone {payload['session_timezone']!r} for {self.datasource_type}")

    def load_session_timezone(self) -> Optional[str]:
        """Return the recorded session-TZ string for this datasource, or None.

        The string is suitable for ``parse_session_timezone``; the caller routes
        it through that helper to get a ``tzinfo``.
        """
        path = self._session_timezone_path()
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            payload = json.load(f)
        return payload.get("session_timezone")
