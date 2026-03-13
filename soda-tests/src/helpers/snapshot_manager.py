from __future__ import annotations

import json
import logging
import os
import pickle
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class SnapshotEntry:
    """A single recorded SQL operation with its result."""

    op_type: str  # "query", "update", "query_one_by_one", "query_iterate"
    sql: str
    result: Any = None  # QueryResult, UpdateResult, (description, rows), etc.


class SnapshotNotFoundError(Exception):
    pass


class SnapshotMismatchError(Exception):
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
            "operations": [
                {"index": i, "type": entry.op_type, "sql": entry.sql} for i, entry in enumerate(recording)
            ],
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
