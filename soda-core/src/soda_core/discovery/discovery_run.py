# soda-core/src/soda_core/discovery/discovery_run.py
from __future__ import annotations

import fnmatch
from typing import Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier

SODA_TEMP_PREFIX = "__soda_temp"


def _matches_any(name: str, patterns: list[str]) -> bool:
    # v3 discovery uses SQL-style % wildcards; translate to fnmatch's * for matching.
    return any(fnmatch.fnmatch(name.lower(), pattern.replace("%", "*").lower()) for pattern in patterns)


class DiscoveryRun:
    """Discovers dataset names for a data source and returns their DQNs. Not a CheckCollectionImpl."""

    @staticmethod
    def execute(
        data_source_impl: DataSourceImpl,
        prefixes: list[str],
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> list[str]:
        objects = data_source_impl.discover_qualified_objects(prefixes=prefixes)
        objects = [o for o in objects if not o.get_object_name().lower().startswith(SODA_TEMP_PREFIX)]
        if include:
            objects = [o for o in objects if _matches_any(o.get_object_name(), include)]
        if exclude:
            objects = [o for o in objects if not _matches_any(o.get_object_name(), exclude)]
        return [
            DatasetIdentifier.from_object(data_source_impl.name, data_source_impl.sql_dialect, o).to_string()
            for o in objects
        ]
