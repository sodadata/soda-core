from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier

SODA_TEMP_PREFIX = "__soda_temp"


class DiscoveryRun:
    """Discovers dataset names for a data source and returns their DQNs."""

    @staticmethod
    def execute(
        data_source_impl: DataSourceImpl,
        prefixes: list[str],
        include: Optional[list[str]] = None,
        exclude: Optional[list[str]] = None,
    ) -> list[str]:
        # include/exclude are pushed down as SQL LIKE filters. The __soda_temp prefix is
        # filtered in Python instead: in a LIKE pattern its leading underscores would be
        # single-character wildcards.
        objects = data_source_impl.discover_qualified_objects(
            prefixes=prefixes,
            include_table_name_like_filters=include,
            exclude_table_name_like_filters=exclude,
        )
        objects = [o for o in objects if not o.get_object_name().lower().startswith(SODA_TEMP_PREFIX)]
        return [
            DatasetIdentifier.from_object(data_source_impl.name, data_source_impl.sql_dialect, o).to_string()
            for o in objects
        ]
