from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.statements.table_types import TableType

SODA_TEMP_PREFIX = "__soda_temp"


def discover_dataset_dqns(
    data_source_impl: DataSourceImpl,
    prefixes: list[str],
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
) -> list[str]:
    """Discover dataset names within a scope and return their DQNs."""
    # include/exclude are pushed down as SQL LIKE filters. The __soda_temp prefix is
    # filtered in Python instead: in a LIKE pattern its leading underscores would be
    # single-character wildcards.
    objects = data_source_impl.discover_qualified_objects(
        prefixes=prefixes,
        # Every object type, explicitly: discovery matches v3 (no type filter — base
        # tables, views and materialized views alike), while the metadata query's
        # None default is tables-only for backward compatibility of other callers.
        object_types=[TableType.TABLE, TableType.VIEW, TableType.MATERIALIZED_VIEW],
        include_table_name_like_filters=include,
        exclude_table_name_like_filters=exclude,
    )
    objects = [o for o in objects if not o.get_object_name().lower().startswith(SODA_TEMP_PREFIX)]
    return [
        DatasetIdentifier.from_object(data_source_impl.name, data_source_impl.sql_dialect, o).to_string()
        for o in objects
    ]
