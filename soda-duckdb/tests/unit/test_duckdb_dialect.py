from __future__ import annotations

import duckdb
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBSqlDialect


def test_random():
    sql_dialect: DuckDBSqlDialect = DuckDBSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'


# ---------------------------------------------------------------------------
# select_all_paginated_sql — executed against an in-memory DuckDB so the DISTINCT
# parameter is verified on real SQL semantics, not just on the rendered string.
# ---------------------------------------------------------------------------


def _connection_with_customers():
    connection = duckdb.connect(":memory:")
    connection.execute('CREATE TABLE "CUSTOMERS" ("id" INTEGER, "country" VARCHAR)')
    connection.execute("INSERT INTO \"CUSTOMERS\" VALUES (1, 'BE'), (1, 'BE'), (2, 'BE'), (3, 'BE'), (4, 'NL')")
    return connection


def _page(connection, *, distinct: bool, limit: int, offset: int, filter: str | None = None) -> list:
    sql = DuckDBSqlDialect().select_all_paginated_sql(
        dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=[], dataset_name="CUSTOMERS"),
        columns=["id"],
        filter=filter,
        order_by=["id"],
        limit=limit,
        offset=offset,
        distinct=distinct,
    )
    return connection.execute(sql.rstrip(";")).fetchall()


def test_select_all_paginated_sql_without_distinct_keeps_duplicates():
    connection = _connection_with_customers()
    assert _page(connection, distinct=False, limit=10, offset=0) == [(1,), (1,), (2,), (3,), (4,)]


def test_select_all_paginated_sql_with_distinct_deduplicates():
    connection = _connection_with_customers()
    assert _page(connection, distinct=True, limit=10, offset=0) == [(1,), (2,), (3,), (4,)]


def test_select_all_paginated_sql_with_distinct_pages():
    """De-duplication happens before LIMIT/OFFSET, so pages carry no duplicates and
    don't overlap."""
    connection = _connection_with_customers()
    assert _page(connection, distinct=True, limit=2, offset=0) == [(1,), (2,)]
    assert _page(connection, distinct=True, limit=2, offset=2) == [(3,), (4,)]


def test_select_all_paginated_sql_with_distinct_and_filter():
    connection = _connection_with_customers()
    assert _page(connection, distinct=True, limit=10, offset=0, filter="\"country\" = 'BE'") == [(1,), (2,), (3,)]
