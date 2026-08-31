from __future__ import annotations

import duckdb
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBSqlDialect

# These exercise `select_all_paginated_sql` by EXECUTING the rendered SQL against an in-memory
# DuckDB, so DISTINCT and the ORDER BY fold are verified on real SQL semantics rather than on a
# rendered string. That makes them integration tests despite the connection being in-process,
# which is why they live here and not under tests/unit — rendering-only goldens for the same
# method are in soda-tests/tests/unit/test_sql_generation.py.


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


# ---------------------------------------------------------------------------
# distinct x normalize_key_columns, executed. The composed shape (de-duplicate in
# a CTE, case-fold the ORDER BY outside it) has to return de-duplicated rows in
# case-insensitive key order — and page them without overlap.
# ---------------------------------------------------------------------------


def _connection_with_codes():
    connection = duckdb.connect(":memory:")
    connection.execute('CREATE TABLE "CODES" ("code" VARCHAR, "label" VARCHAR)')
    connection.execute("INSERT INTO \"CODES\" VALUES ('b', 'x'), ('B', 'x'), ('a', 'y'), ('a', 'y'), ('C', 'z')")
    return connection


def _code_page(connection, *, distinct: bool, normalize: bool, limit: int = 10, offset: int = 0) -> list:
    sql = DuckDBSqlDialect().select_all_paginated_sql(
        dataset_identifier=DatasetIdentifier(data_source_name="ds", prefixes=[], dataset_name="CODES"),
        columns=["code", "label"],
        filter=None,
        order_by=["code"],
        limit=limit,
        offset=offset,
        normalize_key_columns=frozenset({"code"}) if normalize else frozenset(),
        distinct=distinct,
    )
    return connection.execute(sql.rstrip(";")).fetchall()


def test_paginated_neither_distinct_nor_normalized():
    connection = _connection_with_codes()
    # Raw (binary) ordering, duplicates kept.
    assert _code_page(connection, distinct=False, normalize=False) == [
        ("B", "x"),
        ("C", "z"),
        ("a", "y"),
        ("a", "y"),
        ("b", "x"),
    ]


def test_paginated_distinct_only():
    connection = _connection_with_codes()
    assert _code_page(connection, distinct=True, normalize=False) == [("B", "x"), ("C", "z"), ("a", "y"), ("b", "x")]


def test_paginated_normalized_only():
    connection = _connection_with_codes()
    # Case-insensitive order, raw column as tiebreaker ('B' before 'b'), duplicates kept.
    assert _code_page(connection, distinct=False, normalize=True) == [
        ("a", "y"),
        ("a", "y"),
        ("B", "x"),
        ("b", "x"),
        ("C", "z"),
    ]


def test_paginated_distinct_and_normalized():
    connection = _connection_with_codes()
    assert _code_page(connection, distinct=True, normalize=True) == [("a", "y"), ("B", "x"), ("b", "x"), ("C", "z")]


def test_paginated_distinct_and_normalized_pages_without_overlap():
    """De-duplication still happens before LIMIT/OFFSET in the composed shape."""
    connection = _connection_with_codes()
    assert _code_page(connection, distinct=True, normalize=True, limit=2, offset=0) == [("a", "y"), ("B", "x")]
    assert _code_page(connection, distinct=True, normalize=True, limit=2, offset=2) == [("b", "x"), ("C", "z")]
