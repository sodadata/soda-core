from __future__ import annotations

import duckdb
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.sql_dialect import COLUMN, FROM, RANDOM, REGEX_LIKE, SELECT
from soda_core.common.statements.metadata_primary_keys_query import MetadataPrimaryKeysQuery
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


def test_primary_keys_query_uses_ansi_key_column_usage():
    # DuckDB's key_column_usage carries ordinal_position (unlike constraint_column_usage), so the
    # primary-key query uses the ANSI base query to return composite keys in order.
    dialect = DuckDBSqlDialect()
    query = MetadataPrimaryKeysQuery(sql_dialect=dialect, data_source_connection=None)
    namespace = query._build_namespace(["myschema"])
    sql = dialect.build_select_sql(query.build_sql_statement(namespace, ["orders"]))
    assert '"information_schema"."table_constraints"' in sql
    assert '"information_schema"."key_column_usage"' in sql
    assert "ordinal_position" in sql


def test_regex_like_pattern_goes_through_literal_string():
    """DuckDBSqlDialect overrides _build_regex_like_sql (REGEXP_MATCHES), so it needs
    its own copy of the base-dialect guarantee. DuckDB literals are standard
    conforming, so the backslash passes through unchanged -- but an apostrophe in a
    user pattern still breaks the query unless it is escaped. See SCS-1413.
    """
    sql_dialect = DuckDBSqlDialect()
    assert sql_dialect.build_expression_sql(REGEX_LIKE(COLUMN("c"), r"^1\.5$")) == "REGEXP_MATCHES(\"c\", '^1\\.5$')"
    assert sql_dialect.build_expression_sql(REGEX_LIKE(COLUMN("c"), "^it's$")) == "REGEXP_MATCHES(\"c\", '^it''s$')"
