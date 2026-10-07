"""The data-plane statement nodes: UPDATE, DELETE, the upserts, index and statistics DDL, the clauses
they compose, and the expressions their values are computed with.

The ANSI expressions render on every dialect. The statements and the dialect-specific expressions
render only on a dialect class that sets `SUPPORTS_DATA_PLANE_STATEMENTS` in its own body, today
Postgres and SQL Server. Three things are pinned. The exact Postgres SQL of every node, for an
application that runs these statements as rendered (SQL Server's is pinned in soda-sqlserver). That
every other dialect renders the ANSI expressions the same way. And that it refuses the rest with
`UnsupportedSqlStatementError` instead of rendering, so a dialect nobody has verified never emits a
statement nobody has run; the optimisations, CREATE_INDEX_IF_NOT_EXISTS and ANALYZE_TABLE, render
None instead.
"""

from __future__ import annotations

from importlib.metadata import entry_points

import pytest
from soda_core.common.exceptions import UnsupportedSqlStatementError
from soda_core.common.metadata_types import SqlDataType
from soda_core.common.sql_ast import (
    ABS,
    ALTER_TABLE_ADD_COLUMN,
    ANALYZE_TABLE,
    AND,
    ARITHMETIC,
    ASSIGNMENT,
    CASE_WHEN,
    CAST,
    COLUMN,
    CREATE_INDEX_IF_NOT_EXISTS,
    CREATE_SCHEMA_IF_NOT_EXISTS,
    CREATE_TABLE,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    CURRENT_TIMESTAMP,
    DATE_ISO_TEXT,
    DATE_TRUNC,
    DELETE,
    EPOCH_SECONDS,
    EQ,
    FROM,
    GT,
    IN,
    INSERT_INTO,
    IS_NOT_NULL,
    JOIN,
    JSON_MERGE,
    LIKE,
    LITERAL,
    LOWER,
    NULLIF,
    PLACEHOLDER,
    REGEXP_REPLACE,
    SELECT,
    SOURCE_COLUMN,
    STAR,
    TIMESTAMP_ISO_TEXT,
    TO_TIMEZONE,
    UPDATE,
    UPSERT,
    UPSERT_VIA_SELECT,
    VALUES_ROW,
    WHERE,
)
from soda_core.common.sql_dialect import SqlDialect

TABLE = '"s"."t"'


@pytest.fixture
def postgres_dialect() -> SqlDialect:
    module = pytest.importorskip("soda_postgres.common.data_sources.postgres_data_source")
    return module.PostgresSqlDialect()


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------


def test_update_with_alias_from_and_where(postgres_dialect):
    sql = postgres_dialect.build_update_sql(
        UPDATE(
            fully_qualified_table_name=TABLE,
            alias="tgt",
            assignments=[
                ASSIGNMENT("name", COLUMN("name", table_alias="src")),
                ASSIGNMENT(COLUMN("n"), LITERAL(1)),
            ],
            from_elements=[
                FROM("src_table", table_prefix=["s"], alias="src"),
                JOIN("other", table_prefix=["s"], alias="o", on_condition=EQ(COLUMN("id", "o"), COLUMN("id", "src"))),
            ],
            where=WHERE(AND([EQ(COLUMN("id", "tgt"), COLUMN("id", "src")), IS_NOT_NULL(COLUMN("name", "src"))])),
        )
    )

    assert sql == (
        'UPDATE "s"."t" AS "tgt"\n'
        'SET "name" = "src"."name", "n" = 1\n'
        'FROM "s"."src_table" AS "src"\n'
        '     JOIN "s"."other" AS "o" ON "o"."id" = "src"."id"\n'
        'WHERE "tgt"."id" = "src"."id" AND "src"."name" IS NOT NULL;'
    )


def test_update_from_several_tables_is_comma_separated(postgres_dialect):
    sql = postgres_dialect.build_update_sql(
        UPDATE(
            fully_qualified_table_name=TABLE,
            alias="tgt",
            assignments=[ASSIGNMENT("n", COLUMN("n", "a"))],
            from_elements=[FROM("a", table_prefix=["s"], alias="a"), FROM("b", table_prefix=["s"], alias="b")],
            where=WHERE(EQ(COLUMN("id", "a"), COLUMN("id", "b"))),
        )
    )

    assert sql == (
        'UPDATE "s"."t" AS "tgt"\n'
        'SET "n" = "a"."n"\n'
        'FROM "s"."a" AS "a",\n'
        '     "s"."b" AS "b"\n'
        'WHERE "a"."id" = "b"."id";'
    )


def test_update_without_from_or_where(postgres_dialect):
    sql = postgres_dialect.build_update_sql(UPDATE(TABLE, [ASSIGNMENT("name", LITERAL("x"))]))

    assert sql == 'UPDATE "s"."t"\nSET "name" = \'x\';'


def test_update_without_semicolon(postgres_dialect):
    sql = postgres_dialect.build_update_sql(UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(None))]), add_semicolon=False)

    assert sql == 'UPDATE "s"."t"\nSET "n" = NULL'


def test_an_assignment_target_cannot_be_qualified():
    # Postgres rejects `SET "tgt"."n" = ...`; refusing at construction keeps the node portable.
    with pytest.raises(ValueError, match="cannot be qualified"):
        ASSIGNMENT(COLUMN("n", table_alias="tgt"), LITERAL(1))


def test_an_assignment_renders_as_an_expression(postgres_dialect):
    assert postgres_dialect.build_expression_sql(ASSIGNMENT("n", LITERAL(2))) == '"n" = 2'


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------


def test_delete_with_using(postgres_dialect):
    sql = postgres_dialect.build_delete_sql(
        DELETE(
            fully_qualified_table_name=TABLE,
            alias="tgt",
            using_elements=[FROM("u", table_prefix=["s"], alias="u")],
            where=WHERE(AND([EQ(COLUMN("id", "tgt"), COLUMN("id", "u")), GT(COLUMN("n", "u"), LITERAL(5))])),
        )
    )

    assert sql == (
        'DELETE FROM "s"."t" AS "tgt"\n' 'USING "s"."u" AS "u"\n' 'WHERE "tgt"."id" = "u"."id" AND "u"."n" > 5;'
    )


def test_delete_without_using(postgres_dialect):
    sql = postgres_dialect.build_delete_sql(DELETE(TABLE, where=WHERE(IN(COLUMN("id"), [LITERAL(1), LITERAL(2)]))))

    assert sql == 'DELETE FROM "s"."t"\nWHERE "id" IN (1, 2);'


def test_delete_everything(postgres_dialect):
    assert postgres_dialect.build_delete_sql(DELETE(TABLE)) == 'DELETE FROM "s"."t";'


# ---------------------------------------------------------------------------
# UPSERT / UPSERT_VIA_SELECT
# ---------------------------------------------------------------------------


def test_upsert_updates_a_matching_row_from_the_incoming_one(postgres_dialect):
    sql = postgres_dialect.build_upsert_sql(
        UPSERT(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("name"), COLUMN("n")],
            values=[
                VALUES_ROW([LITERAL(1), LITERAL("a"), LITERAL(10)]),
                VALUES_ROW([LITERAL(2), LITERAL("it's"), LITERAL(None)]),
            ],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("name", SOURCE_COLUMN("name")), ASSIGNMENT("n", SOURCE_COLUMN("n"))],
        )
    )

    assert sql == (
        'INSERT INTO "s"."t" ("id", "name", "n") VALUES\n'
        "(1, 'a', 10),\n"
        "(2, 'it''s', NULL)\n"
        'ON CONFLICT ("id") DO UPDATE SET "name" = EXCLUDED."name", "n" = EXCLUDED."n";'
    )


@pytest.mark.parametrize("update_assignments", [None, []], ids=["none", "empty"])
def test_upsert_without_assignments_skips_a_matching_row(postgres_dialect, update_assignments):
    sql = postgres_dialect.build_upsert_sql(
        UPSERT(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("name")],
            values=[VALUES_ROW([LITERAL(1), LITERAL("a")])],
            key_columns=[COLUMN("id"), COLUMN("name")],
            update_assignments=update_assignments,
        )
    )

    assert sql == ('INSERT INTO "s"."t" ("id", "name") VALUES\n' "(1, 'a')\n" 'ON CONFLICT ("id", "name") DO NOTHING;')


def test_upsert_via_select(postgres_dialect):
    sql = postgres_dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[SELECT([COLUMN("id"), COLUMN("n")]), FROM("src", table_prefix=["s"])],
            columns=[COLUMN("id"), COLUMN("n")],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("n", SOURCE_COLUMN("n"))],
        )
    )

    assert sql == (
        'INSERT INTO "s"."t"\n'
        ' ("id", "n")\n'
        "(\n"
        'SELECT "id",\n'
        '       "n"\n'
        'FROM "s"."src"\n'
        ")\n"
        'ON CONFLICT ("id") DO UPDATE SET "n" = EXCLUDED."n";'
    )


def test_upsert_via_select_without_assignments(postgres_dialect):
    sql = postgres_dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[SELECT([COLUMN("id")]), FROM("src", table_prefix=["s"])],
            columns=[COLUMN("id")],
            key_columns=[COLUMN("id")],
        ),
        add_semicolon=False,
    )

    assert sql == (
        'INSERT INTO "s"."t"\n'
        ' ("id")\n'
        "(\n"
        'SELECT "id"\n'
        'FROM "s"."src"\n'
        ")\n"
        'ON CONFLICT ("id") DO NOTHING'
    )


def test_source_column_is_the_excluded_row(postgres_dialect):
    assert postgres_dialect.build_expression_sql(SOURCE_COLUMN("name")) == 'EXCLUDED."name"'


def test_upsert_with_an_alias_reads_the_existing_row(postgres_dialect):
    sql = postgres_dialect.build_upsert_sql(
        UPSERT(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("fix_count"), COLUMN("attrs"), COLUMN("updated_at")],
            values=[VALUES_ROW([LITERAL(1), LITERAL(0), CAST(LITERAL('{"a": 1}'), "jsonb"), CURRENT_TIMESTAMP()])],
            key_columns=[COLUMN("id")],
            update_assignments=[
                ASSIGNMENT("fix_count", ARITHMETIC("+", COLUMN("fix_count", "cur"), LITERAL(1))),
                ASSIGNMENT("attrs", JSON_MERGE(COLUMN("attrs", "cur"), SOURCE_COLUMN("attrs"))),
                ASSIGNMENT("updated_at", CURRENT_TIMESTAMP()),
            ],
            alias="cur",
        )
    )

    assert sql == (
        'INSERT INTO "s"."t" AS "cur" ("id", "fix_count", "attrs", "updated_at") VALUES\n'
        "(1, 0, '{\"a\": 1}'::jsonb, CURRENT_TIMESTAMP)\n"
        'ON CONFLICT ("id") DO UPDATE SET "fix_count" = ("cur"."fix_count" + 1), '
        '"attrs" = ("cur"."attrs" || EXCLUDED."attrs"), "updated_at" = CURRENT_TIMESTAMP;'
    )


def test_upsert_via_select_with_an_alias_reads_the_existing_row(postgres_dialect):
    sql = postgres_dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[SELECT([COLUMN("id"), COLUMN("n")]), FROM("src", table_prefix=["s"])],
            columns=[COLUMN("id"), COLUMN("n")],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("n", ARITHMETIC("+", COLUMN("n", "cur"), SOURCE_COLUMN("n")))],
            alias="cur",
        )
    )

    assert sql == (
        'INSERT INTO "s"."t" AS "cur"\n'
        ' ("id", "n")\n'
        "(\n"
        'SELECT "id",\n'
        '       "n"\n'
        'FROM "s"."src"\n'
        ")\n"
        'ON CONFLICT ("id") DO UPDATE SET "n" = ("cur"."n" + EXCLUDED."n");'
    )


# ---------------------------------------------------------------------------
# CREATE INDEX / ANALYZE / CREATE SCHEMA
# ---------------------------------------------------------------------------


def test_create_index_if_not_exists(postgres_dialect):
    sql = postgres_dialect.build_create_index_sql(
        CREATE_INDEX_IF_NOT_EXISTS(
            index_name="t_name_idx", fully_qualified_table_name=TABLE, columns=["name", COLUMN("id")]
        )
    )

    assert sql == 'CREATE INDEX IF NOT EXISTS "t_name_idx" ON "s"."t" ("name", "id");'


def test_an_index_column_must_be_named(postgres_dialect):
    with pytest.raises(ValueError, match="must be named"):
        postgres_dialect.build_create_index_sql(CREATE_INDEX_IF_NOT_EXISTS("i", TABLE, [COLUMN(LOWER(COLUMN("name")))]))


def test_analyze_table(postgres_dialect):
    assert postgres_dialect.build_analyze_table_sql(ANALYZE_TABLE(TABLE)) == 'ANALYZE "s"."t";'


def test_analyze_table_quotes_the_table_for_ddl(postgres_dialect):
    class BacktickDdlPostgres(type(postgres_dialect), sqlglot_dialect="postgres"):
        SUPPORTS_DATA_PLANE_STATEMENTS = True

        def quote_for_ddl(self, identifier):
            return f"`{identifier}`"

    sql = BacktickDdlPostgres().build_analyze_table_sql(ANALYZE_TABLE(TABLE), add_semicolon=False)

    assert sql == "ANALYZE `s`.`t`"


def test_create_schema_is_the_existing_statement(postgres_dialect):
    sql = postgres_dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(prefixes=["db", "s"]))

    assert sql == 'CREATE SCHEMA IF NOT EXISTS "s" AUTHORIZATION CURRENT_USER;'
    assert sql == postgres_dialect.create_schema_if_not_exists_sql(["db", "s"])


# ---------------------------------------------------------------------------
# CREATE TABLE update_heavy
# ---------------------------------------------------------------------------


def _create_table(update_heavy: bool | None = None, node: type = CREATE_TABLE_IF_NOT_EXISTS):
    kwargs = {} if update_heavy is None else {"update_heavy": update_heavy}
    return node(
        fully_qualified_table_name=TABLE,
        columns=[
            CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name="integer")),
            CREATE_TABLE_COLUMN(name="name", type=SqlDataType(name="varchar", character_maximum_length=255)),
        ],
        primary_key_column_names=["id"],
        **kwargs,
    )


def test_create_table_update_heavy_lowers_the_fillfactor(postgres_dialect):
    assert postgres_dialect.build_create_table_sql(_create_table(update_heavy=True)) == (
        'CREATE TABLE IF NOT EXISTS "s"."t" (\n'
        '\t"id" integer NOT NULL,\n'
        '\t"name" varchar(255),\n'
        '\tPRIMARY KEY ("id")\n'
        ") WITH (fillfactor = 70);"
    )


def test_create_table_update_heavy_without_if_not_exists(postgres_dialect):
    assert postgres_dialect.build_create_table_sql(_create_table(update_heavy=True, node=CREATE_TABLE)).endswith(
        ") WITH (fillfactor = 70);"
    )


@pytest.mark.parametrize("update_heavy", [None, False], ids=["omitted", "false"])
def test_create_table_default_is_unchanged(postgres_dialect, update_heavy):
    assert postgres_dialect.build_create_table_sql(_create_table(update_heavy=update_heavy)) == (
        'CREATE TABLE IF NOT EXISTS "s"."t" (\n'
        '\t"id" integer NOT NULL,\n'
        '\t"name" varchar(255),\n'
        '\tPRIMARY KEY ("id")\n'
        ");"
    )


# ---------------------------------------------------------------------------
# Read-only transactions, LIKE ... ESCAPE, a table-qualified star
# ---------------------------------------------------------------------------


def test_read_only_transaction_statements(postgres_dialect):
    assert postgres_dialect.begin_read_only_transaction_sql() == "BEGIN READ ONLY"
    assert postgres_dialect.rollback_sql() == "ROLLBACK"


def test_like_with_an_escape_character(postgres_dialect):
    sql = postgres_dialect.build_expression_sql(LIKE(LOWER(COLUMN("name")), LITERAL("%50\\%%"), escape="\\"))

    assert sql == "LOWER(\"name\") like '%50\\%%' ESCAPE '\\'"


def test_like_with_an_escape_is_a_plain_like_plus_the_escape_clause(postgres_dialect):
    plain = postgres_dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("a!_%")))
    escaped = postgres_dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("a!_%"), escape="!"))

    assert escaped == f"{plain} ESCAPE '!'"


def test_like_without_an_escape_is_unchanged(postgres_dialect):
    assert postgres_dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("%a%"))) == "\"name\" like '%a%'"


def test_a_star_can_be_table_qualified(postgres_dialect):
    assert postgres_dialect.build_expression_sql(STAR("fr")) == '"fr".*'


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------

_POSTGRES_EXPRESSIONS = [
    ("CURRENT_TIMESTAMP", CURRENT_TIMESTAMP(), "CURRENT_TIMESTAMP"),
    ("NULLIF", NULLIF(COLUMN("n"), LITERAL(0)), 'NULLIF("n", 0)'),
    ("ABS", ABS(COLUMN("n")), 'ABS("n")'),
    ("ARITHMETIC", ARITHMETIC("+", COLUMN("fix_count", "cur"), LITERAL(1)), '("cur"."fix_count" + 1)'),
    (
        "ARITHMETIC nested",
        ARITHMETIC("/", ARITHMETIC("-", COLUMN("a"), COLUMN("b")), ARITHMETIC("*", LITERAL(2), COLUMN("c"))),
        '(("a" - "b") / (2 * "c"))',
    ),
    ("PLACEHOLDER", PLACEHOLDER(), "?"),
    (
        "REGEXP_REPLACE",
        REGEXP_REPLACE(COLUMN("s"), "^(-?[0-9]+)$", "\\1.0"),
        "regexp_replace(\"s\", '^(-?[0-9]+)$', '\\1.0', 'g')",
    ),
    ("REGEXP_REPLACE quotes", REGEXP_REPLACE(COLUMN("s"), "'", "''"), "regexp_replace(\"s\", '''', '''''', 'g')"),
    ("DATE_TRUNC", DATE_TRUNC("hour", COLUMN("ts")), "date_trunc('hour', \"ts\")"),
    ("TO_TIMEZONE", TO_TIMEZONE(COLUMN("ts"), "Europe/Brussels"), "(\"ts\" AT TIME ZONE 'Europe/Brussels')"),
    ("EPOCH_SECONDS", EPOCH_SECONDS(ARITHMETIC("-", COLUMN("b"), COLUMN("a"))), 'EXTRACT(EPOCH FROM ("b" - "a"))'),
    ("TIMESTAMP_ISO_TEXT", TIMESTAMP_ISO_TEXT(COLUMN("ts")), 'to_char("ts", \'YYYY-MM-DD"T"HH24:MI:SS.US\')'),
    (
        "TIMESTAMP_ISO_TEXT whole seconds",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False),
        'to_char("ts", \'YYYY-MM-DD"T"HH24:MI:SS\')',
    ),
    (
        "TIMESTAMP_ISO_TEXT UTC suffix",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), utc_suffix=True),
        'to_char("ts", \'YYYY-MM-DD"T"HH24:MI:SS.US"+00:00"\')',
    ),
    (
        "TIMESTAMP_ISO_TEXT whole seconds UTC suffix",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False, utc_suffix=True),
        'to_char("ts", \'YYYY-MM-DD"T"HH24:MI:SS"+00:00"\')',
    ),
    ("DATE_ISO_TEXT", DATE_ISO_TEXT(COLUMN("d")), "to_char(\"d\", 'YYYY-MM-DD')"),
    ("JSON_MERGE", JSON_MERGE(COLUMN("attrs", "cur"), SOURCE_COLUMN("attrs")), '("cur"."attrs" || EXCLUDED."attrs")'),
]


@pytest.mark.parametrize(
    "expression, expected_sql", [case[1:] for case in _POSTGRES_EXPRESSIONS], ids=[c[0] for c in _POSTGRES_EXPRESSIONS]
)
def test_postgres_expression(postgres_dialect, expression, expected_sql):
    assert postgres_dialect.build_expression_sql(expression) == expected_sql


def test_arithmetic_rejects_an_unknown_operator():
    with pytest.raises(ValueError, match="Invalid arithmetic operator '%'"):
        ARITHMETIC("%", COLUMN("a"), LITERAL(2))


def test_date_trunc_rejects_an_unknown_unit():
    with pytest.raises(ValueError, match="Invalid date_trunc unit 'week'"):
        DATE_TRUNC("week", COLUMN("ts"))


@pytest.mark.parametrize("type_name", ["double precision", "numeric", "jsonb", "text"])
def test_cast_to_a_dialect_type_name_renders_it_verbatim(postgres_dialect, type_name):
    # Postgres has always rendered CAST as `::`; the base renders the ANSI form.
    assert postgres_dialect.build_expression_sql(CAST(COLUMN("v"), type_name)) == f'"v"::{type_name}'
    assert SqlDialect().build_expression_sql(CAST(COLUMN("v"), type_name)) == f'CAST("v" AS {type_name})'


def test_case_when_nests_into_a_when_chain(postgres_dialect):
    sql = postgres_dialect.build_expression_sql(
        CASE_WHEN(
            EQ(COLUMN("s"), LITERAL("a")),
            LITERAL(1),
            CASE_WHEN(EQ(COLUMN("s"), LITERAL("b")), LITERAL(2), LITERAL(0)),
        )
    )

    assert sql == "CASE WHEN \"s\" = 'a' THEN 1 ELSE CASE WHEN \"s\" = 'b' THEN 2 ELSE 0 END END"


def test_insert_values_can_be_expressions(postgres_dialect):
    sql = postgres_dialect.build_insert_into_sql(
        INSERT_INTO(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("name"), COLUMN("n"), COLUMN("at"), COLUMN("total")],
            values=[VALUES_ROW([LITERAL(1), "a", None, CURRENT_TIMESTAMP(), ARITHMETIC("*", LITERAL(2), LITERAL(3))])],
        )
    )

    assert sql == (
        'INSERT INTO "s"."t" ("id", "name", "n", "at", "total") VALUES\n' "(1, 'a', NULL, CURRENT_TIMESTAMP, (2 * 3));"
    )


def test_a_column_default_can_be_an_expression(postgres_dialect):
    sql = postgres_dialect.build_create_table_sql(
        CREATE_TABLE_IF_NOT_EXISTS(
            fully_qualified_table_name=TABLE,
            columns=[
                CREATE_TABLE_COLUMN(
                    name="created_at", type=SqlDataType(name="timestamptz"), default=CURRENT_TIMESTAMP()
                ),
                CREATE_TABLE_COLUMN(name="status", type=SqlDataType(name="text"), default="open"),
            ],
        )
    )

    assert sql == (
        'CREATE TABLE IF NOT EXISTS "s"."t" (\n'
        '\t"created_at" timestamptz DEFAULT CURRENT_TIMESTAMP,\n'
        "\t\"status\" text DEFAULT 'open'\n"
        ");"
    )


def test_an_added_column_default_can_be_an_expression(postgres_dialect):
    sql = postgres_dialect.build_alter_table_sql(
        ALTER_TABLE_ADD_COLUMN(
            fully_qualified_table_name=TABLE,
            column=CREATE_TABLE_COLUMN(
                name="updated_at", type=SqlDataType(name="timestamptz"), default=CURRENT_TIMESTAMP()
            ),
        )
    )

    assert sql == 'ALTER TABLE "s"."t" ADD COLUMN "updated_at" timestamptz DEFAULT CURRENT_TIMESTAMP;'


# ---------------------------------------------------------------------------
# Every other dialect renders the ANSI expressions and refuses the rest
# ---------------------------------------------------------------------------

# Every dialect class shipped in soda-core other than Postgres and SQL Server, including the ones
# derived from another (Hive from Databricks, Fabric and Synapse from SQL Server, SparkDF from
# Databricks).
_UNSUPPORTED_DIALECT_CASES = [
    ("duckdb", "soda_duckdb.common.data_sources.duckdb_data_source", "DuckDBSqlDialect"),
    ("snowflake", "soda_snowflake.common.data_sources.snowflake_data_source", "SnowflakeSqlDialect"),
    ("bigquery", "soda_bigquery.common.data_sources.bigquery_data_source", "BigQuerySqlDialect"),
    ("databricks", "soda_databricks.common.data_sources.databricks_data_source", "DatabricksSqlDialect"),
    ("databricks_hive", "soda_databricks.common.data_sources.databricks_data_source", "DatabricksHiveSqlDialect"),
    ("redshift", "soda_redshift.common.data_sources.redshift_data_source", "RedshiftSqlDialect"),
    ("athena", "soda_athena.common.data_sources.athena_data_source", "AthenaSqlDialect"),
    ("fabric", "soda_fabric.common.data_sources.fabric_data_source", "FabricSqlDialect"),
    ("synapse", "soda_synapse.common.data_sources.synapse_data_source", "SynapseSqlDialect"),
    ("sparkdf", "soda_sparkdf.common.data_sources.sparkdf_data_source", "SparkDataFrameSqlDialect"),
    ("trino", "soda_trino.common.data_sources.trino_data_source", "TrinoSqlDialect"),
]


@pytest.fixture(params=_UNSUPPORTED_DIALECT_CASES, ids=[case[0] for case in _UNSUPPORTED_DIALECT_CASES])
def unsupported_dialect(request) -> SqlDialect:
    _, module_path, class_name = request.param
    return getattr(pytest.importorskip(module_path), class_name)()


@pytest.fixture(params=["base", "unverified_postgres_derivative"])
def unsupported_core_dialect(request, postgres_dialect) -> SqlDialect:
    if request.param == "base":
        return SqlDialect()

    class UnverifiedPostgresDerivative(type(postgres_dialect), sqlglot_dialect="postgres"):
        pass

    return UnverifiedPostgresDerivative()


_STATEMENTS = [
    ("UPDATE", lambda d: d.build_update_sql(UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(1))]))),
    ("DELETE", lambda d: d.build_delete_sql(DELETE(TABLE))),
    (
        "UPSERT",
        lambda d: d.build_upsert_sql(UPSERT(TABLE, [COLUMN("id")], [VALUES_ROW([LITERAL(1)])], [COLUMN("id")])),
    ),
    (
        "UPSERT_VIA_SELECT",
        lambda d: d.build_upsert_via_select_sql(
            UPSERT_VIA_SELECT(TABLE, [SELECT([COLUMN("id")]), FROM("src")], [COLUMN("id")], [COLUMN("id")])
        ),
    ),
    (
        "CREATE_SCHEMA_IF_NOT_EXISTS",
        lambda d: d.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(["db", "s"])),
    ),
    ("ASSIGNMENT", lambda d: d.build_expression_sql(ASSIGNMENT("n", LITERAL(1)))),
    ("SOURCE_COLUMN", lambda d: d.build_expression_sql(SOURCE_COLUMN("n"))),
    ("LIKE ESCAPE", lambda d: d.build_expression_sql(LIKE(COLUMN("n"), LITERAL("a!%"), escape="!"))),
]

_DIALECT_SPECIFIC_EXPRESSIONS = [
    ("REGEXP_REPLACE", REGEXP_REPLACE(COLUMN("s"), "[0-9]", "")),
    ("DATE_TRUNC", DATE_TRUNC("day", COLUMN("ts"))),
    ("TO_TIMEZONE", TO_TIMEZONE(COLUMN("ts"), "UTC")),
    ("EPOCH_SECONDS", EPOCH_SECONDS(COLUMN("i"))),
    ("TIMESTAMP_ISO_TEXT", TIMESTAMP_ISO_TEXT(COLUMN("ts"))),
    ("DATE_ISO_TEXT", DATE_ISO_TEXT(COLUMN("d"))),
    ("JSON_MERGE", JSON_MERGE(COLUMN("a"), COLUMN("b"))),
]

_REFUSED = _STATEMENTS + [
    (node_name, lambda d, expression=expression: d.build_expression_sql(expression))
    for node_name, expression in _DIALECT_SPECIFIC_EXPRESSIONS
]

# Optimisations: a dialect that does not render one returns None, and the caller skips it.
_OPTIONAL_STATEMENTS = [
    ("CREATE_INDEX_IF_NOT_EXISTS", lambda d: d.build_create_index_sql(CREATE_INDEX_IF_NOT_EXISTS("i", TABLE, ["id"]))),
    ("ANALYZE_TABLE", lambda d: d.build_analyze_table_sql(ANALYZE_TABLE(TABLE))),
]
_OPTIONAL_STATEMENT_RENDERS = [case[1] for case in _OPTIONAL_STATEMENTS]
_OPTIONAL_STATEMENT_IDS = [case[0] for case in _OPTIONAL_STATEMENTS]


def _assert_refuses(sql_dialect: SqlDialect, node_name: str, render) -> None:
    with pytest.raises(UnsupportedSqlStatementError) as error:
        render(sql_dialect)
    assert str(error.value) == f"{type(sql_dialect).__name__} does not support {node_name}"


@pytest.mark.parametrize("node_name, render", _REFUSED, ids=[case[0] for case in _REFUSED])
def test_other_dialects_refuse_the_data_plane_nodes(unsupported_dialect, node_name, render):
    _assert_refuses(unsupported_dialect, node_name, render)


@pytest.mark.parametrize("node_name, render", _REFUSED, ids=[case[0] for case in _REFUSED])
def test_the_base_and_an_opted_out_derivative_refuse_the_data_plane_nodes(unsupported_core_dialect, node_name, render):
    _assert_refuses(unsupported_core_dialect, node_name, render)


class _OptedInWithoutOverrides(SqlDialect, sqlglot_dialect="postgres"):
    """A dialect that opted in before implementing the dialect-specific hooks."""

    SUPPORTS_DATA_PLANE_STATEMENTS = True


@pytest.mark.parametrize(
    "node_name, expression", _DIALECT_SPECIFIC_EXPRESSIONS, ids=[case[0] for case in _DIALECT_SPECIFIC_EXPRESSIONS]
)
def test_an_opted_in_dialect_refuses_an_expression_it_does_not_implement(node_name, expression):
    _assert_refuses(_OptedInWithoutOverrides(), node_name, lambda d: d.build_expression_sql(expression))


def _assert_renders_the_ansi_expressions(sql_dialect: SqlDialect) -> None:
    n: str = sql_dialect.build_expression_sql(COLUMN("n"))
    zero: str = sql_dialect.build_expression_sql(LITERAL(0))
    assert sql_dialect.build_expression_sql(CURRENT_TIMESTAMP()) == "CURRENT_TIMESTAMP"
    assert sql_dialect.build_expression_sql(NULLIF(COLUMN("n"), LITERAL(0))) == f"NULLIF({n}, {zero})"
    assert sql_dialect.build_expression_sql(ABS(COLUMN("n"))) == f"ABS({n})"
    for operator in ["+", "-", "*", "/"]:
        assert (
            sql_dialect.build_expression_sql(ARITHMETIC(operator, COLUMN("n"), LITERAL(0)))
            == f"({n} {operator} {zero})"
        )
    assert sql_dialect.build_expression_sql(PLACEHOLDER()) == "?"


def test_other_dialects_render_the_ansi_expressions(unsupported_dialect):
    _assert_renders_the_ansi_expressions(unsupported_dialect)


def test_the_base_and_an_opted_out_derivative_render_the_ansi_expressions(unsupported_core_dialect):
    _assert_renders_the_ansi_expressions(unsupported_core_dialect)


@pytest.mark.parametrize("render", _OPTIONAL_STATEMENT_RENDERS, ids=_OPTIONAL_STATEMENT_IDS)
def test_other_dialects_render_no_optional_statement(unsupported_dialect, render):
    assert render(unsupported_dialect) is None


@pytest.mark.parametrize("render", _OPTIONAL_STATEMENT_RENDERS, ids=_OPTIONAL_STATEMENT_IDS)
def test_the_base_and_an_opted_out_derivative_render_no_optional_statement(unsupported_core_dialect, render):
    assert render(unsupported_core_dialect) is None


@pytest.mark.parametrize("render", _OPTIONAL_STATEMENT_RENDERS, ids=_OPTIONAL_STATEMENT_IDS)
def test_an_opted_in_dialect_without_an_override_renders_no_optional_statement(render):
    assert render(_OptedInWithoutOverrides()) is None


def test_other_dialects_offer_no_read_only_transaction(unsupported_dialect):
    assert unsupported_dialect.begin_read_only_transaction_sql() is None
    assert unsupported_dialect.rollback_sql() is None


def test_the_base_and_an_opted_out_derivative_offer_no_read_only_transaction(unsupported_core_dialect):
    assert unsupported_core_dialect.begin_read_only_transaction_sql() is None
    assert unsupported_core_dialect.rollback_sql() is None


def test_other_dialects_ignore_update_heavy(unsupported_dialect):
    if type(unsupported_dialect).__name__ == "AthenaSqlDialect":
        pytest.skip("Athena renders CREATE TABLE only with a storage location")
    assert unsupported_dialect.build_create_table_sql(
        _create_table(update_heavy=True)
    ) == unsupported_dialect.build_create_table_sql(_create_table())


def test_the_base_and_an_opted_out_derivative_ignore_update_heavy(unsupported_core_dialect):
    assert unsupported_core_dialect.build_create_table_sql(
        _create_table(update_heavy=True)
    ) == unsupported_core_dialect.build_create_table_sql(_create_table())


def test_other_dialects_still_render_like_without_an_escape(unsupported_dialect):
    # Only a LIKE that asks for an escape is gated; every existing LIKE renders as before.
    assert "ESCAPE" not in unsupported_dialect.build_expression_sql(LIKE(COLUMN("n"), LITERAL("%a%")))


# ---------------------------------------------------------------------------
# A dialect derived from SQL Server that opts in
# ---------------------------------------------------------------------------

_SQLSERVER_DERIVED_DIALECT_CASES = [case for case in _UNSUPPORTED_DIALECT_CASES if case[0] in ("fabric", "synapse")]


@pytest.mark.parametrize(
    "module_path, class_name",
    [case[1:] for case in _SQLSERVER_DERIVED_DIALECT_CASES],
    ids=[case[0] for case in _SQLSERVER_DERIVED_DIALECT_CASES],
)
def test_a_sqlserver_derivative_that_opts_in_merges_from_its_own_insert_rows(module_path, class_name):
    # These dialects insert rows as SELECT ... UNION ALL; the MERGE source takes the same form.
    class OptedIn(getattr(pytest.importorskip(module_path), class_name), sqlglot_dialect="tsql"):
        SUPPORTS_DATA_PLANE_STATEMENTS = True

    sql = OptedIn().build_upsert_sql(
        UPSERT(
            "[s].[t]",
            [COLUMN("id"), COLUMN("name")],
            [VALUES_ROW([LITERAL(1), LITERAL("a")]), VALUES_ROW([LITERAL(2), LITERAL("b")])],
            [COLUMN("id")],
        )
    )

    assert sql == (
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [tgt]\n"
        "USING (\n"
        "SELECT 1, 'a'\n"
        "UNION ALL SELECT 2, 'b'\n"
        ") AS [src] ([id], [name])\n"
        "ON ([tgt].[id] = [src].[id])\n"
        "WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES ([src].[id], [src].[name]);"
    )


# ---------------------------------------------------------------------------
# The capability flag is not inherited
# ---------------------------------------------------------------------------


def _shipped_dialect_classes() -> set[type]:
    """Every SqlDialect subclass defined in soda-core or in an installed data source package."""
    data_source_modules: set[str] = set()
    for group in entry_points().groups:
        if group.startswith("soda.plugins.data_source."):
            for entry_point in entry_points(group=group):
                entry_point.load()
                data_source_modules.add(entry_point.module)
    packages: set[str] = {"soda_core"} | {module.split(".")[0] for module in data_source_modules}

    dialect_classes: set[type] = set()
    pending: list[type] = SqlDialect.__subclasses__()
    while pending:
        dialect_class: type = pending.pop()
        pending.extend(dialect_class.__subclasses__())
        if dialect_class.__module__.split(".")[0] in packages:
            dialect_classes.add(dialect_class)
    # Each data source module defines its dialect, so a module the walk missed shows up here.
    assert data_source_modules <= {dialect_class.__module__ for dialect_class in dialect_classes}
    return dialect_classes


def test_postgres_and_sqlserver_are_the_only_shipped_dialects_with_the_flag(postgres_dialect):
    sqlserver_module = pytest.importorskip("soda_sqlserver.common.data_sources.sqlserver_data_source")
    flagged = {
        dialect_class for dialect_class in _shipped_dialect_classes() if dialect_class.SUPPORTS_DATA_PLANE_STATEMENTS
    }

    assert flagged == {type(postgres_dialect), sqlserver_module.SqlServerSqlDialect}


def test_a_dialect_derived_from_postgres_has_the_flag_only_when_it_sets_it(postgres_dialect):
    class PostgresDerivative(type(postgres_dialect), sqlglot_dialect="postgres"):
        pass

    class VerifiedPostgresDerivative(type(postgres_dialect), sqlglot_dialect="postgres"):
        SUPPORTS_DATA_PLANE_STATEMENTS = True

    assert PostgresDerivative.SUPPORTS_DATA_PLANE_STATEMENTS is False
    assert VerifiedPostgresDerivative.SUPPORTS_DATA_PLANE_STATEMENTS is True
