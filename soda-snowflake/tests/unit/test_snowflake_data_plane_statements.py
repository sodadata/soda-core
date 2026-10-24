"""The data-plane statement nodes and expressions as SnowflakeSqlDialect renders them.

Every node is pinned to its exact Snowflake SQL. UPDATE and DELETE are the base forms. The upserts are a
MERGE whose source is a subquery: the rows of an UPSERT as SELECTs joined by UNION ALL, the SELECT of an
UPSERT_VIA_SELECT with any WITH kept inside it. Snowflake creates no index and has no statistics to
refresh, so those render None, and it refuses EPOCH_SECONDS and JSON_MERGE.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from soda_core.common.exceptions import UnsupportedSqlStatementError
from soda_core.common.metadata_types import SqlDataType
from soda_core.common.sql_ast import (
    ABS,
    ANALYZE_TABLE,
    AND,
    ARITHMETIC,
    ASSIGNMENT,
    CAST,
    COLUMN,
    CREATE_INDEX_IF_NOT_EXISTS,
    CREATE_SCHEMA_IF_NOT_EXISTS,
    CREATE_TABLE,
    CREATE_TABLE_COLUMN,
    CREATE_TABLE_IF_NOT_EXISTS,
    CTE,
    CURRENT_TIMESTAMP,
    DATE_ISO_TEXT,
    DATE_TRUNC,
    DATE_TRUNC_UNITS,
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
    REGEXP_REPLACE,
    SELECT,
    SOURCE_COLUMN,
    TIMESTAMP_ISO_TEXT,
    TO_TIMEZONE,
    UPDATE,
    UPSERT,
    UPSERT_VIA_SELECT,
    VALUES_ROW,
    WHERE,
    WITH,
)
from soda_snowflake.common.data_sources.snowflake_data_source import SnowflakeSqlDialect

TABLE = '"s"."t"'


@pytest.fixture
def dialect() -> SnowflakeSqlDialect:
    return SnowflakeSqlDialect()


def test_the_dialect_has_the_flag():
    assert SnowflakeSqlDialect.SUPPORTS_DATA_PLANE_STATEMENTS is True


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------


def test_update_with_alias_from_and_where(dialect):
    sql = dialect.build_update_sql(
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


def test_update_from_several_tables_is_comma_separated(dialect):
    sql = dialect.build_update_sql(
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


def test_update_without_alias_or_from(dialect):
    sql = dialect.build_update_sql(
        UPDATE(TABLE, [ASSIGNMENT("name", LITERAL("x"))], where=WHERE(EQ(COLUMN("id"), LITERAL(1))))
    )

    assert sql == 'UPDATE "s"."t"\nSET "name" = \'x\'\nWHERE "id" = 1;'


def test_update_without_semicolon(dialect):
    sql = dialect.build_update_sql(UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(None))]), add_semicolon=False)

    assert sql == 'UPDATE "s"."t"\nSET "n" = NULL'


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------


def test_delete_with_using(dialect):
    sql = dialect.build_delete_sql(
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


def test_delete_using_several_tables_and_a_join(dialect):
    sql = dialect.build_delete_sql(
        DELETE(
            fully_qualified_table_name=TABLE,
            alias="tgt",
            using_elements=[
                FROM("a", table_prefix=["s"], alias="a"),
                FROM("b", table_prefix=["s"], alias="b"),
                JOIN("o", table_prefix=["s"], alias="o", on_condition=EQ(COLUMN("id", "o"), COLUMN("id", "b"))),
            ],
            where=WHERE(AND([EQ(COLUMN("id", "tgt"), COLUMN("id", "a")), EQ(COLUMN("id", "a"), COLUMN("id", "b"))])),
        )
    )

    assert sql == (
        'DELETE FROM "s"."t" AS "tgt"\n'
        'USING "s"."a" AS "a",\n'
        '      "s"."b" AS "b"\n'
        '      JOIN "s"."o" AS "o" ON "o"."id" = "b"."id"\n'
        'WHERE "tgt"."id" = "a"."id" AND "a"."id" = "b"."id";'
    )


def test_delete_without_using(dialect):
    sql = dialect.build_delete_sql(DELETE(TABLE, where=WHERE(IN(COLUMN("id"), [LITERAL(1), LITERAL(2)]))))

    assert sql == 'DELETE FROM "s"."t"\nWHERE "id" IN (1, 2);'


def test_delete_everything(dialect):
    assert dialect.build_delete_sql(DELETE(TABLE)) == 'DELETE FROM "s"."t";'


def test_delete_without_semicolon(dialect):
    sql = dialect.build_delete_sql(DELETE(TABLE, where=WHERE(EQ(COLUMN("id"), LITERAL(1)))), add_semicolon=False)

    assert sql == 'DELETE FROM "s"."t"\nWHERE "id" = 1'


# ---------------------------------------------------------------------------
# UPSERT / UPSERT_VIA_SELECT
# ---------------------------------------------------------------------------


def test_upsert_updates_a_matching_row_from_the_incoming_one(dialect):
    sql = dialect.build_upsert_sql(
        UPSERT(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("name"), COLUMN("n")],
            values=[
                VALUES_ROW([LITERAL(1), LITERAL("a"), LITERAL(10)]),
                VALUES_ROW([LITERAL(2), LITERAL("it's"), LITERAL(None)]),
                VALUES_ROW([LITERAL(3), LITERAL("c"), LITERAL(30)]),
            ],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("name", SOURCE_COLUMN("name")), ASSIGNMENT("n", SOURCE_COLUMN("n"))],
        )
    )

    assert sql == (
        'MERGE INTO "s"."t" AS "tgt"\n'
        "USING (\n"
        'SELECT 1 AS "id", \'a\' AS "name", 10 AS "n"\n'
        "UNION ALL SELECT 2, 'it''s', NULL\n"
        "UNION ALL SELECT 3, 'c', 30\n"
        ') AS "src"\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "name" = "src"."name", "n" = "src"."n"\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "name", "n") VALUES ("src"."id", "src"."name", "src"."n");'
    )


@pytest.mark.parametrize("update_assignments", [None, []], ids=["none", "empty"])
def test_upsert_without_assignments_skips_a_matching_row(dialect, update_assignments):
    sql = dialect.build_upsert_sql(
        UPSERT(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("name")],
            values=[VALUES_ROW([LITERAL(1), LITERAL("a")])],
            key_columns=[COLUMN("id"), COLUMN("name")],
            update_assignments=update_assignments,
        )
    )

    assert sql == (
        'MERGE INTO "s"."t" AS "tgt"\n'
        "USING (\n"
        'SELECT 1 AS "id", \'a\' AS "name"\n'
        ') AS "src"\n'
        'ON ("tgt"."id" = "src"."id" AND "tgt"."name" = "src"."name")\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "name") VALUES ("src"."id", "src"."name");'
    )


def test_upsert_with_an_alias_reads_the_existing_row(dialect):
    sql = dialect.build_upsert_sql(
        UPSERT(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("fix_count"), COLUMN("attrs"), COLUMN("updated_at")],
            values=[VALUES_ROW([LITERAL(1), LITERAL(0), CAST(LITERAL('{"a": 1}'), "varchar"), CURRENT_TIMESTAMP()])],
            key_columns=[COLUMN("id")],
            update_assignments=[
                ASSIGNMENT("fix_count", ARITHMETIC("+", COLUMN("fix_count", "cur"), LITERAL(1))),
                ASSIGNMENT("updated_at", CURRENT_TIMESTAMP()),
            ],
            alias="cur",
        )
    )

    assert sql == (
        'MERGE INTO "s"."t" AS "cur"\n'
        "USING (\n"
        'SELECT 1 AS "id", 0 AS "fix_count", CAST(\'{"a": 1}\' AS varchar) AS "attrs", '
        'CURRENT_TIMESTAMP AS "updated_at"\n'
        ') AS "src"\n'
        'ON ("cur"."id" = "src"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "fix_count" = ("cur"."fix_count" + 1), "updated_at" = CURRENT_TIMESTAMP\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "fix_count", "attrs", "updated_at") '
        'VALUES ("src"."id", "src"."fix_count", "src"."attrs", "src"."updated_at");'
    )


def test_upsert_renders_each_value_as_an_insert_does(dialect):
    # Bare values and expressions alike: the rows go through the same literal rendering as INSERT INTO,
    # which doubles a backslash for Snowflake.
    columns = [COLUMN(name) for name in ("id", "name", "n", "at", "total", "flag", "ts", "path")]
    row = VALUES_ROW(
        [
            LITERAL(1),
            "a",
            None,
            CURRENT_TIMESTAMP(),
            ARITHMETIC("*", LITERAL(2), LITERAL(3)),
            True,
            datetime(2024, 1, 2, 3, 4, 5),
            "C:\\dir",
        ]
    )
    later_row = VALUES_ROW([2, "b", 1.5, None, 0, False, None, None])

    insert_sql = dialect.build_insert_into_sql(INSERT_INTO(TABLE, [row], columns))
    upsert_sql = dialect.build_upsert_sql(UPSERT(TABLE, columns, [row, later_row], [columns[0]]))

    assert insert_sql == (
        'INSERT INTO "s"."t" ("id", "name", "n", "at", "total", "flag", "ts", "path") VALUES\n'
        "(1, 'a', NULL, CURRENT_TIMESTAMP, (2 * 3), TRUE, '2024-01-02T03:04:05', 'C:\\\\dir');"
    )
    assert upsert_sql == (
        'MERGE INTO "s"."t" AS "tgt"\n'
        "USING (\n"
        'SELECT 1 AS "id", \'a\' AS "name", NULL AS "n", CURRENT_TIMESTAMP AS "at", (2 * 3) AS "total", '
        'TRUE AS "flag", \'2024-01-02T03:04:05\' AS "ts", \'C:\\\\dir\' AS "path"\n'
        "UNION ALL SELECT 2, 'b', 1.5, NULL, 0, FALSE, NULL, NULL\n"
        ') AS "src"\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "name", "n", "at", "total", "flag", "ts", "path") '
        'VALUES ("src"."id", "src"."name", "src"."n", "src"."at", "src"."total", "src"."flag", "src"."ts", '
        '"src"."path");'
    )


def test_upsert_via_select(dialect):
    sql = dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[SELECT([COLUMN("id"), COLUMN("n")]), FROM("src", table_prefix=["s"])],
            columns=[COLUMN("id"), COLUMN("n")],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("n", SOURCE_COLUMN("n"))],
        )
    )

    assert sql == (
        'MERGE INTO "s"."t" AS "tgt"\n'
        "USING (\n"
        'SELECT "id",\n'
        '       "n"\n'
        'FROM "s"."src"\n'
        ') AS "src" ("id", "n")\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "n" = "src"."n"\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "n") VALUES ("src"."id", "src"."n");'
    )


def test_upsert_via_select_without_assignments(dialect):
    sql = dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[SELECT([COLUMN("id")]), FROM("src", table_prefix=["s"])],
            columns=[COLUMN("id")],
            key_columns=[COLUMN("id")],
        ),
        add_semicolon=False,
    )

    assert sql == (
        'MERGE INTO "s"."t" AS "tgt"\n'
        "USING (\n"
        'SELECT "id"\n'
        'FROM "s"."src"\n'
        ') AS "src" ("id")\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN NOT MATCHED THEN INSERT ("id") VALUES ("src"."id")'
    )


def test_upsert_via_select_with_an_alias_reads_the_existing_row(dialect):
    sql = dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[SELECT([LITERAL(2), LITERAL(5)])],
            columns=[COLUMN("id"), COLUMN("n")],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("n", ARITHMETIC("+", COLUMN("n", "cur"), SOURCE_COLUMN("n")))],
            alias="cur",
        )
    )

    assert sql == (
        'MERGE INTO "s"."t" AS "cur"\n'
        "USING (\n"
        "SELECT 2,\n"
        "       5\n"
        ') AS "src" ("id", "n")\n'
        'ON ("cur"."id" = "src"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "n" = ("cur"."n" + "src"."n")\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "n") VALUES ("src"."id", "src"."n");'
    )


def test_upsert_via_select_keeps_a_with_inside_the_merge_source(dialect):
    # Snowflake refuses a WITH in front of a MERGE and takes one inside its USING subquery.
    sql = dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[
                WITH([CTE("k").AS([SELECT([COLUMN("id")]), FROM("keys", table_prefix=["s"])])]),
                SELECT([COLUMN("id")]),
                FROM("k"),
            ],
            columns=[COLUMN("id")],
            key_columns=[COLUMN("id")],
        )
    )

    assert sql == (
        'MERGE INTO "s"."t" AS "tgt"\n'
        "USING (\n"
        "WITH \n"
        '"k" AS (\n'
        'SELECT "id"\n'
        'FROM "s"."keys"\n'
        ")\n"
        'SELECT "id"\n'
        'FROM "k"\n'
        ') AS "src" ("id")\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN NOT MATCHED THEN INSERT ("id") VALUES ("src"."id");'
    )


_MINIMAL_UPSERT_SQL = (
    'MERGE INTO "s"."t" AS "tgt"\n'
    "USING (\n"
    'SELECT 1 AS "id"\n'
    ') AS "src"\n'
    'ON ("tgt"."id" = "src"."id")\n'
    'WHEN NOT MATCHED THEN INSERT ("id") VALUES ("src"."id")'
)
_MINIMAL_UPSERT_VIA_SELECT_SQL = (
    'MERGE INTO "s"."t" AS "tgt"\n'
    "USING (\n"
    'SELECT "id"\n'
    'FROM "src"\n'
    ') AS "src" ("id")\n'
    'ON ("tgt"."id" = "src"."id")\n'
    'WHEN NOT MATCHED THEN INSERT ("id") VALUES ("src"."id")'
)
_UPSERT_RENDERS = [
    (
        "UPSERT",
        lambda d, add_semicolon: d.build_upsert_sql(
            UPSERT(TABLE, [COLUMN("id")], [VALUES_ROW([LITERAL(1)])], [COLUMN("id")]), add_semicolon=add_semicolon
        ),
        _MINIMAL_UPSERT_SQL,
    ),
    (
        "UPSERT_VIA_SELECT",
        lambda d, add_semicolon: d.build_upsert_via_select_sql(
            UPSERT_VIA_SELECT(TABLE, [SELECT([COLUMN("id")]), FROM("src")], [COLUMN("id")], [COLUMN("id")]),
            add_semicolon=add_semicolon,
        ),
        _MINIMAL_UPSERT_VIA_SELECT_SQL,
    ),
]


@pytest.mark.parametrize(
    "render, expected_sql", [case[1:] for case in _UPSERT_RENDERS], ids=[case[0] for case in _UPSERT_RENDERS]
)
@pytest.mark.parametrize("add_semicolon, semicolon", [(None, ";"), (True, ";"), (False, "")])
def test_the_merge_ends_with_a_semicolon_as_any_statement_does(dialect, render, expected_sql, add_semicolon, semicolon):
    assert render(dialect, add_semicolon) == expected_sql + semicolon


_RENDER_ALIASED_UPSERT = pytest.mark.parametrize(
    "render",
    [
        lambda d, alias: d.build_upsert_sql(
            UPSERT(TABLE, [COLUMN("id")], [VALUES_ROW([LITERAL(1)])], [COLUMN("id")], alias=alias)
        ),
        lambda d, alias: d.build_upsert_via_select_sql(
            UPSERT_VIA_SELECT(TABLE, [SELECT([COLUMN("id")]), FROM("u")], [COLUMN("id")], [COLUMN("id")], alias=alias)
        ),
    ],
    ids=["UPSERT", "UPSERT_VIA_SELECT"],
)


@_RENDER_ALIASED_UPSERT
def test_an_upsert_target_cannot_be_aliased_as_the_incoming_rows(dialect, render):
    with pytest.raises(UnsupportedSqlStatementError) as error:
        render(dialect, "src")

    assert str(error.value) == (
        "SnowflakeSqlDialect does not support an upsert target alias 'src': it names the incoming rows"
    )


@_RENDER_ALIASED_UPSERT
def test_an_upsert_target_alias_differing_only_in_case_from_the_incoming_rows_is_allowed(dialect, render):
    # A quoted identifier is case-sensitive.
    sql = render(dialect, "SRC")

    assert sql.startswith('MERGE INTO "s"."t" AS "SRC"\n')
    assert 'ON ("SRC"."id" = "src"."id")\n' in sql


@pytest.mark.parametrize(
    "columns, key_columns",
    [([COLUMN(LOWER(COLUMN("id")))], [COLUMN("id")]), ([COLUMN("id")], [COLUMN(LOWER(COLUMN("id")))])],
    ids=["column", "key column"],
)
def test_an_upsert_column_must_be_named(dialect, columns, key_columns):
    upsert = UPSERT(TABLE, columns, [VALUES_ROW([LITERAL(1)])], key_columns)

    with pytest.raises(ValueError, match="An upsert column must be named"):
        dialect.build_upsert_sql(upsert)


@pytest.mark.parametrize(
    "rows, message",
    [
        ([[1]], "An upsert row must have one value per column, got 1 for 2 in values[0]"),
        ([[1, "a", 2]], "An upsert row must have one value per column, got 3 for 2 in values[0]"),
        ([[1, "a"], [3]], "An upsert row must have one value per column, got 1 for 2 in values[1]"),
        ([[1, "a"], [3, "c", 4]], "An upsert row must have one value per column, got 3 for 2 in values[1]"),
    ],
    ids=["too few", "too many", "too few in a later row", "too many in a later row"],
)
def test_an_upsert_row_has_one_value_per_column(dialect, rows, message):
    upsert = UPSERT(TABLE, [COLUMN("id"), COLUMN("name")], [VALUES_ROW(values) for values in rows], [COLUMN("id")])

    with pytest.raises(ValueError) as error:
        dialect.build_upsert_sql(upsert)

    assert str(error.value) == message


def test_an_upsert_has_at_least_one_row(dialect):
    with pytest.raises(ValueError) as error:
        dialect.build_upsert_sql(UPSERT(TABLE, [COLUMN("id")], [], [COLUMN("id")]))

    assert str(error.value) == "An upsert must have at least one row"


def test_source_column_is_the_incoming_row(dialect):
    assert dialect.build_expression_sql(SOURCE_COLUMN("name")) == '"src"."name"'


# ---------------------------------------------------------------------------
# CREATE INDEX / ANALYZE / CREATE SCHEMA / CREATE TABLE update_heavy
# ---------------------------------------------------------------------------


def test_no_index_is_created(dialect):
    # A standard Snowflake table has no secondary indexes.
    create_index = CREATE_INDEX_IF_NOT_EXISTS("t_name_idx", TABLE, ["name", COLUMN("id")])

    assert dialect.build_create_index_sql(create_index) is None


def test_no_statistics_are_refreshed(dialect):
    assert dialect.build_analyze_table_sql(ANALYZE_TABLE(TABLE)) is None


def test_create_schema_is_the_existing_statement(dialect):
    sql = dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(prefixes=["db", "s"]))

    assert sql == 'CREATE SCHEMA IF NOT EXISTS "s";'
    assert sql == dialect.create_schema_if_not_exists_sql(["db", "s"])


def test_create_schema_without_semicolon(dialect):
    sql = dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(prefixes=["db", "s"]), add_semicolon=False)

    assert sql == 'CREATE SCHEMA IF NOT EXISTS "s"'


@pytest.mark.parametrize(
    "node, expected_sql",
    [
        (
            CREATE_TABLE,
            'CREATE TABLE  "s"."t" (\n\t"id" integer NOT NULL,\n\t"name" varchar(255),\n\tPRIMARY KEY ("id")\n);',
        ),
        (
            CREATE_TABLE_IF_NOT_EXISTS,
            'CREATE TABLE IF NOT EXISTS "s"."t" (\n'
            '\t"id" integer NOT NULL,\n'
            '\t"name" varchar(255),\n'
            '\tPRIMARY KEY ("id")\n'
            ");",
        ),
    ],
    ids=["CREATE_TABLE", "CREATE_TABLE_IF_NOT_EXISTS"],
)
def test_create_table_ignores_update_heavy(dialect, node, expected_sql):
    def create_table(update_heavy: bool):
        return node(
            fully_qualified_table_name=TABLE,
            columns=[
                CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name="integer")),
                CREATE_TABLE_COLUMN(name="name", type=SqlDataType(name="varchar", character_maximum_length=255)),
            ],
            primary_key_column_names=["id"],
            update_heavy=update_heavy,
        )

    assert dialect.build_create_table_sql(create_table(True)) == expected_sql
    assert dialect.build_create_table_sql(create_table(False)) == expected_sql


def test_a_column_default_can_be_the_current_timestamp(dialect):
    sql = dialect.build_create_table_sql(
        CREATE_TABLE_IF_NOT_EXISTS(
            fully_qualified_table_name=TABLE,
            columns=[
                CREATE_TABLE_COLUMN(
                    name="created_at", type=SqlDataType(name="timestamp_tz"), default=CURRENT_TIMESTAMP()
                )
            ],
        )
    )

    assert sql == 'CREATE TABLE IF NOT EXISTS "s"."t" (\n\t"created_at" timestamp_tz DEFAULT CURRENT_TIMESTAMP\n);'


# ---------------------------------------------------------------------------
# LIKE ... ESCAPE
# ---------------------------------------------------------------------------


def test_like_with_an_escape_character(dialect):
    sql = dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("x!_y!%%"), escape="!"))

    assert sql == "\"name\" like 'x!_y!%%' ESCAPE '!'"


def test_like_with_a_backslash_escape_character(dialect):
    # escape_string doubles every backslash in a string literal, so the SQL reads '\\', one backslash
    # to Snowflake, in the pattern and the ESCAPE clause alike.
    sql = dialect.build_expression_sql(LIKE(LOWER(COLUMN("name")), LITERAL("%50\\%%"), escape="\\"))

    assert sql == "LOWER(\"name\") like '%50\\\\%%' ESCAPE '\\\\'"


def test_like_without_an_escape_is_unchanged(dialect):
    assert dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("%a%"))) == "\"name\" like '%a%'"


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------

_EXPRESSIONS = [
    ("CURRENT_TIMESTAMP", CURRENT_TIMESTAMP(), "CURRENT_TIMESTAMP"),
    ("NULLIF", NULLIF(COLUMN("n"), LITERAL(0)), 'NULLIF("n", 0)'),
    ("ABS", ABS(COLUMN("n")), 'ABS("n")'),
    ("ARITHMETIC", ARITHMETIC("+", COLUMN("fix_count", "cur"), LITERAL(1)), '("cur"."fix_count" + 1)'),
    (
        "ARITHMETIC nested",
        ARITHMETIC("/", ARITHMETIC("-", COLUMN("a"), COLUMN("b")), ARITHMETIC("*", LITERAL(2), COLUMN("c"))),
        '(("a" - "b") / (2 * "c"))',
    ),
    (
        "TIMESTAMP_ISO_TEXT",
        TIMESTAMP_ISO_TEXT(COLUMN("ts")),
        'TO_CHAR("ts", \'YYYY-MM-DD"T"HH24:MI:SS.FF6\')',
    ),
    (
        "TIMESTAMP_ISO_TEXT whole seconds",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False),
        'TO_CHAR("ts", \'YYYY-MM-DD"T"HH24:MI:SS\')',
    ),
    (
        "TIMESTAMP_ISO_TEXT UTC suffix",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), utc_suffix=True),
        'TO_CHAR("ts", \'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"\')',
    ),
    (
        "TIMESTAMP_ISO_TEXT whole seconds UTC suffix",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False, utc_suffix=True),
        'TO_CHAR("ts", \'YYYY-MM-DD"T"HH24:MI:SS"+00:00"\')',
    ),
    (
        "TIMESTAMP_ISO_TEXT of a UTC wall clock",
        TIMESTAMP_ISO_TEXT(TO_TIMEZONE(COLUMN("ts"), "UTC"), utc_suffix=True),
        'TO_CHAR(CAST(CONVERT_TIMEZONE(\'UTC\', "ts") AS TIMESTAMP_NTZ), \'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"\')',
    ),
    ("DATE_ISO_TEXT", DATE_ISO_TEXT(COLUMN("d")), "TO_CHAR(\"d\", 'YYYY-MM-DD')"),
    ("TO_TIMEZONE UTC", TO_TIMEZONE(COLUMN("ts"), "UTC"), "CAST(CONVERT_TIMEZONE('UTC', \"ts\") AS TIMESTAMP_NTZ)"),
    (
        "TO_TIMEZONE IANA zone",
        TO_TIMEZONE(COLUMN("ts"), "Europe/Brussels"),
        "CAST(CONVERT_TIMEZONE('Europe/Brussels', \"ts\") AS TIMESTAMP_NTZ)",
    ),
    *[
        (f"DATE_TRUNC {unit}", DATE_TRUNC(unit, COLUMN("ts")), f"DATE_TRUNC('{unit}', \"ts\")")
        for unit in DATE_TRUNC_UNITS
    ],
    ("REGEXP_REPLACE", REGEXP_REPLACE(COLUMN("s"), "[0-9]+", "#"), "REGEXP_REPLACE(\"s\", '[0-9]+', '#')"),
    ("REGEXP_REPLACE quotes", REGEXP_REPLACE(COLUMN("s"), "'", "''"), "REGEXP_REPLACE(\"s\", '''', '''''')"),
]


@pytest.mark.parametrize(
    "expression, expected_sql", [case[1:] for case in _EXPRESSIONS], ids=[case[0] for case in _EXPRESSIONS]
)
def test_expression(dialect, expression, expected_sql):
    assert dialect.build_expression_sql(expression) == expected_sql


def test_regexp_replace_doubles_the_backslash_of_a_backreference(dialect):
    # Snowflake reads a backslash escape in a string literal, so the SQL text '\\1.0' is the
    # replacement \1.0.
    sql = dialect.build_expression_sql(REGEXP_REPLACE(COLUMN("s"), "^(-?[0-9]+)$", "\\1.0"))

    assert sql == "REGEXP_REPLACE(\"s\", '^(-?[0-9]+)$', '\\\\1.0')"


@pytest.mark.parametrize(
    "node_name, expression",
    [
        ("EPOCH_SECONDS", EPOCH_SECONDS(ARITHMETIC("-", COLUMN("b"), COLUMN("a")))),
        ("JSON_MERGE", JSON_MERGE(COLUMN("a"), COLUMN("b"))),
    ],
    ids=["EPOCH_SECONDS", "JSON_MERGE"],
)
def test_expressions_without_a_snowflake_rendering_are_refused(dialect, node_name, expression):
    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_expression_sql(expression)

    assert str(error.value) == f"SnowflakeSqlDialect does not support {node_name}"
