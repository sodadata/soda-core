"""The data-plane statement nodes and expressions as AthenaSqlDialect renders them.

Every node is pinned to its exact Athena SQL. A transactional table is created as an Iceberg table, the
only kind Athena updates, deletes from and merges into row by row; any other CREATE TABLE is the Hive one,
unchanged. UPDATE and DELETE are the base forms without a target alias, and an UPDATE reading another
table is a MERGE from it. The upserts are a MERGE whose source is a subquery: the rows of an UPSERT as
VALUES, the SELECT of an UPSERT_VIA_SELECT with any WITH kept inside it. Athena has no index to create
and no statistics to refresh, so those render None, and it refuses EPOCH_SECONDS,
JSON_MERGE and a REGEXP_REPLACE replacement with a backslash other than one escaping a dollar.
"""

from __future__ import annotations

from datetime import datetime

import pytest
from soda_athena.common.data_sources.athena_data_source import AthenaSqlDialect
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
    LEFT_INNER_JOIN,
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

TABLE = '"c"."s"."t"'
LOCATION = "s3://staging/c/s/t/"


@pytest.fixture
def dialect() -> AthenaSqlDialect:
    return AthenaSqlDialect(get_table_storage_location={TABLE: LOCATION}.__getitem__)


def test_the_dialect_has_the_flag():
    assert AthenaSqlDialect.SUPPORTS_DATA_PLANE_STATEMENTS is True


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------


def test_update_without_alias_or_from(dialect):
    sql = dialect.build_update_sql(
        UPDATE(
            TABLE,
            [ASSIGNMENT("name", LITERAL("x")), ASSIGNMENT(COLUMN("n"), LITERAL(1))],
            where=WHERE(EQ(COLUMN("id"), LITERAL(1))),
        )
    )

    assert sql == 'UPDATE "c"."s"."t"\nSET "name" = \'x\', "n" = 1\nWHERE "id" = 1;'


def test_update_without_semicolon(dialect):
    sql = dialect.build_update_sql(UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(None))]), add_semicolon=False)

    assert sql == 'UPDATE "c"."s"."t"\nSET "n" = NULL'


def test_update_from_a_table_is_a_merge_on_the_where(dialect):
    sql = dialect.build_update_sql(
        UPDATE(
            fully_qualified_table_name=TABLE,
            alias="tgt",
            assignments=[
                ASSIGNMENT("name", COLUMN("name", table_alias="src")),
                ASSIGNMENT(COLUMN("n"), ARITHMETIC("+", COLUMN("n", "tgt"), COLUMN("n", "src"))),
            ],
            from_elements=[FROM("src_table", table_prefix=["c", "s"], alias="src")],
            where=WHERE(AND([EQ(COLUMN("id", "tgt"), COLUMN("id", "src")), IS_NOT_NULL(COLUMN("name", "src"))])),
        )
    )

    assert sql == (
        'MERGE INTO "c"."s"."t" AS "tgt"\n'
        'USING "c"."s"."src_table" AS "src"\n'
        'ON ("tgt"."id" = "src"."id" AND "src"."name" IS NOT NULL)\n'
        'WHEN MATCHED THEN UPDATE SET "name" = "src"."name", "n" = ("tgt"."n" + "src"."n");'
    )


def test_update_from_a_table_without_alias_or_semicolon(dialect):
    sql = dialect.build_update_sql(
        UPDATE(
            fully_qualified_table_name=TABLE,
            assignments=[ASSIGNMENT("n", COLUMN("n", "u"))],
            from_elements=[FROM("u", table_prefix=["c", "s"])],
            where=WHERE(EQ(COLUMN("id", "t"), COLUMN("id", "u"))),
        ),
        add_semicolon=False,
    )

    assert sql == (
        'MERGE INTO "c"."s"."t"\n'
        'USING "c"."s"."u"\n'
        'ON ("t"."id" = "u"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "n" = "u"."n"'
    )


def test_an_update_without_from_cannot_be_aliased(dialect):
    update = UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(1))], alias="tgt", where=WHERE(EQ(COLUMN("id", "tgt"), LITERAL(1))))

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_update_sql(update)

    assert str(error.value) == (
        "AthenaSqlDialect does not support UPDATE with an alias and no from_elements: "
        "Athena's UPDATE takes no target alias"
    )


@pytest.mark.parametrize(
    "from_elements",
    [
        [FROM("a", table_prefix=["c", "s"], alias="a"), FROM("b", table_prefix=["c", "s"], alias="b")],
        [JOIN("o", table_prefix=["c", "s"], alias="o", on_condition=EQ(COLUMN("id", "o"), COLUMN("id", "tgt")))],
        [
            LEFT_INNER_JOIN(
                "o", table_prefix=["c", "s"], alias="o", on_condition=EQ(COLUMN("id", "o"), COLUMN("id", "tgt"))
            )
        ],
    ],
    ids=["two tables", "a join", "a left join"],
)
def test_an_update_reads_one_table_only(dialect, from_elements):
    update = UPDATE(
        TABLE,
        [ASSIGNMENT("n", LITERAL(1))],
        alias="tgt",
        from_elements=from_elements,
        where=WHERE(EQ(COLUMN("id", "tgt"), LITERAL(1))),
    )

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_update_sql(update)

    assert str(error.value) == (
        "AthenaSqlDialect does not support UPDATE with from_elements other than one table: "
        "it renders as a MERGE, which reads one"
    )


def test_an_update_from_a_table_needs_a_where(dialect):
    update = UPDATE(TABLE, [ASSIGNMENT("n", COLUMN("n", "u"))], alias="tgt", from_elements=[FROM("u", alias="u")])

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_update_sql(update)

    assert str(error.value) == (
        "AthenaSqlDialect does not support UPDATE with from_elements and no where: "
        "the where is the MERGE's join condition"
    )


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------


def test_delete_without_alias_or_using(dialect):
    sql = dialect.build_delete_sql(DELETE(TABLE, where=WHERE(IN(COLUMN("id"), [LITERAL(1), LITERAL(2)]))))

    assert sql == 'DELETE FROM "c"."s"."t"\nWHERE "id" IN (1, 2);'


def test_delete_everything(dialect):
    assert dialect.build_delete_sql(DELETE(TABLE)) == 'DELETE FROM "c"."s"."t";'


def test_delete_without_semicolon(dialect):
    sql = dialect.build_delete_sql(DELETE(TABLE, where=WHERE(EQ(COLUMN("id"), LITERAL(1)))), add_semicolon=False)

    assert sql == 'DELETE FROM "c"."s"."t"\nWHERE "id" = 1'


def test_a_delete_cannot_be_aliased(dialect):
    delete = DELETE(TABLE, alias="tgt", where=WHERE(EQ(COLUMN("id", "tgt"), LITERAL(1))))

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_delete_sql(delete)

    assert str(error.value) == (
        "AthenaSqlDialect does not support DELETE with an alias: Athena's DELETE takes no target alias"
    )


@pytest.mark.parametrize("alias", [None, "tgt"], ids=["without alias", "with alias"])
def test_a_delete_reads_no_other_table(dialect, alias):
    delete = DELETE(
        TABLE,
        alias=alias,
        using_elements=[FROM("u", table_prefix=["c", "s"], alias="u")],
        where=WHERE(GT(COLUMN("n", "u"), LITERAL(5))),
    )

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_delete_sql(delete)

    assert str(error.value) == (
        "AthenaSqlDialect does not support DELETE with using_elements: Athena's DELETE has no USING clause"
    )


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
        'MERGE INTO "c"."s"."t" AS "tgt"\n'
        "USING (\n"
        "VALUES\n"
        "(1, 'a', 10),\n"
        "(2, 'it''s', NULL),\n"
        "(3, 'c', 30)\n"
        ') AS "src" ("id", "name", "n")\n'
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
        'MERGE INTO "c"."s"."t" AS "tgt"\n'
        "USING (\n"
        "VALUES\n"
        "(1, 'a')\n"
        ') AS "src" ("id", "name")\n'
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
        'MERGE INTO "c"."s"."t" AS "cur"\n'
        "USING (\n"
        "VALUES\n"
        "(1, 0, CAST('{\"a\": 1}' AS varchar), CURRENT_TIMESTAMP)\n"
        ') AS "src" ("id", "fix_count", "attrs", "updated_at")\n'
        'ON ("cur"."id" = "src"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "fix_count" = ("cur"."fix_count" + 1), "updated_at" = CURRENT_TIMESTAMP\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "fix_count", "attrs", "updated_at") '
        'VALUES ("src"."id", "src"."fix_count", "src"."attrs", "src"."updated_at");'
    )


def test_upsert_renders_each_value_as_an_insert_does(dialect):
    # Bare values and expressions alike: the rows go through the same literal rendering as INSERT INTO.
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

    insert_sql = dialect.build_insert_into_sql(INSERT_INTO(TABLE, [row, later_row], columns))
    upsert_sql = dialect.build_upsert_sql(UPSERT(TABLE, columns, [row, later_row], [columns[0]]))

    rows_sql = (
        "(1, 'a', NULL, CURRENT_TIMESTAMP, (2 * 3), TRUE, From_iso8601_timestamp('2024-01-02T03:04:05'), "
        "'C:\\dir'),\n"
        "(2, 'b', 1.5, NULL, 0, FALSE, NULL, NULL)"
    )
    assert insert_sql == (
        'INSERT INTO "c"."s"."t" ("id", "name", "n", "at", "total", "flag", "ts", "path") VALUES\n' + rows_sql + ";"
    )
    assert upsert_sql == (
        'MERGE INTO "c"."s"."t" AS "tgt"\n'
        "USING (\n"
        "VALUES\n" + rows_sql + "\n"
        ') AS "src" ("id", "name", "n", "at", "total", "flag", "ts", "path")\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "name", "n", "at", "total", "flag", "ts", "path") '
        'VALUES ("src"."id", "src"."name", "src"."n", "src"."at", "src"."total", "src"."flag", "src"."ts", '
        '"src"."path");'
    )


def test_upsert_via_select(dialect):
    sql = dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[SELECT([COLUMN("id"), COLUMN("n")]), FROM("src", table_prefix=["c", "s"])],
            columns=[COLUMN("id"), COLUMN("n")],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("n", SOURCE_COLUMN("n"))],
        )
    )

    assert sql == (
        'MERGE INTO "c"."s"."t" AS "tgt"\n'
        "USING (\n"
        'SELECT "id",\n'
        '       "n"\n'
        'FROM "c"."s"."src"\n'
        ') AS "src" ("id", "n")\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "n" = "src"."n"\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "n") VALUES ("src"."id", "src"."n");'
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
        'MERGE INTO "c"."s"."t" AS "cur"\n'
        "USING (\n"
        "SELECT 2,\n"
        "       5\n"
        ') AS "src" ("id", "n")\n'
        'ON ("cur"."id" = "src"."id")\n'
        'WHEN MATCHED THEN UPDATE SET "n" = ("cur"."n" + "src"."n")\n'
        'WHEN NOT MATCHED THEN INSERT ("id", "n") VALUES ("src"."id", "src"."n");'
    )


def test_upsert_via_select_keeps_a_with_inside_the_merge_source(dialect):
    sql = dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[
                WITH([CTE("k").AS([SELECT([COLUMN("id")]), FROM("keys", table_prefix=["c", "s"])])]),
                SELECT([COLUMN("id")]),
                FROM("k"),
            ],
            columns=[COLUMN("id")],
            key_columns=[COLUMN("id")],
        )
    )

    assert sql == (
        'MERGE INTO "c"."s"."t" AS "tgt"\n'
        "USING (\n"
        "WITH \n"
        '"k" AS (\n'
        'SELECT "id"\n'
        'FROM "c"."s"."keys"\n'
        ")\n"
        'SELECT "id"\n'
        'FROM "k"\n'
        ') AS "src" ("id")\n'
        'ON ("tgt"."id" = "src"."id")\n'
        'WHEN NOT MATCHED THEN INSERT ("id") VALUES ("src"."id");'
    )


_MINIMAL_UPSERT_SQL = (
    'MERGE INTO "c"."s"."t" AS "tgt"\n'
    "USING (\n"
    "VALUES\n"
    "(1)\n"
    ') AS "src" ("id")\n'
    'ON ("tgt"."id" = "src"."id")\n'
    'WHEN NOT MATCHED THEN INSERT ("id") VALUES ("src"."id")'
)
_MINIMAL_UPSERT_VIA_SELECT_SQL = (
    'MERGE INTO "c"."s"."t" AS "tgt"\n'
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


@pytest.mark.parametrize("alias", ["src", "SRC"])
@pytest.mark.parametrize(
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
def test_an_upsert_target_cannot_be_aliased_as_the_incoming_rows(dialect, render, alias):
    with pytest.raises(UnsupportedSqlStatementError) as error:
        render(dialect, alias)

    assert str(error.value) == (
        f"AthenaSqlDialect does not support an upsert target alias {alias!r}: it names the incoming rows"
    )


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
    upsert = UPSERT(TABLE, [COLUMN("id")], [], [COLUMN("id")])

    with pytest.raises(ValueError) as error:
        dialect.build_upsert_sql(upsert)

    assert str(error.value) == "An upsert must have at least one row"


def test_source_column_is_the_incoming_row(dialect):
    assert dialect.build_expression_sql(SOURCE_COLUMN("name")) == '"src"."name"'


# ---------------------------------------------------------------------------
# CREATE INDEX / ANALYZE / CREATE SCHEMA
# ---------------------------------------------------------------------------


def test_no_index_is_created(dialect):
    create_index = CREATE_INDEX_IF_NOT_EXISTS("t_name_idx", TABLE, ["name", COLUMN("id")])

    assert dialect.build_create_index_sql(create_index) is None


def test_no_statistics_are_refreshed(dialect):
    assert dialect.build_analyze_table_sql(ANALYZE_TABLE(TABLE)) is None


def test_create_schema_is_the_existing_statement(dialect):
    sql = dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(prefixes=["c", "s"]))

    assert sql == "CREATE SCHEMA IF NOT EXISTS `s`;"
    assert sql == dialect.create_schema_if_not_exists_sql(["c", "s"])


def test_create_schema_without_semicolon(dialect):
    sql = dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(prefixes=["c", "s"]), add_semicolon=False)

    assert sql == "CREATE SCHEMA IF NOT EXISTS `s`"


# ---------------------------------------------------------------------------
# CREATE TABLE: Hive, or Iceberg when transactional
# ---------------------------------------------------------------------------


def _create_table(node: type = CREATE_TABLE_IF_NOT_EXISTS, **kwargs) -> CREATE_TABLE:
    return node(
        fully_qualified_table_name=TABLE,
        columns=[
            CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name="integer")),
            CREATE_TABLE_COLUMN(name="name", type=SqlDataType(name="varchar", character_maximum_length=255)),
            CREATE_TABLE_COLUMN(name="ts", type=SqlDataType(name="timestamp")),
        ],
        primary_key_column_names=["id"],
        **kwargs,
    )


_HIVE_COLUMNS_SQL = "(\n\t`id` integer,\n\t`name` varchar(255),\n\t`ts` timestamp\n)\n"
_ICEBERG_COLUMNS_SQL = "(\n\t`id` int,\n\t`name` string,\n\t`ts` timestamp\n)\n"
_ICEBERG_PROPERTIES_SQL = "TBLPROPERTIES ('table_type'='ICEBERG', 'format'='parquet')"

_CREATE_TABLE_RENDERS = [
    (
        "Hive",
        CREATE_TABLE,
        {},
        "CREATE EXTERNAL TABLE  `c`.`s`.`t` " + _HIVE_COLUMNS_SQL + f"STORED AS PARQUET\nLOCATION '{LOCATION}'",
    ),
    (
        "Hive IF NOT EXISTS",
        CREATE_TABLE_IF_NOT_EXISTS,
        {},
        "CREATE EXTERNAL TABLE IF NOT EXISTS `c`.`s`.`t` "
        + _HIVE_COLUMNS_SQL
        + f"STORED AS PARQUET\nLOCATION '{LOCATION}'",
    ),
    (
        "Hive not transactional",
        CREATE_TABLE_IF_NOT_EXISTS,
        {"transactional": False, "update_heavy": True},
        "CREATE EXTERNAL TABLE IF NOT EXISTS `c`.`s`.`t` "
        + _HIVE_COLUMNS_SQL
        + f"STORED AS PARQUET\nLOCATION '{LOCATION}'",
    ),
    (
        "Iceberg",
        CREATE_TABLE,
        {"transactional": True},
        "CREATE TABLE `c`.`s`.`t` " + _ICEBERG_COLUMNS_SQL + f"LOCATION '{LOCATION}'\n" + _ICEBERG_PROPERTIES_SQL,
    ),
    (
        "Iceberg IF NOT EXISTS",
        CREATE_TABLE_IF_NOT_EXISTS,
        {"transactional": True},
        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` "
        + _ICEBERG_COLUMNS_SQL
        + f"LOCATION '{LOCATION}'\n"
        + _ICEBERG_PROPERTIES_SQL,
    ),
    (
        "Iceberg update_heavy",
        CREATE_TABLE_IF_NOT_EXISTS,
        {"transactional": True, "update_heavy": True},
        "CREATE TABLE IF NOT EXISTS `c`.`s`.`t` "
        + _ICEBERG_COLUMNS_SQL
        + f"LOCATION '{LOCATION}'\n"
        + _ICEBERG_PROPERTIES_SQL,
    ),
]


@pytest.mark.parametrize(
    "node, kwargs, expected_sql",
    [case[1:] for case in _CREATE_TABLE_RENDERS],
    ids=[case[0] for case in _CREATE_TABLE_RENDERS],
)
@pytest.mark.parametrize("add_semicolon, semicolon", [((), ";"), ((True,), ";"), ((False,), "")])
def test_create_table(dialect, node, kwargs, expected_sql, add_semicolon, semicolon):
    # No primary key and no NOT NULL either way: neither DDL has them.
    assert dialect.build_create_table_sql(_create_table(node, **kwargs), *add_semicolon) == expected_sql + semicolon


@pytest.mark.parametrize(
    "sql_data_type, iceberg_type",
    [
        (SqlDataType(name="char", character_maximum_length=3), "string"),
        (SqlDataType(name="char"), "string"),
        (SqlDataType(name="varchar", character_maximum_length=255), "string"),
        (SqlDataType(name="varchar"), "string"),
        (SqlDataType(name="varchar(10)"), "string"),
        (SqlDataType(name="tinyint"), "int"),
        (SqlDataType(name="smallint"), "int"),
        (SqlDataType(name="integer"), "int"),
        (SqlDataType(name="int"), "int"),
        (SqlDataType(name="bigint"), "bigint"),
        (SqlDataType(name="decimal", numeric_precision=10, numeric_scale=2), "decimal(10,2)"),
        (SqlDataType(name="double"), "double"),
        (SqlDataType(name="boolean"), "boolean"),
        (SqlDataType(name="date"), "date"),
        (SqlDataType(name="time"), "string"),
        (SqlDataType(name="timestamp", datetime_precision=6), "timestamp"),
        (SqlDataType(name="timestamp(3)"), "timestamp"),
        (SqlDataType(name="timestamp with time zone"), "timestamp"),
        (SqlDataType(name="timestamp(6) with time zone"), "timestamp"),
        (SqlDataType(name="binary"), "binary"),
    ],
    ids=lambda value: str(value) if isinstance(value, SqlDataType) else None,
)
def test_an_iceberg_column_type(dialect, sql_data_type, iceberg_type):
    sql = dialect.build_create_table_sql(
        CREATE_TABLE(TABLE, [CREATE_TABLE_COLUMN(name="x", type=sql_data_type)], transactional=True)
    )

    assert (
        sql == f"CREATE TABLE `c`.`s`.`t` (\n\t`x` {iceberg_type}\n)\nLOCATION '{LOCATION}'\n{_ICEBERG_PROPERTIES_SQL};"
    )


def test_an_iceberg_column_drops_not_null(dialect):
    sql = dialect.build_create_table_sql(
        CREATE_TABLE(
            TABLE,
            [CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name="integer"), nullable=False)],
            transactional=True,
        )
    )

    assert sql == f"CREATE TABLE `c`.`s`.`t` (\n\t`id` int\n)\nLOCATION '{LOCATION}'\n{_ICEBERG_PROPERTIES_SQL};"


def test_an_iceberg_location_is_a_string_literal():
    dialect = AthenaSqlDialect(get_table_storage_location={TABLE: "s3://staging/it's/"}.__getitem__)

    sql = dialect.build_create_table_sql(
        CREATE_TABLE(TABLE, [CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name="integer"))], transactional=True)
    )

    assert (
        sql == f"CREATE TABLE `c`.`s`.`t` (\n\t`id` int\n)\nLOCATION 's3://staging/it''s/'\n{_ICEBERG_PROPERTIES_SQL};"
    )


@pytest.mark.parametrize("default", [CURRENT_TIMESTAMP(), LITERAL(0)], ids=["CURRENT_TIMESTAMP", "a literal"])
def test_an_iceberg_column_has_no_default(dialect, default):
    create_table = CREATE_TABLE_IF_NOT_EXISTS(
        TABLE,
        [
            CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name="integer")),
            CREATE_TABLE_COLUMN(name="created_at", type=SqlDataType(name="timestamp"), default=default),
        ],
        transactional=True,
    )

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_create_table_sql(create_table)

    assert str(error.value) == (
        "AthenaSqlDialect does not support a column DEFAULT on a transactional table: "
        "Athena's Iceberg DDL has none, got one on 'created_at'"
    )


def test_a_table_needs_a_storage_location_either_way():
    dialect = AthenaSqlDialect()

    for transactional in (False, True):
        create_table = _create_table(transactional=transactional)
        with pytest.raises(ValueError, match="get_table_storage_location function must be provided"):
            dialect.build_create_table_sql(create_table)


# ---------------------------------------------------------------------------
# LIKE ... ESCAPE
# ---------------------------------------------------------------------------


def test_like_with_an_escape_character(dialect):
    sql = dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("x!_y!%%"), escape="!"))

    assert sql == "\"name\" like 'x!_y!%%' ESCAPE '!'"


def test_like_with_a_backslash_escape_character(dialect):
    # A string literal takes a backslash as it is, in the pattern and the ESCAPE clause alike.
    sql = dialect.build_expression_sql(LIKE(LOWER(COLUMN("name")), LITERAL("%50\\%%"), escape="\\"))

    assert sql == "LOWER(\"name\") like '%50\\%%' ESCAPE '\\'"


def test_like_without_an_escape_is_unchanged(dialect):
    assert dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("%a%"))) == "\"name\" like '%a%'"


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------

_ISO = "replace(CAST(CAST(\"ts\" AS timestamp(6)) AS varchar), ' ', 'T')"
_ISO_WHOLE_SECONDS = "date_format(\"ts\", '%Y-%m-%dT%H:%i:%s')"
_UTC = "CAST(format_datetime(at_timezone(\"ts\", 'UTC'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS timestamp)"

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
    ("TIMESTAMP_ISO_TEXT", TIMESTAMP_ISO_TEXT(COLUMN("ts")), _ISO),
    (
        "TIMESTAMP_ISO_TEXT whole seconds",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False),
        _ISO_WHOLE_SECONDS,
    ),
    ("TIMESTAMP_ISO_TEXT UTC suffix", TIMESTAMP_ISO_TEXT(COLUMN("ts"), utc_suffix=True), f"({_ISO} || '+00:00')"),
    (
        "TIMESTAMP_ISO_TEXT whole seconds UTC suffix",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False, utc_suffix=True),
        f"({_ISO_WHOLE_SECONDS} || '+00:00')",
    ),
    (
        "TIMESTAMP_ISO_TEXT of a UTC wall clock",
        TIMESTAMP_ISO_TEXT(TO_TIMEZONE(COLUMN("ts"), "UTC"), utc_suffix=True),
        f"(replace(CAST(CAST({_UTC} AS timestamp(6)) AS varchar), ' ', 'T') || '+00:00')",
    ),
    ("DATE_ISO_TEXT", DATE_ISO_TEXT(COLUMN("d")), "date_format(\"d\", '%Y-%m-%d')"),
    ("TO_TIMEZONE UTC", TO_TIMEZONE(COLUMN("ts"), "UTC"), _UTC),
    (
        "TO_TIMEZONE IANA zone",
        TO_TIMEZONE(COLUMN("ts"), "Europe/Brussels"),
        "CAST(format_datetime(at_timezone(\"ts\", 'Europe/Brussels'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS timestamp)",
    ),
    (
        "TO_TIMEZONE quotes the zone",
        TO_TIMEZONE(COLUMN("ts"), "it's"),
        "CAST(format_datetime(at_timezone(\"ts\", 'it''s'), 'yyyy-MM-dd HH:mm:ss.SSSSSS') AS timestamp)",
    ),
    *[
        (f"DATE_TRUNC {unit}", DATE_TRUNC(unit, COLUMN("ts")), f"date_trunc('{unit}', \"ts\")")
        for unit in DATE_TRUNC_UNITS
    ],
    ("REGEXP_REPLACE", REGEXP_REPLACE(COLUMN("s"), "[0-9]+", "#"), "regexp_replace(\"s\", '[0-9]+', '#')"),
    (
        "REGEXP_REPLACE backreference",
        REGEXP_REPLACE(COLUMN("s"), "^(-?[0-9]+)$", "$1.0"),
        "regexp_replace(\"s\", '^(-?[0-9]+)$', '$1.0')",
    ),
    (
        "REGEXP_REPLACE backslash in the pattern",
        REGEXP_REPLACE(COLUMN("s"), "\\d+", "#"),
        "regexp_replace(\"s\", '\\d+', '#')",
    ),
    (
        "REGEXP_REPLACE escaped dollar",
        REGEXP_REPLACE(COLUMN("s"), "^([0-9]+)$", "\\$$1"),
        "regexp_replace(\"s\", '^([0-9]+)$', '\\$$1')",
    ),
    ("REGEXP_REPLACE quotes", REGEXP_REPLACE(COLUMN("s"), "'", "''"), "regexp_replace(\"s\", '''', '''''')"),
]


@pytest.mark.parametrize(
    "expression, expected_sql", [case[1:] for case in _EXPRESSIONS], ids=[case[0] for case in _EXPRESSIONS]
)
def test_expression(dialect, expression, expected_sql):
    assert dialect.build_expression_sql(expression) == expected_sql


@pytest.mark.parametrize(
    "replacement",
    ["\\1.0", "a\\b", "a\\"],
    ids=["backreference", "backslash", "trailing backslash"],
)
def test_regexp_replace_refuses_a_backslash_in_the_replacement(dialect, replacement):
    # Athena drops a \1 backreference without an error, so the replacement would silently be wrong.
    expression = REGEXP_REPLACE(COLUMN("s"), "^(-?[0-9]+)$", replacement)

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_expression_sql(expression)

    assert str(error.value) == (
        "AthenaSqlDialect does not support a REGEXP_REPLACE replacement with a backslash not followed by $: "
        "a backreference is written $1 and a literal dollar \\$, Athena drops any other backslash silently"
    )


@pytest.mark.parametrize(
    "node_name, expression",
    [
        ("EPOCH_SECONDS", EPOCH_SECONDS(ARITHMETIC("-", COLUMN("b"), COLUMN("a")))),
        ("JSON_MERGE", JSON_MERGE(COLUMN("a"), COLUMN("b"))),
    ],
    ids=["EPOCH_SECONDS", "JSON_MERGE"],
)
def test_expressions_without_an_athena_rendering_are_refused(dialect, node_name, expression):
    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_expression_sql(expression)

    assert str(error.value) == f"AthenaSqlDialect does not support {node_name}"
