"""The data-plane statement nodes and expressions as SqlServerSqlDialect renders them.

Every node is pinned to its exact T-SQL. The forms differ from Postgres where T-SQL requires it: UPDATE
and DELETE name an aliased target in FROM, the upserts are a MERGE that always ends with its mandatory
semicolon, and the functions that need a newer engine than the connected one raise instead of
rendering.
"""

from __future__ import annotations

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
    IS_NOT_NULL,
    JOIN,
    JSON_MERGE,
    LIKE,
    LIMIT,
    LITERAL,
    LOWER,
    NULLIF,
    ORDER_BY_ASC,
    PLACEHOLDER,
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
from soda_sqlserver.common.data_sources.sqlserver_data_source import SqlServerSqlDialect

TABLE = "[s].[t]"


@pytest.fixture
def dialect() -> SqlServerSqlDialect:
    return SqlServerSqlDialect()


def test_the_dialect_has_the_flag():
    assert SqlServerSqlDialect.SUPPORTS_DATA_PLANE_STATEMENTS is True


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
        "UPDATE [tgt]\n"
        "SET [name] = [src].[name], [n] = 1\n"
        "FROM [s].[t] AS [tgt],\n"
        "     [s].[src_table] AS [src]\n"
        "     JOIN [s].[other] AS [o] ON [o].[id] = [src].[id]\n"
        "WHERE [tgt].[id] = [src].[id] AND [src].[name] IS NOT NULL;"
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
        "UPDATE [tgt]\n"
        "SET [n] = [a].[n]\n"
        "FROM [s].[t] AS [tgt],\n"
        "     [s].[a] AS [a],\n"
        "     [s].[b] AS [b]\n"
        "WHERE [a].[id] = [b].[id];"
    )


def test_update_with_an_alias_and_no_from(dialect):
    sql = dialect.build_update_sql(
        UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(0))], alias="tgt", where=WHERE(EQ(COLUMN("id", "tgt"), LITERAL(3))))
    )

    assert sql == "UPDATE [tgt]\nSET [n] = 0\nFROM [s].[t] AS [tgt]\nWHERE [tgt].[id] = 3;"


def test_update_without_alias_or_from(dialect):
    sql = dialect.build_update_sql(
        UPDATE(TABLE, [ASSIGNMENT("name", LITERAL("x"))], where=WHERE(EQ(COLUMN("id"), LITERAL(1))))
    )

    assert sql == "UPDATE [s].[t]\nSET [name] = 'x'\nWHERE [id] = 1;"


def test_update_without_semicolon(dialect):
    sql = dialect.build_update_sql(UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(None))]), add_semicolon=False)

    assert sql == "UPDATE [s].[t]\nSET [n] = NULL"


def test_update_from_further_tables_needs_an_alias(dialect):
    # Unaliased, T-SQL would bind the target to a FROM reference of the same table.
    update = UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(1))], from_elements=[FROM("u", table_prefix=["s"], alias="u")])

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_update_sql(update)

    assert str(error.value) == (
        "SqlServerSqlDialect does not support UPDATE with from_elements and no alias: T-SQL binds an unaliased "
        "target to a reference to the same table among the further tables"
    )


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
        "DELETE [tgt]\n"
        "FROM [s].[t] AS [tgt],\n"
        "     [s].[u] AS [u]\n"
        "WHERE [tgt].[id] = [u].[id] AND [u].[n] > 5;"
    )


def test_delete_with_an_alias_and_no_using(dialect):
    sql = dialect.build_delete_sql(DELETE(TABLE, alias="tgt", where=WHERE(EQ(COLUMN("id", "tgt"), LITERAL(3)))))

    assert sql == "DELETE [tgt]\nFROM [s].[t] AS [tgt]\nWHERE [tgt].[id] = 3;"


def test_delete_without_using(dialect):
    sql = dialect.build_delete_sql(DELETE(TABLE, where=WHERE(IN(COLUMN("id"), [LITERAL(1), LITERAL(2)]))))

    assert sql == "DELETE FROM [s].[t]\nWHERE [id] IN (1, 2);"


def test_delete_everything(dialect):
    assert dialect.build_delete_sql(DELETE(TABLE)) == "DELETE FROM [s].[t];"


def test_delete_using_further_tables_needs_an_alias(dialect):
    delete = DELETE(TABLE, using_elements=[FROM("u", table_prefix=["s"], alias="u")])

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_delete_sql(delete)

    assert str(error.value) == (
        "SqlServerSqlDialect does not support DELETE with using_elements and no alias: T-SQL binds an unaliased "
        "target to a reference to the same table among the further tables"
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
            ],
            key_columns=[COLUMN("id")],
            update_assignments=[ASSIGNMENT("name", SOURCE_COLUMN("name")), ASSIGNMENT("n", SOURCE_COLUMN("n"))],
        )
    )

    assert sql == (
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [tgt]\n"
        "USING (\n"
        "VALUES\n"
        "(1, 'a', 10),\n"
        "(2, 'it''s', NULL)\n"
        ") AS [src] ([id], [name], [n])\n"
        "ON ([tgt].[id] = [src].[id])\n"
        "WHEN MATCHED THEN UPDATE SET [name] = [src].[name], [n] = [src].[n]\n"
        "WHEN NOT MATCHED THEN INSERT ([id], [name], [n]) VALUES ([src].[id], [src].[name], [src].[n]);"
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
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [tgt]\n"
        "USING (\n"
        "VALUES\n"
        "(1, 'a')\n"
        ") AS [src] ([id], [name])\n"
        "ON ([tgt].[id] = [src].[id] AND [tgt].[name] = [src].[name])\n"
        "WHEN NOT MATCHED THEN INSERT ([id], [name]) VALUES ([src].[id], [src].[name]);"
    )


def test_upsert_with_an_alias_reads_the_existing_row(dialect):
    sql = dialect.build_upsert_sql(
        UPSERT(
            fully_qualified_table_name=TABLE,
            columns=[COLUMN("id"), COLUMN("fix_count"), COLUMN("attrs"), COLUMN("updated_at")],
            values=[
                VALUES_ROW([LITERAL(1), LITERAL(0), CAST(LITERAL('{"a": 1}'), "nvarchar(max)"), CURRENT_TIMESTAMP()])
            ],
            key_columns=[COLUMN("id")],
            update_assignments=[
                ASSIGNMENT("fix_count", ARITHMETIC("+", COLUMN("fix_count", "cur"), LITERAL(1))),
                ASSIGNMENT("updated_at", CURRENT_TIMESTAMP()),
            ],
            alias="cur",
        )
    )

    assert sql == (
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [cur]\n"
        "USING (\n"
        "VALUES\n"
        "(1, 0, CAST('{\"a\": 1}' AS nvarchar(max)), SYSDATETIMEOFFSET())\n"
        ") AS [src] ([id], [fix_count], [attrs], [updated_at])\n"
        "ON ([cur].[id] = [src].[id])\n"
        "WHEN MATCHED THEN UPDATE SET [fix_count] = ([cur].[fix_count] + 1), [updated_at] = SYSDATETIMEOFFSET()\n"
        "WHEN NOT MATCHED THEN INSERT ([id], [fix_count], [attrs], [updated_at]) "
        "VALUES ([src].[id], [src].[fix_count], [src].[attrs], [src].[updated_at]);"
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
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [tgt]\n"
        "USING (\n"
        "SELECT [id],\n"
        "       [n]\n"
        "FROM [s].[src]\n"
        ") AS [src] ([id], [n])\n"
        "ON ([tgt].[id] = [src].[id])\n"
        "WHEN MATCHED THEN UPDATE SET [n] = [src].[n]\n"
        "WHEN NOT MATCHED THEN INSERT ([id], [n]) VALUES ([src].[id], [src].[n]);"
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
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [cur]\n"
        "USING (\n"
        "SELECT 2,\n"
        "       5\n"
        ") AS [src] ([id], [n])\n"
        "ON ([cur].[id] = [src].[id])\n"
        "WHEN MATCHED THEN UPDATE SET [n] = ([cur].[n] + [src].[n])\n"
        "WHEN NOT MATCHED THEN INSERT ([id], [n]) VALUES ([src].[id], [src].[n]);"
    )


def test_upsert_via_select_moves_a_with_in_front_of_the_merge(dialect):
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
        "WITH \n"
        "[k] AS (\n"
        "SELECT [id]\n"
        "FROM [s].[keys]\n"
        ")\n"
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [tgt]\n"
        "USING (\n"
        "SELECT [id]\n"
        "FROM [k]\n"
        ") AS [src] ([id])\n"
        "ON ([tgt].[id] = [src].[id])\n"
        "WHEN NOT MATCHED THEN INSERT ([id]) VALUES ([src].[id]);"
    )


def test_upsert_via_select_orders_its_source_only_with_a_limit(dialect):
    # An ORDER BY in a MERGE source needs TOP (error 1033); a LIMIT renders it.
    sql = dialect.build_upsert_via_select_sql(
        UPSERT_VIA_SELECT(
            fully_qualified_table_name=TABLE,
            select_elements=[
                SELECT([COLUMN("id")]),
                FROM("src", table_prefix=["s"]),
                ORDER_BY_ASC(COLUMN("id")),
                LIMIT(10),
            ],
            columns=[COLUMN("id")],
            key_columns=[COLUMN("id")],
        )
    )

    assert sql == (
        "MERGE INTO [s].[t] WITH (HOLDLOCK) AS [tgt]\n"
        "USING (\n"
        "SELECT TOP 10 [id]\n"
        "FROM [s].[src]\n"
        "ORDER BY [id] ASC\n"
        ") AS [src] ([id])\n"
        "ON ([tgt].[id] = [src].[id])\n"
        "WHEN NOT MATCHED THEN INSERT ([id]) VALUES ([src].[id]);"
    )


_UPSERT_RENDERS = [
    (
        "UPSERT",
        lambda d, add_semicolon: d.build_upsert_sql(
            UPSERT(TABLE, [COLUMN("id")], [VALUES_ROW([LITERAL(1)])], [COLUMN("id")]), add_semicolon=add_semicolon
        ),
    ),
    (
        "UPSERT_VIA_SELECT",
        lambda d, add_semicolon: d.build_upsert_via_select_sql(
            UPSERT_VIA_SELECT(TABLE, [SELECT([COLUMN("id")]), FROM("src")], [COLUMN("id")], [COLUMN("id")]),
            add_semicolon=add_semicolon,
        ),
    ),
]


@pytest.mark.parametrize("render", [case[1] for case in _UPSERT_RENDERS], ids=[case[0] for case in _UPSERT_RENDERS])
@pytest.mark.parametrize("add_semicolon", [None, True, False])
def test_the_merge_always_ends_with_one_semicolon(dialect, render, add_semicolon):
    # T-SQL refuses a MERGE without its terminating semicolon (error 10713).
    sql = render(dialect, add_semicolon)

    assert sql.endswith(");") and not sql.endswith(";;")


@pytest.mark.parametrize("alias", ["src", "SRC"])
def test_an_upsert_target_cannot_be_aliased_as_the_incoming_rows(dialect, alias):
    upsert = UPSERT(TABLE, [COLUMN("id")], [VALUES_ROW([LITERAL(1)])], [COLUMN("id")], alias=alias)

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_upsert_sql(upsert)

    assert str(error.value) == (
        f"SqlServerSqlDialect does not support an upsert target alias {alias!r}: it names the incoming rows"
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


def test_source_column_is_the_incoming_row(dialect):
    assert dialect.build_expression_sql(SOURCE_COLUMN("name")) == "[src].[name]"


# ---------------------------------------------------------------------------
# CREATE INDEX / ANALYZE / CREATE SCHEMA / CREATE TABLE update_heavy
# ---------------------------------------------------------------------------


def test_create_index_if_not_exists(dialect):
    sql = dialect.build_create_index_sql(
        CREATE_INDEX_IF_NOT_EXISTS(
            index_name="t_name_idx", fully_qualified_table_name=TABLE, columns=["name", COLUMN("id")]
        )
    )

    assert sql == (
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N't_name_idx' AND object_id = OBJECT_ID(N'[s].[t]')) "
        "CREATE INDEX [t_name_idx] ON [s].[t] ([name], [id]);"
    )


def test_create_index_doubles_quotes_in_its_catalog_lookup(dialect):
    sql = dialect.build_create_index_sql(
        CREATE_INDEX_IF_NOT_EXISTS("it's_idx", "[s].[it's]", ["name"]), add_semicolon=False
    )

    assert sql == (
        "IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name = N'it''s_idx' AND object_id = OBJECT_ID(N'[s].[it''s]')) "
        "CREATE INDEX [it's_idx] ON [s].[it's] ([name])"
    )


@pytest.mark.parametrize(
    "table_name, sys_indexes",
    [
        ("[db].[s].[t]", "[db].sys.indexes"),
        ("[my.db].[s].[t]", "[my.db].sys.indexes"),
        ("[a]]b].[s].[t]", "[a]]b].sys.indexes"),
        ("[db]..[t]", "[db].sys.indexes"),
        ("db.s.t", "db.sys.indexes"),
        ("[s].[t]", "sys.indexes"),
        ("[t]", "sys.indexes"),
    ],
)
def test_create_index_looks_the_index_up_in_the_tables_database(dialect, table_name, sys_indexes):
    # OBJECT_ID resolves a table in the database its name qualifies it with; sys.indexes lists the current
    # database's indexes alone, so a check there would miss the index on the second run (error 1913).
    sql = dialect.build_create_index_sql(CREATE_INDEX_IF_NOT_EXISTS("i", table_name, ["name"]), add_semicolon=False)

    assert sql == (
        f"IF NOT EXISTS (SELECT 1 FROM {sys_indexes} WHERE name = N'i' AND object_id = OBJECT_ID(N'{table_name}')) "
        f"CREATE INDEX [i] ON {table_name} ([name])"
    )


def test_an_index_column_must_be_named(dialect):
    with pytest.raises(ValueError, match="must be named"):
        dialect.build_create_index_sql(CREATE_INDEX_IF_NOT_EXISTS("i", TABLE, [COLUMN(LOWER(COLUMN("name")))]))


def test_analyze_table_updates_the_statistics(dialect):
    assert dialect.build_analyze_table_sql(ANALYZE_TABLE(TABLE)) == "UPDATE STATISTICS [s].[t];"


def test_create_schema_if_not_exists(dialect):
    sql = dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(prefixes=["db", "s"]))

    assert sql == "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N's') EXEC(N'CREATE SCHEMA [s]');"


@pytest.mark.parametrize(
    "schema_name, expected_sql",
    [
        (
            "it's",
            "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'it''s') EXEC(N'CREATE SCHEMA [it''s]')",
        ),
        ("a]b", "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'a]b') EXEC(N'CREATE SCHEMA [a]]b]')"),
    ],
)
def test_create_schema_escapes_the_name(dialect, schema_name, expected_sql):
    # The name is a string literal in the lookup, and a bracket-quoted identifier inside the EXEC'd literal.
    sql = dialect.build_create_schema_sql(CREATE_SCHEMA_IF_NOT_EXISTS(["db", schema_name]), add_semicolon=False)

    assert sql == expected_sql


def test_the_create_schema_method_is_unchanged(dialect):
    assert dialect.create_schema_if_not_exists_sql(["db", "s"]) == (
        "\n        IF NOT EXISTS ( SELECT  *\n"
        "                        FROM    sys.schemas\n"
        "                        WHERE   name = N's' )\n"
        "        EXEC('CREATE SCHEMA [s]')\n"
        "        ;"
    )


@pytest.mark.parametrize("node", [CREATE_TABLE, CREATE_TABLE_IF_NOT_EXISTS])
def test_create_table_ignores_update_heavy(dialect, node):
    # T-SQL has no table-level fill factor; only an index option sets one.
    def create_table(update_heavy: bool):
        return node(
            fully_qualified_table_name=TABLE,
            columns=[
                CREATE_TABLE_COLUMN(name="id", type=SqlDataType(name="int")),
                CREATE_TABLE_COLUMN(name="name", type=SqlDataType(name="varchar", character_maximum_length=255)),
            ],
            primary_key_column_names=["id"],
            update_heavy=update_heavy,
        )

    assert dialect.build_create_table_sql(create_table(True)) == dialect.build_create_table_sql(create_table(False))
    assert dialect.build_create_table_sql(create_table(True)).endswith("\tPRIMARY KEY ([id])\n);")


def test_a_column_default_can_be_the_current_timestamp(dialect):
    sql = dialect.build_create_table_sql(
        CREATE_TABLE_IF_NOT_EXISTS(
            fully_qualified_table_name=TABLE,
            columns=[
                CREATE_TABLE_COLUMN(
                    name="created_at", type=SqlDataType(name="datetimeoffset"), default=CURRENT_TIMESTAMP()
                )
            ],
        )
    )

    assert sql == (
        "IF OBJECT_ID('[s].[t]', 'U') IS NULL CREATE TABLE [s].[t] (\n"
        "\t[created_at] datetimeoffset DEFAULT SYSDATETIMEOFFSET()\n"
        ");"
    )


# ---------------------------------------------------------------------------
# Read-only transactions, LIKE ... ESCAPE
# ---------------------------------------------------------------------------


def test_read_only_transaction_statements(dialect):
    # No read-only transaction in T-SQL: writes made in it are rolled back, not refused.
    assert dialect.begin_read_only_transaction_sql() == "BEGIN TRANSACTION"
    assert dialect.rollback_sql() == "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION"


def test_like_with_an_escape_character(dialect):
    sql = dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("x!_y![a]%"), escape="!"))

    assert sql == "[name] like 'x!_y![a]%' ESCAPE '!'"


def test_like_without_an_escape_is_unchanged(dialect):
    assert dialect.build_expression_sql(LIKE(COLUMN("name"), LITERAL("%a%"))) == "[name] like '%a%'"


# ---------------------------------------------------------------------------
# Expressions
# ---------------------------------------------------------------------------

_EXPRESSIONS = [
    ("CURRENT_TIMESTAMP", CURRENT_TIMESTAMP(), "SYSDATETIMEOFFSET()"),
    ("NULLIF", NULLIF(COLUMN("n"), LITERAL(0)), "NULLIF([n], 0)"),
    ("ABS", ABS(COLUMN("n")), "ABS([n])"),
    ("ARITHMETIC", ARITHMETIC("+", COLUMN("fix_count", "cur"), LITERAL(1)), "([cur].[fix_count] + 1)"),
    (
        "ARITHMETIC nested",
        ARITHMETIC("/", ARITHMETIC("-", COLUMN("a"), COLUMN("b")), ARITHMETIC("*", LITERAL(2), COLUMN("c"))),
        "(([a] - [b]) / (2 * [c]))",
    ),
    ("PLACEHOLDER", PLACEHOLDER(), "?"),
    (
        "TIMESTAMP_ISO_TEXT",
        TIMESTAMP_ISO_TEXT(COLUMN("ts")),
        "STUFF(LEFT(CONVERT(varchar(27), CAST([ts] AS datetime2(7)), 121), 26), 11, 1, 'T')",
    ),
    (
        "TIMESTAMP_ISO_TEXT whole seconds",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False),
        "CONVERT(varchar(19), CAST([ts] AS datetime2(7)), 126)",
    ),
    (
        "TIMESTAMP_ISO_TEXT UTC suffix",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), utc_suffix=True),
        "(STUFF(LEFT(CONVERT(varchar(27), CAST([ts] AS datetime2(7)), 121), 26), 11, 1, 'T') + '+00:00')",
    ),
    (
        "TIMESTAMP_ISO_TEXT whole seconds UTC suffix",
        TIMESTAMP_ISO_TEXT(COLUMN("ts"), fractional_seconds=False, utc_suffix=True),
        "(CONVERT(varchar(19), CAST([ts] AS datetime2(7)), 126) + '+00:00')",
    ),
    ("DATE_ISO_TEXT", DATE_ISO_TEXT(COLUMN("d")), "CONVERT(char(10), [d], 23)"),
    ("TO_TIMEZONE UTC", TO_TIMEZONE(COLUMN("ts"), "UTC"), "CAST(SWITCHOFFSET([ts], '+00:00') AS datetime2(7))"),
    ("TO_TIMEZONE utc", TO_TIMEZONE(COLUMN("ts"), "utc"), "CAST(SWITCHOFFSET([ts], '+00:00') AS datetime2(7))"),
    ("TO_TIMEZONE Etc/UTC", TO_TIMEZONE(COLUMN("ts"), "Etc/UTC"), "CAST(SWITCHOFFSET([ts], '+00:00') AS datetime2(7))"),
    *[
        (f"DATE_TRUNC {unit}", DATE_TRUNC(unit, COLUMN("ts")), f"DATETRUNC({unit.upper()}, [ts])")
        for unit in DATE_TRUNC_UNITS
    ],
    (
        "REGEXP_REPLACE",
        REGEXP_REPLACE(COLUMN("s"), "^(-?[0-9]+)$", "\\1.0"),
        "REGEXP_REPLACE([s], '^(-?[0-9]+)$', '\\1.0')",
    ),
    ("REGEXP_REPLACE quotes", REGEXP_REPLACE(COLUMN("s"), "'", "''"), "REGEXP_REPLACE([s], '''', '''''')"),
]


@pytest.mark.parametrize(
    "expression, expected_sql", [case[1:] for case in _EXPRESSIONS], ids=[case[0] for case in _EXPRESSIONS]
)
def test_expression(dialect, expression, expected_sql):
    assert dialect.build_expression_sql(expression) == expected_sql


@pytest.mark.parametrize(
    "node_name, expression, reason",
    [
        ("EPOCH_SECONDS", EPOCH_SECONDS(COLUMN("i")), ""),
        ("JSON_MERGE", JSON_MERGE(COLUMN("a"), COLUMN("b")), ""),
        ("TO_TIMEZONE", TO_TIMEZONE(COLUMN("ts"), "Europe/Brussels"), " to 'Europe/Brussels': only UTC"),
        (
            "TO_TIMEZONE",
            TO_TIMEZONE(COLUMN("ts"), "W. Europe Standard Time"),
            " to 'W. Europe Standard Time': only UTC",
        ),
    ],
    ids=["EPOCH_SECONDS", "JSON_MERGE", "TO_TIMEZONE IANA zone", "TO_TIMEZONE Windows zone"],
)
def test_expressions_without_a_t_sql_rendering_are_refused(dialect, node_name, expression, reason):
    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_expression_sql(expression)

    assert str(error.value) == f"SqlServerSqlDialect does not support {node_name}{reason}"


# DATETRUNC is SQL Server 2022 (major version 16), REGEXP_REPLACE SQL Server 2025 (17). Azure SQL
# Database (edition 5) and Managed Instance (edition 8) report a legacy major version; both have
# DATETRUNC, and Azure SQL Database has REGEXP_REPLACE. A Managed Instance has it only under some
# update policies, which its facts do not show. Without server facts the newest engine is assumed.
@pytest.mark.parametrize(
    "server_major_version, engine_edition, renders_date_trunc, renders_regexp_replace",
    [
        (None, None, True, True),
        (14, 3, False, False),
        (15, 3, False, False),
        (16, 3, True, False),
        (16, None, True, False),
        (17, 3, True, True),
        (12, 5, True, True),
        (None, 5, True, True),
        (12, 8, True, False),
        (None, 8, True, False),
        (17, 8, True, True),
        (None, 3, False, False),
    ],
)
def test_newer_functions_render_only_on_an_engine_that_has_them(
    dialect, server_major_version, engine_edition, renders_date_trunc, renders_regexp_replace
):
    dialect.server_major_version = server_major_version
    dialect.engine_edition = engine_edition

    for expression, renders in [
        (DATE_TRUNC("day", COLUMN("ts")), renders_date_trunc),
        (REGEXP_REPLACE(COLUMN("s"), "[0-9]", ""), renders_regexp_replace),
    ]:
        if renders:
            dialect.build_expression_sql(expression)
        else:
            with pytest.raises(UnsupportedSqlStatementError):
                dialect.build_expression_sql(expression)


def test_an_engine_without_a_function_names_what_it_lacks(dialect):
    dialect.server_major_version = 15
    dialect.engine_edition = 3

    with pytest.raises(UnsupportedSqlStatementError) as date_trunc_error:
        dialect.build_expression_sql(DATE_TRUNC("day", COLUMN("ts")))
    with pytest.raises(UnsupportedSqlStatementError) as regexp_replace_error:
        dialect.build_expression_sql(REGEXP_REPLACE(COLUMN("s"), "[0-9]", ""))

    assert str(date_trunc_error.value) == (
        "SqlServerSqlDialect does not support DATE_TRUNC on this server (major version 15, engine edition 3): "
        "DATETRUNC needs SQL Server 2022, Azure SQL Database or Azure SQL Managed Instance"
    )
    assert str(regexp_replace_error.value) == (
        "SqlServerSqlDialect does not support REGEXP_REPLACE on this server (major version 15, engine edition 3): "
        "REGEXP_REPLACE needs SQL Server 2025 or Azure SQL Database"
    )


def test_a_managed_instance_is_refused_regexp_replace(dialect):
    dialect.server_major_version = 12
    dialect.engine_edition = 8

    with pytest.raises(UnsupportedSqlStatementError) as error:
        dialect.build_expression_sql(REGEXP_REPLACE(COLUMN("s"), "[0-9]", ""))

    assert str(error.value) == (
        "SqlServerSqlDialect does not support REGEXP_REPLACE on this server (major version 12, engine edition 8): "
        "REGEXP_REPLACE needs SQL Server 2025 or Azure SQL Database"
    )


# ---------------------------------------------------------------------------
# A dialect derived from SQL Server inherits these renderings but not the flag
# ---------------------------------------------------------------------------


class _UnverifiedSqlServerDerivative(SqlServerSqlDialect, sqlglot_dialect="tsql"):
    pass


def test_a_derived_dialect_keeps_the_base_current_timestamp():
    assert _UnverifiedSqlServerDerivative.SUPPORTS_DATA_PLANE_STATEMENTS is False
    assert _UnverifiedSqlServerDerivative().build_expression_sql(CURRENT_TIMESTAMP()) == "CURRENT_TIMESTAMP"


@pytest.mark.parametrize(
    "node_name, render",
    [
        ("UPDATE", lambda d: d.build_update_sql(UPDATE(TABLE, [ASSIGNMENT("n", LITERAL(1))], alias="tgt"))),
        ("DELETE", lambda d: d.build_delete_sql(DELETE(TABLE, alias="tgt"))),
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
        ("SOURCE_COLUMN", lambda d: d.build_expression_sql(SOURCE_COLUMN("n"))),
        ("TIMESTAMP_ISO_TEXT", lambda d: d.build_expression_sql(TIMESTAMP_ISO_TEXT(COLUMN("ts")))),
        ("DATE_TRUNC", lambda d: d.build_expression_sql(DATE_TRUNC("day", COLUMN("ts")))),
        ("TO_TIMEZONE", lambda d: d.build_expression_sql(TO_TIMEZONE(COLUMN("ts"), "UTC"))),
    ],
)
def test_a_derived_dialect_refuses_the_inherited_statements(node_name, render):
    with pytest.raises(UnsupportedSqlStatementError) as error:
        render(_UnverifiedSqlServerDerivative())

    assert str(error.value) == f"_UnverifiedSqlServerDerivative does not support {node_name}"


def test_a_derived_dialect_renders_no_optional_statement():
    derived = _UnverifiedSqlServerDerivative()

    assert derived.build_create_index_sql(CREATE_INDEX_IF_NOT_EXISTS("i", TABLE, ["id"])) is None
    assert derived.build_analyze_table_sql(ANALYZE_TABLE(TABLE)) is None
    assert derived.begin_read_only_transaction_sql() is None
    assert derived.rollback_sql() is None
