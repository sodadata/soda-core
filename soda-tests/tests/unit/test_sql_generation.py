from __future__ import annotations

from unittest import mock

from soda_core.common.sql_dialect import *

mock_data_source_impl = mock.MagicMock()


def test_sql_ast_select_star():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql([SELECT(STAR()), FROM("customers")]) == ("SELECT *\n" 'FROM "customers";')


def test_sql_ast_modeling_query2():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql(
        [
            SELECT(["colA", FUNCTION("avg", ["colB", 25, FUNCTION("+", ["colD", "colE"])]), COLUMN("colC").AS("cc")]),
            FROM("customers"),
            WHERE(EQ("colA", LITERAL(25))),
        ]
    ) == (
        'SELECT "colA",\n'
        '       avg("colB", 25, ("colD" + "colE")),\n'
        '       "colC" AS "cc"\n'
        'FROM "customers"\n'
        'WHERE "colA" = 25;'
    )


def test_sql_ast_modeling_query3():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql(
        [
            SELECT(COLUMN("name").IN("C")),
            FROM("customers").AS("C").IN(["sdb", "mschm"]),
            WHERE(GTE("colA", LITERAL(25))),
            AND(EQ("colB", LITERAL("XXX"))),
            AND(LIKE("colC", LITERAL("%xxx"))),
        ]
    ) == (
        'SELECT "C"."name"\n'
        'FROM "sdb"."mschm"."customers" AS "C"\n'
        'WHERE "colA" >= 25\n'
        "  AND \"colB\" = 'XXX'\n"
        "  AND \"colC\" like '%xxx';"
    )


def test_sql_ast_modeling_cte():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql(
        [
            WITH([CTE("customers_filtered").AS([SELECT(STAR()), FROM("customers"), WHERE(GTE("colA", LITERAL(25)))])]),
            SELECT(SUM(COLUMN("size"))),
            FROM("customers_filtered"),
        ]
    ) == (
        "WITH \n"
        '"customers_filtered" AS (\n'
        "SELECT *\n"
        'FROM "customers"\n'
        'WHERE "colA" >= 25\n'
        ")\n"
        'SELECT SUM("size")\n'
        'FROM "customers_filtered";'
    )


def test_sql_ast_modeling_cte_with_multi_line_text_field():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql(
        [
            WITH(
                [
                    CTE("customers_filtered").AS(
                        [SELECT([STAR(), LITERAL("My\nLine")]), FROM("customers"), WHERE(GTE("colA", LITERAL(25)))]
                    )
                ]
            ),
            SELECT(SUM(COLUMN("size"))),
            FROM("customers_filtered"),
        ]
    ) == (
        "WITH \n"
        '"customers_filtered" AS (\n'
        "SELECT *,\n"
        "       'My\n"
        "Line'\n"
        'FROM "customers"\n'
        'WHERE "colA" >= 25\n'
        ")\n"
        'SELECT SUM("size")\n'
        'FROM "customers_filtered";'
    )


def test_sql_ast_create_table_if_not_exists():
    sql_dialect: SqlDialect = SqlDialect()

    my_create_table_statement = sql_dialect.build_create_table_sql(
        CREATE_TABLE_IF_NOT_EXISTS(
            fully_qualified_table_name='"customers"',
            columns=[
                CREATE_TABLE_COLUMN(
                    name="id",
                    type=SqlDataType(
                        name=SodaDataTypeName.INTEGER,
                    ),
                    nullable=False,
                ),
                CREATE_TABLE_COLUMN(
                    name="name",
                    type=SqlDataType(name="varchar", character_maximum_length=255),
                    nullable=True,
                ),
                CREATE_TABLE_COLUMN(name="age", type=SqlDataType(name="integer"), nullable=True, default=LITERAL(25)),
                CREATE_TABLE_COLUMN(name="my_float", type=SqlDataType(name="decimal"), nullable=True),
                CREATE_TABLE_COLUMN(
                    name="custom_type", type=SqlDataType(name="my_own_datatype"), nullable=True
                ),  # This is not in the contract type dict.
            ],
        )
    )
    assert (
        my_create_table_statement == 'CREATE TABLE IF NOT EXISTS "customers" (\n'
        '\t"id" integer NOT NULL,\n'
        '\t"name" varchar(255),\n'
        '\t"age" integer DEFAULT 25,\n'
        '\t"my_float" decimal,\n'
        '\t"custom_type" my_own_datatype\n'
        ");"
    )


def test_sql_ast_create_table_strips_inapplicable_precision_fields():
    """Regression for the postgres → athena/oracle/redshift leak: a timestamp
    column carrying a stray numeric_precision (e.g. from psycopg's polymorphic
    column.precision) used to render as `timestamp(2)` because
    get_sql_data_type_str_with_parameters picked the first non-None field.
    The base _build_create_table_column_type now strips fields the column's
    data type doesn't actually support before rendering."""
    sql_dialect: SqlDialect = SqlDialect()

    leaked_timestamp = CREATE_TABLE_COLUMN(
        name="ts",
        # numeric_precision is inapplicable to timestamp — must not leak into DDL.
        type=SqlDataType(name="timestamp", numeric_precision=2),
    )
    statement = sql_dialect.build_create_table_sql(
        CREATE_TABLE_IF_NOT_EXISTS(fully_qualified_table_name='"t"', columns=[leaked_timestamp])
    )
    assert '"ts" timestamp\n' in statement
    assert "timestamp(2)" not in statement
    # Defensive copy: rendering must not have mutated the caller's SqlDataType.
    assert leaked_timestamp.type.numeric_precision == 2


def test_sql_ast_insert_into_with_columns():
    sql_dialect: SqlDialect = SqlDialect()

    my_insert_into_statement = sql_dialect.build_insert_into_sql(
        INSERT_INTO(
            fully_qualified_table_name='"customers"',
            columns=[COLUMN("id"), COLUMN("name"), COLUMN("age")],
            values=[
                VALUES_ROW([LITERAL(1), LITERAL("John"), LITERAL(25)]),
                VALUES_ROW([LITERAL(2), LITERAL("Jane"), LITERAL(30)]),
            ],
        )
    )
    assert (
        my_insert_into_statement == 'INSERT INTO "customers" ("id", "name", "age") VALUES\n'
        "(1, 'John', 25),\n"
        "(2, 'Jane', 30);"
    )


def test_sql_ast_insert_into_with_datetimes():
    sql_dialect: SqlDialect = SqlDialect()

    now = datetime.now()

    my_insert_into_statement = sql_dialect.build_insert_into_sql(
        INSERT_INTO(
            fully_qualified_table_name='"customers"',
            values=[
                VALUES_ROW([LITERAL(1), LITERAL("John"), LITERAL(25), LITERAL(now)]),
                VALUES_ROW([LITERAL(2), LITERAL("Jane"), LITERAL(30), LITERAL(now)]),
            ],
            columns=[COLUMN("id"), COLUMN("name"), COLUMN("age"), COLUMN("my_date")],
        )
    )
    assert (
        my_insert_into_statement == f'INSERT INTO "customers" ("id", "name", "age", "my_date") VALUES\n'
        f"(1, 'John', 25, '{now.isoformat()}'),\n"
        f"(2, 'Jane', 30, '{now.isoformat()}');"
    )


def test_sql_ast_insert_into_via_select():
    sql_dialect: SqlDialect = SqlDialect()
    my_insert_into_statement = sql_dialect.build_insert_into_via_select_sql(
        INSERT_INTO_VIA_SELECT(
            fully_qualified_table_name='"customers"',
            select_elements=[
                SELECT(COLUMN("name").IN("C")),
                FROM("customers").AS("C").IN(["sdb", "mschm"]),
                WHERE(GTE("colA", LITERAL(25))),
                AND(EQ("colB", LITERAL("XXX"))),
                AND(LIKE("colC", LITERAL("%xxx"))),
            ],
            columns=[COLUMN("name"), COLUMN("age")],
        )
    )
    assert (
        my_insert_into_statement
        == 'INSERT INTO "customers"\n'
        + ' ("name", "age")\n'
        + "(\n"
        + 'SELECT "C"."name"\n'
        + 'FROM "sdb"."mschm"."customers" AS "C"\n'
        + 'WHERE "colA" >= 25\n'
        + "  AND \"colB\" = 'XXX'\n"
        + "  AND \"colC\" like '%xxx'\n"
        + ");"
    )


def test_sql_ast_drop_table():
    sql_dialect: SqlDialect = SqlDialect()

    my_drop_table_statement = sql_dialect.build_drop_table_sql(DROP_TABLE(fully_qualified_table_name='"customers"'))
    assert my_drop_table_statement == 'DROP TABLE "customers";'


def test_sql_ast_drop_table_if_exists():
    sql_dialect: SqlDialect = SqlDialect()

    my_drop_table_statement = sql_dialect.build_drop_table_sql(
        DROP_TABLE_IF_EXISTS(fully_qualified_table_name='"customers"')
    )
    assert my_drop_table_statement == 'DROP TABLE IF EXISTS "customers";'


def test_sql_ast_alter_table_add_column():
    sql_dialect: SqlDialect = SqlDialect()

    my_alter_table_statement = sql_dialect.build_alter_table_sql(
        ALTER_TABLE_ADD_COLUMN(
            fully_qualified_table_name='"customers"',
            column=CREATE_TABLE_COLUMN(
                name="age", type=SqlDataType(name=SodaDataTypeName.INTEGER), nullable=False, default=LITERAL(25)
            ),
        )
    )
    assert my_alter_table_statement == 'ALTER TABLE "customers" ADD COLUMN "age" integer NOT NULL DEFAULT 25;'


def test_sql_ast_alter_table_drop_column():
    sql_dialect: SqlDialect = SqlDialect()

    my_alter_table_statement = sql_dialect.build_alter_table_sql(
        ALTER_TABLE_DROP_COLUMN(fully_qualified_table_name='"customers"', column_name="age")
    )
    assert my_alter_table_statement == 'ALTER TABLE "customers" DROP COLUMN "age";'


def test_sql_ast_create_table_as_select():
    sql_dialect: SqlDialect = SqlDialect()

    my_create_table_as_select_statement = sql_dialect.build_create_table_as_select_sql(
        CREATE_TABLE_AS_SELECT(
            fully_qualified_table_name='"customers"',
            select_elements=[SELECT(COLUMN("name")), FROM("customers")],
        )
    )
    assert my_create_table_as_select_statement == 'CREATE TABLE "customers" AS (\nSELECT "name"\nFROM "customers");'


def test_sql_ast_union():
    sql_dialect: SqlDialect = SqlDialect()

    my_union_statement = sql_dialect.build_union_sql(
        UNION(select_elements=[[SELECT(COLUMN("name")), FROM("customers")], [SELECT(COLUMN("age")), FROM("customers")]])
    )
    assert my_union_statement == '(\nSELECT "name"\nFROM "customers"\n)\nUNION\n(\nSELECT "age"\nFROM "customers"\n);'


def test_sql_ast_union_all():
    sql_dialect: SqlDialect = SqlDialect()

    my_union_statement = sql_dialect.build_union_sql(
        UNION_ALL(
            select_elements=[[SELECT(COLUMN("name")), FROM("customers")], [SELECT(COLUMN("age")), FROM("customers")]]
        )
    )
    assert (
        my_union_statement == '(\nSELECT "name"\nFROM "customers"\n)\nUNION ALL\n(\nSELECT "age"\nFROM "customers"\n);'
    )


# ---------------------------------------------------------------------------
# ALIAS — DTL-1780 consolidation
# ---------------------------------------------------------------------------


def test_alias_renders_expression_with_quoted_alias():
    """Baseline: ALIAS(<expr>, name) emits `<expr> AS "<name>"`."""
    sql_dialect: SqlDialect = SqlDialect()
    assert sql_dialect.build_expression_sql(ALIAS(COLUMN("name"), "n")) == '"name" AS "n"'


def test_alias_wraps_arbitrary_expression():
    """ALIAS must wrap any SqlExpression (LITERAL, COUNT, COMBINED_HASH, etc.) — the
    pre-DTL-1780 per-class field_alias mechanism only worked on COLUMN and COUNT."""
    sql_dialect: SqlDialect = SqlDialect()
    assert sql_dialect.build_expression_sql(ALIAS(LITERAL(None), "database_name")) == 'NULL AS "database_name"'
    assert sql_dialect.build_expression_sql(ALIAS(COUNT(STAR()), "n_count")) == 'COUNT(*) AS "n_count"'


def test_column_AS_returns_alias_node():
    """`COLUMN(...).AS("x")` is the legacy fluent API for aliasing. Post-DTL-1780 it
    must return an ALIAS wrapper (not a mutated COLUMN with field_alias) so that
    consumers can inspect the alias as `.alias` instead of `.field_alias`."""
    aliased = COLUMN("name").AS("output")
    assert isinstance(aliased, ALIAS), f"COLUMN.AS must return ALIAS; got {type(aliased).__name__}"
    assert aliased.alias == "output"
    sql_dialect: SqlDialect = SqlDialect()
    assert sql_dialect.build_expression_sql(aliased) == '"name" AS "output"'


def test_column_no_longer_accepts_field_alias_kwarg():
    """Regression guard: a re-introduction of `field_alias` on COLUMN would silently
    revive the deprecated per-class aliasing path."""
    import pytest

    with pytest.raises(TypeError):
        COLUMN("name", field_alias="x")  # type: ignore[call-arg]


def test_count_no_longer_accepts_field_alias_kwarg():
    """Same guard for COUNT."""
    import pytest

    with pytest.raises(TypeError):
        COUNT(STAR(), field_alias="x")  # type: ignore[call-arg]


def test_alias_in_select_list_emits_in_sql():
    """End-to-end: ALIAS inside SELECT emits the `AS` clause in the rendered SQL.
    Load-bearing case for metadata queries that synthesize literal-backed output
    columns (e.g. `NULL AS "database_name"`)."""
    sql_dialect: SqlDialect = SqlDialect()
    sql = sql_dialect.build_select_sql(
        [
            SELECT([ALIAS(LITERAL(None), "database_name"), ALIAS(COLUMN("name"), "table_name")]),
            FROM("information_schema.tables"),
        ]
    )
    assert 'NULL AS "database_name"' in sql
    assert '"name" AS "table_name"' in sql


def test_AS_method_works_on_any_sql_expression():
    """`.AS()` lives on `SqlExpression` so every expression type — not just COLUMN —
    can be aliased fluently. `expr.AS("x")` is equivalent to `ALIAS(expr, "x")`."""
    sql_dialect: SqlDialect = SqlDialect()
    assert sql_dialect.build_expression_sql(LITERAL(None).AS("database_name")) == 'NULL AS "database_name"'
    assert sql_dialect.build_expression_sql(COUNT(STAR()).AS("n_count")) == 'COUNT(*) AS "n_count"'
    assert sql_dialect.build_expression_sql(RAW_SQL("current_database()").AS("table_catalog")) == (
        'current_database() AS "table_catalog"'
    )
    assert isinstance(LITERAL(None).AS("x"), ALIAS)
