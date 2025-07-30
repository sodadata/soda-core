from __future__ import annotations

from datetime import datetime

from soda_core.common.sql_dialect import *


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
            WITH("customers_filtered").AS([SELECT(STAR()), FROM("customers"), WHERE(GTE("colA", LITERAL(25)))]),
            SELECT(SUM(COLUMN("size"))),
            FROM("customers_filtered"),
        ]
    ) == (
        'WITH "customers_filtered" AS (\n'
        "  SELECT *\n"
        '  FROM "customers"\n'
        '  WHERE "colA" >= 25\n'
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
                CREATE_TABLE_COLUMN(name="id", type=DBDataType.INTEGER, nullable=False),
                CREATE_TABLE_COLUMN(name="name", type=DBDataType.TEXT, length=255, nullable=True),
                CREATE_TABLE_COLUMN(name="age", type=DBDataType.INTEGER, nullable=True, default=LITERAL(25)),
                CREATE_TABLE_COLUMN(name="my_float", type=DBDataType.DECIMAL, nullable=True),
                CREATE_TABLE_COLUMN(
                    name="custom_type", type="my_own_datatype", nullable=True
                ),  # This is not in the contract type dict.
            ],
        )
    )
    assert (
        my_create_table_statement == 'CREATE TABLE IF NOT EXISTS "customers" (\n'
        '\t"id" integer NOT NULL,\n'
        '\t"name" character varying(255),\n'
        '\t"age" integer DEFAULT 25,\n'
        '\t"my_float" double precision,\n'
        '\t"custom_type" my_own_datatype\n'
        ");"
    )


def test_sql_ast_insert_into_no_columns():
    sql_dialect: SqlDialect = SqlDialect()

    my_insert_into_statement = sql_dialect.build_insert_into_sql(
        INSERT_INTO(
            fully_qualified_table_name='"customers"',
            values=[
                VALUES_ROW([LITERAL(1), LITERAL("John"), LITERAL(25)]),
                VALUES_ROW([LITERAL(2), LITERAL("Jane"), LITERAL(30)]),
            ],
        )
    )

    assert my_insert_into_statement == ('INSERT INTO "customers" VALUES\n' + "(1, 'John', 25),\n" + "(2, 'Jane', 30);")


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
        )
    )
    assert (
        my_insert_into_statement == f'INSERT INTO "customers" VALUES\n'
        f"(1, 'John', 25, '{now.isoformat()}'),\n"
        f"(2, 'Jane', 30, '{now.isoformat()}');"
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
