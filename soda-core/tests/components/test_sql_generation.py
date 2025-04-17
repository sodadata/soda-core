from __future__ import annotations

from soda_core.common.sql_dialect import *


def test_sql_ast_select_star():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql(
        [
            SELECT(STAR()),
            FROM("customers")
        ]) == (
        'SELECT *\n'
        'FROM "customers";'
    )


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
            WITH([
                SELECT(STAR()),
                FROM("customers"),
                WHERE(GTE("colA", LITERAL(25)))
            ]).AS("customers_filtered"),
            SELECT(SUM(COLUMN("size"))),
            FROM("customers_filtered")
        ]
    ) == (
        'WITH (\n'
        '  SELECT *\n'
        '  FROM "customers"\n'
        '  WHERE "colA" >= 25\n'
        ') AS "customers_filtered",\n'
        'SELECT SUM("size")\n'
        'FROM "customers_filtered";'
    )
