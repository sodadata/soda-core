from __future__ import annotations

from soda_core.common.sql_dialect import *


def test_sql_ast_modeling_query1():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql([
        SELECT(STAR()),
        FROM("customers")
    ]) == (
        'SELECT *\n'
        'FROM "customers";'
    )


def test_sql_ast_modeling_query2():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql([
        SELECT(["colA", "colB", "colC"]),
        FROM("customers"),
        WHERE(EQ("colA", LITERAL(25)))
    ]) == (
        'SELECT "colA",\n'
        '       "colB",\n'
        '       "colC"\n'
        'FROM "customers"\n'
        'WHERE "colA" = 25;'
    )


def test_sql_ast_modeling_query3():
    sql_dialect: SqlDialect = SqlDialect()

    assert sql_dialect.build_select_sql([
        SELECT(COLUMN("name").IN("C")),
        FROM("customers").AS("C"),
        WHERE(EQ("colA", LITERAL(25))),
          AND(EQ("colB", LITERAL("XXX")))
    ]) == (
        'SELECT "C"."name"\n'
        'FROM "customers" AS "C"\n'
        'WHERE "colA" = 25\n'
        '  AND "colB" = \'XXX\';'
    )
