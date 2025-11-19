from __future__ import annotations

from unittest import mock

from soda_core.common.sql_dialect import *

mock_data_source_impl = mock.MagicMock()


def test_sql_ast_select_star():
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

    assert sql_dialect.build_select_sql([SELECT(STAR()), FROM("customers")]) == ("SELECT *\n" 'FROM "customers";')


def test_sql_ast_modeling_query2():
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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


def test_sql_ast_insert_into_with_columns():
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)
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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

    my_drop_table_statement = sql_dialect.build_drop_table_sql(DROP_TABLE(fully_qualified_table_name='"customers"'))
    assert my_drop_table_statement == 'DROP TABLE "customers";'


def test_sql_ast_drop_table_if_exists():
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

    my_drop_table_statement = sql_dialect.build_drop_table_sql(
        DROP_TABLE_IF_EXISTS(fully_qualified_table_name='"customers"')
    )
    assert my_drop_table_statement == 'DROP TABLE IF EXISTS "customers";'


def test_sql_ast_alter_table_add_column():
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
    sql_dialect: SqlDialect = SqlDialect(mock_data_source_impl)

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
