from soda_core.common.metadata_types import (
    DbSchemaDataSourceNamespace,
    SchemaDataSourceNamespace,
)
from soda_core.common.statements.metadata_primary_keys_query import (
    MetadataPrimaryKeysQuery,
)
from soda_postgres.common.data_sources.postgres_data_source import PostgresSqlDialect


def _postgres_query() -> MetadataPrimaryKeysQuery:
    # Postgres uses the base MetadataPrimaryKeysQuery with the postgres dialect for shared tokens.
    return MetadataPrimaryKeysQuery(sql_dialect=PostgresSqlDialect(), data_source_connection=None)


def test_postgres_primary_keys_sql_schema_only():
    query = _postgres_query()
    sql = query.sql_dialect.build_select_sql(
        query.build_sql_statement(table_namespace=SchemaDataSourceNamespace(schema="public"), table_name="orders")
    )
    assert sql == (
        'SELECT "kcu"."column_name"\n'
        'FROM "information_schema"."table_constraints" AS "tc"\n'
        '     JOIN "information_schema"."key_column_usage" AS "kcu" '
        'ON "tc"."constraint_name" = "kcu"."constraint_name"\n'
        'WHERE "tc"."constraint_type" = \'PRIMARY KEY\' '
        'AND "tc"."table_schema" = \'public\' '
        'AND "tc"."table_name" = \'orders\';'
    )


def test_postgres_primary_keys_sql_with_database():
    query = _postgres_query()
    sql = query.sql_dialect.build_select_sql(
        query.build_sql_statement(
            table_namespace=DbSchemaDataSourceNamespace(database="mydb", schema="public"),
            table_name="orders",
        )
    )
    assert '"tc"."table_catalog" = \'mydb\'' in sql
    assert '"mydb"."information_schema"."table_constraints"' in sql
