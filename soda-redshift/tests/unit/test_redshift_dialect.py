from soda_core.common.metadata_types import DbSchemaDataSourceNamespace
from soda_core.common.sql_dialect import FROM, RANDOM, SELECT
from soda_redshift.common.data_sources.redshift_data_source import RedshiftSqlDialect


def test_random():
    sql_dialect: RedshiftSqlDialect = RedshiftSqlDialect()
    sql = sql_dialect.build_select_sql([SELECT(RANDOM()), FROM("a")])
    assert sql == 'SELECT RANDOM()\nFROM "a";'


def test_schema_existence_query_uses_svv_all_schemas_not_information_schema():
    # Regression for SCS-1193: information_schema.schemata on Redshift only lists schemas the
    # current user OWNS, so a pre-provisioned DWH schema the service user merely has USAGE on is
    # invisible -> Soda wrongly issues CREATE SCHEMA -> "permission denied for database". The
    # existence check must query SVV_ALL_SCHEMAS (access-visible), like svv_tables / svv_columns.
    sql_dialect: RedshiftSqlDialect = RedshiftSqlDialect()
    sql = sql_dialect.build_schemas_metadata_query_str(
        table_namespace=DbSchemaDataSourceNamespace(database="comm", schema="qe_gold"),
        filter_on_schema_name="qe_gold",
    )
    lowered = sql.lower()
    assert "svv_all_schemas" in lowered
    assert "information_schema" not in lowered
    # SVV_ALL_SCHEMAS exposes the database via database_name (not information_schema's catalog_name)
    assert "database_name" in lowered
    assert "catalog_name" not in lowered
    # still filters on the target database + schema so it answers "does THIS schema exist"
    assert "comm" in lowered
    assert "qe_gold" in lowered


def test_max_sql_statement_length_respects_redshift_16mb_cap():
    # Redshift's documented statement cap is 16 MB; the dialect reserves
    # 1 MB for multi-byte characters (we count chars, the server counts bytes).
    sql_dialect: RedshiftSqlDialect = RedshiftSqlDialect()
    assert sql_dialect.get_max_sql_statement_length() == 15 * 1024 * 1024
    assert sql_dialect.get_max_sql_statement_length() < 16 * 1024 * 1024


def test_is_system_schema():
    sql_dialect: RedshiftSqlDialect = RedshiftSqlDialect()
    assert sql_dialect.is_system_schema("pg_catalog") is True
    assert sql_dialect.is_system_schema("PG_AUTOMV") is True
    assert sql_dialect.is_system_schema("information_schema") is True
    assert sql_dialect.is_system_schema("public") is False


def test_time_delta_inherits_base_epoch_floor_form():
    from datetime import datetime

    from soda_core.common.sql_ast import LITERAL, TIME_DELTA, SqlExpressionStr

    sql = RedshiftSqlDialect().build_expression_sql(
        TIME_DELTA(LITERAL(datetime(2020, 6, 20)), SqlExpressionStr('"ts"'), "days", 1)
    )
    assert sql == "FLOOR(EXTRACT(EPOCH FROM (\"ts\") - '2020-06-20T00:00:00') / 86400)"


def test_add_interval_inherits_base_interval_multiply_form():
    from datetime import datetime

    from soda_core.common.sql_ast import ADD_INTERVAL, LITERAL, SqlExpressionStr

    sql = RedshiftSqlDialect().build_expression_sql(
        ADD_INTERVAL(LITERAL(datetime(2020, 6, 20)), "days", SqlExpressionStr("(soda_partition__ + 1) * 1"))
    )
    assert sql == "'2020-06-20T00:00:00' + INTERVAL '1 days' * ((soda_partition__ + 1) * 1)"


def test_percentile_within_group_renders_approximate_percentile_disc():
    from soda_core.common.sql_ast import COLUMN, PERCENTILE_WITHIN_GROUP

    sql = RedshiftSqlDialect().build_expression_sql(PERCENTILE_WITHIN_GROUP(COLUMN("c"), 0.75))
    assert sql == 'APPROXIMATE PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY "c")'


def test_supports_percentile_within_group_is_true():
    assert RedshiftSqlDialect().supports_percentile_within_group() is True


def test_percentiles_cannot_share_select_with_other_distinct_aggregates():
    """Redshift refuses PERCENTILE_CONT/DISC/MEDIAN combined with other
    DISTINCT aggregates in one SELECT; consumers batch them separately."""
    assert RedshiftSqlDialect().supports_percentiles_with_other_distinct_aggregates() is False


def test_primary_keys_query_reads_pg_catalog_not_information_schema():
    """The PK query must read pg_catalog.pg_constraint: Redshift's Postgres-8-lineage
    information_schema constraint views only list tables the current user owns, so a
    read-only monitoring user silently gets zero primary keys through them (same reason
    the schemas/tables/columns queries read SVV/pg_catalog views)."""
    from soda_redshift.statements.redshift_metadata_primary_keys_query import (
        RedshiftMetadataPrimaryKeysQuery,
    )

    dialect = RedshiftSqlDialect()
    query = RedshiftMetadataPrimaryKeysQuery(sql_dialect=dialect, data_source_connection=None)
    namespace = query._build_namespace(["soda_test", "MySchema"])
    sql = dialect.build_select_sql(query.build_sql_statement(namespace, ["orders"]))

    assert "information_schema" not in sql
    assert '"pg_catalog"."pg_constraint"' in sql
    # pg_get_constraintdef carries the key columns in declared order (Redshift has no
    # array_position to sort a conkey join).
    assert "pg_get_constraintdef(constraints.oid)" in sql
    # Schema filter is case-insensitive, so a schema spelled in a different case doesn't
    # silently match nothing.
    assert 'LOWER("schemas"."nspname") = \'myschema\'' in sql


def test_primary_keys_constraint_definition_parsing_preserves_order():
    from soda_core.common.data_source_results import QueryResult
    from soda_redshift.statements.redshift_metadata_primary_keys_query import (
        RedshiftMetadataPrimaryKeysQuery,
    )

    parse = RedshiftMetadataPrimaryKeysQuery._parse_primary_key_columns
    assert parse("PRIMARY KEY (id)") == ["id"]
    assert parse("PRIMARY KEY (tenant_id, id)") == ["tenant_id", "id"]
    # Quoted identifiers: embedded commas and doubled-quote escapes must survive.
    assert parse('PRIMARY KEY ("Tenant, Id", id)') == ["Tenant, Id", "id"]
    assert parse('PRIMARY KEY ("we""ird", other)') == ['we"ird', "other"]

    query = RedshiftMetadataPrimaryKeysQuery(sql_dialect=RedshiftSqlDialect(), data_source_connection=None)
    query_result = QueryResult(
        columns=[("table_name",), ("constraint_definition",)],
        rows=[
            ("orders", "PRIMARY KEY (tenant_id, id)"),
            ("customers", "PRIMARY KEY (id)"),
        ],
    )
    assert query.get_results(query_result) == {
        "orders": ["tenant_id", "id"],
        "customers": ["id"],
    }
