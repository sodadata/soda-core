from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect
from soda_postgres.common.data_sources.postgres_data_source import PostgresDataSourceImpl, PostgresSqlDialect
from soda_supabase.common.data_sources.supabase_data_source_connection import (
    SupabaseDataSource as SupabaseDataSourceModel,
)
from soda_supabase.common.data_sources.supabase_data_source_connection import SupabaseDataSourceConnection


class SupabaseDataSourceImpl(PostgresDataSourceImpl, model_class=SupabaseDataSourceModel):
    def _create_sql_dialect(self) -> SqlDialect:
        return SupabaseSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SupabaseDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SupabaseSqlDialect(PostgresSqlDialect, sqlglot_dialect="postgres"):
    # Not inherited from PostgresSqlDialect, see SqlDialect.__init_subclass__. The integration suite
    # verifies the data plane statements on supabase as it does on postgres.
    SUPPORTS_DATA_PLANE_STATEMENTS = True
