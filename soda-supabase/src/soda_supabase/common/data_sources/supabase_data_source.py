from soda_core.common.data_source_connection import DataSourceConnection
from soda_postgres.common.data_sources.postgres_data_source import PostgresDataSourceImpl
from soda_supabase.common.data_sources.supabase_data_source_connection import (
    SupabaseDataSource as SupabaseDataSourceModel,
)
from soda_supabase.common.data_sources.supabase_data_source_connection import SupabaseDataSourceConnection


class SupabaseDataSourceImpl(PostgresDataSourceImpl, model_class=SupabaseDataSourceModel):
    def _create_data_source_connection(self) -> DataSourceConnection:
        return SupabaseDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )
