from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSourceConnection,
)
from soda_bigquery.common.statements.metadata_tables_query import (
    BigQueryMetadataTablesQuery,
)
from soda_bigquery.model.data_source.bigquery_data_source import (
    BigQueryDataSource as BigQueryDataSourceModel,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import TUPLE
from soda_core.common.sql_dialect import SqlDialect


class BigQueryDataSourceImpl(DataSourceImpl, model_class=BigQueryDataSourceModel):
    def __init__(self, data_source_model: BigQueryDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return BigQuerySqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return BigQueryDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def get_location(self) -> str:
        return self.data_source_model.connection_properties.location

    def create_metadata_tables_query(self) -> BigQueryMetadataTablesQuery:
        super_metadata_tables_query = BigQueryMetadataTablesQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            location=self.get_location(),
        )
        return super_metadata_tables_query


class BigQuerySqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        return f"ARRAY_CONSTRUCT{super()._build_tuple_sql(tuple)}"
