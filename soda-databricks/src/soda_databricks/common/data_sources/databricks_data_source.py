from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    DatabricksDataSourceConnection,
)
from soda_databricks.model.data_source.databricks_data_source import (
    DatabricksDataSource as DatabricksDataSourceModel,
)
from soda_databricks.common.statements.databricks_metadata_columns_query import DatabricksMetadataColumnsQuery


class DatabricksDataSourceImpl(DataSourceImpl, model_class=DatabricksDataSourceModel):
    def __init__(self, data_source_model: DatabricksDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return DatabricksSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return DatabricksDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.default_connection
        )

    def create_metadata_columns_query(self) -> DatabricksMetadataColumnsQuery:
        return DatabricksMetadataColumnsQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection
        )


class DatabricksSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def _get_default_quote_char(self) -> str:
        return "`"

    def column_data_type(self) -> str:
        return self.default_casify("full_data_type")
