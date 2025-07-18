from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import TUPLE
from soda_core.common.sql_dialect import SqlDialect
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSource as SnowflakeDataSourceModel,
)
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSourceConnection,
)


class SnowflakeDataSourceImpl(DataSourceImpl, model_class=SnowflakeDataSourceModel):
    def __init__(self, data_source_model: SnowflakeDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SnowflakeSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SnowflakeDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SnowflakeSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        return f"ARRAY_CONSTRUCT{super()._build_tuple_sql(tuple)}"
