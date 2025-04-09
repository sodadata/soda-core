from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)
from soda_postgres.model.data_source.postgres_data_source import (
    PostgresDataSource as PostgresDataSourceModel,
)


class PostgresDataSourceImpl(DataSourceImpl):
    def __init__(self, data_source_model: PostgresDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def get_data_source_type_name(self) -> str:
        return "postgres"

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return PostgresDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class PostgresSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"{expression} ~ '{matches.regex_pattern}'"
