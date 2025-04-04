from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.yaml import YamlSource
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)


class PostgresDataSourceImpl(DataSourceImpl):
    def __init__(
        self,
        data_source_yaml_source: YamlSource,
        name: str,
        type_name: str,
        connection_properties: dict,
    ):
        super().__init__(data_source_yaml_source, name, type_name, connection_properties)

    def get_data_source_type_name(self) -> str:
        return "postgres"

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_data_source_connection(self, name: str, connection_properties: dict) -> DataSourceConnection:
        return PostgresDataSourceConnection(name=name, connection_properties=connection_properties)


class PostgresSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"{expression} ~ '{matches.regex_pattern}'"
