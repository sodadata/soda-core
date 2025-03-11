from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logs import Logs
from soda_core.common.sql_ast import REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.yaml import YamlFileContent
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)


class PostgresDataSource(DataSource):
    def __init__(
        self,
        data_source_yaml_file_content: YamlFileContent,
        name: str,
        type_name: str,
        connection_properties: dict,
        format_regexes: dict[str, str],
    ):
        super().__init__(data_source_yaml_file_content, name, type_name, connection_properties, format_regexes)

    def get_data_source_type_name(self) -> str:
        return "postgres"

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_data_source_connection(
        self, name: str, connection_properties: dict, logs: Logs
    ) -> DataSourceConnection:
        return PostgresDataSourceConnection(name=name, connection_properties=connection_properties, logs=logs)


class PostgresSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"{expression} ~ '{matches.regex_pattern}'"
