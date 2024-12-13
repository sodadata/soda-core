from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.logs import Logs
from soda_core.common.sql_dialect import SqlDialect
from soda_postgres.common.data_sources.postgres_data_source_connection import PostgresDataSourceConnection


class PostgresDataSource(DataSource):

    def __init__(self, data_source_yaml_file: YamlFile, name: str, type_name: str, connection_properties: dict):
        super().__init__(data_source_yaml_file, name, type_name, connection_properties)

    def _get_data_source_type_name(self) -> str:
        return "postgres"

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_data_source_connection(
        self,
        name: str,
        connection_properties: dict,
        logs: Logs
    ) -> DataSourceConnection:
        return PostgresDataSourceConnection(
            name=name,
            connection_properties=connection_properties,
            logs=logs
        )


class PostgresSqlDialect(SqlDialect):

    def __init__(self):
        super().__init__()
