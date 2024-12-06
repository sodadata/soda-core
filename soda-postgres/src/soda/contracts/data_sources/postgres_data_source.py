from __future__ import annotations

from soda.common.data_source import DataSource
from soda.common.data_source_connection import DataSourceConnection
from soda.common.logs import Logs
from soda.common.sql_dialect import SqlDialect
from soda.common.yaml import YamlFile


class PostgresDataSourceConnection(DataSourceConnection):

    def __init__(self, name: str, connection_properties: dict, logs: Logs):
        super().__init__(name, connection_properties, logs)

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        import psycopg2

        if not "password" in connection_yaml_dict or connection_yaml_dict["password"] == "":
            connection_yaml_dict["password"] = None

        if "username" in connection_yaml_dict and "user" not in connection_yaml_dict:
            raise ValueError(
                "Rename postgres connection property username to user. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS"
            )

        return psycopg2.connect(**connection_yaml_dict)


class PostgresSqlDialect(SqlDialect):

    def __init__(self):
        super().__init__()


class PostgresDataSource(DataSource):

    def __init__(self, data_source_yaml_file: YamlFile, name: str, type_name: str, connection_properties: dict):
        super().__init__(data_source_yaml_file, name, type_name, connection_properties)

    def _get_data_source_type_name(self) -> str:
        return "postgres"

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

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()
