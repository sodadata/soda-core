from __future__ import annotations

from soda_core.common.data_source_connection import DataSourceConnection


class PostgresDataSourceConnection(DataSourceConnection):
    def __init__(self, name: str, connection_properties: dict):
        super().__init__(name, connection_properties)

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        import psycopg2

        if not "password" in connection_yaml_dict or connection_yaml_dict["password"] == "":
            connection_yaml_dict["password"] = None

        if "username" in connection_yaml_dict and "user" not in connection_yaml_dict:
            raise ValueError(
                "Rename postgres connection property username to user. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS"
            )

        return psycopg2.connect(**connection_yaml_dict)
