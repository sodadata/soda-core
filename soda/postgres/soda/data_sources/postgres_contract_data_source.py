from __future__ import annotations

import logging

from soda.contracts.impl.contract_data_source import FileClContractDataSource
from soda.contracts.impl.sql_dialect import SqlDialect
from soda.contracts.impl.yaml_helper import YamlFile

logger = logging.getLogger(__name__)


class PostgresSqlDialect(SqlDialect):
    pass

class PostgresContractDataSource(FileClContractDataSource):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file)

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        import psycopg2

        if connection_yaml_dict["password"] == "":
            connection_yaml_dict["password"] = None

        self._log_connection_properties_excl_pwd("postgres", connection_yaml_dict)

        if "username" in connection_yaml_dict and "user" not in connection_yaml_dict:
            raise ValueError("Rename postgres connection property username to user. See https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS")

        return psycopg2.connect(**connection_yaml_dict)
