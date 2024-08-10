from __future__ import annotations

import logging
import re

from soda.contracts.impl.contract_data_source import FileClContractDataSource
from soda.contracts.impl.sql_dialect import SqlDialect
from soda.contracts.impl.yaml_helper import YamlFile

logger = logging.getLogger(__name__)


class SnowflakeSqlDialect(SqlDialect):


    def stmt_drop_schema_if_exists(self, database_name: str, schema_name: str) -> str:
        return f"DROP SCHEMA IF EXISTS {schema_name} CASCADE"

    def stmt_create_schema_if_not_exists(self, database_name: str, schema_name: str) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name}"

    def escape_regex(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def regex_replace_flags(self) -> str:
        return ""

    def expr_regexp_like(self, expr: str, pattern: str):
        return f"REGEXP_LIKE(COLLATE({expr}, ''), '{pattern}')"

    def cast_text_to_number(self, column_name, validity_format: str):
        """Cast string to number
        - first regex replace removes extra chars, keeps: "digits + - . ,"
        - second regex changes "," to "."
        - Nullif makes sure that if regexes return empty string then Null is returned instead
        """
        regex = self.escape_regex(r"'[^-0-9\.\,]'")
        return f"CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE(COLLATE({column_name}, ''), {regex}, ''{self.regex_replace_flags()}), ',', '.'{self.regex_replace_flags()}), '') AS {self.SQL_TYPE_FOR_CREATE_TABLE_MAP[DataType.DECIMAL]})"

    # def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: list[object] | None, expr: str):
    #     # TODO add all of these snowflake specific statistical aggregate functions: https://docs.snowflake.com/en/sql-reference/functions-aggregation.html
    #     if metric_name in [
    #         "stddev",
    #         "stddev_pop",
    #         "stddev_samp",
    #         "variance",
    #         "var_pop",
    #         "var_samp",
    #     ]:
    #         return f"{metric_name.upper()}({expr})"
    #     if metric_name in ["percentile", "percentile_disc"]:
    #         # TODO ensure proper error if the metric_args[0] is not a valid number
    #         percentile_fraction = metric_args[1] if metric_args else None
    #         return f"PERCENTILE_DISC({percentile_fraction}) WITHIN GROUP (ORDER BY {expr})"
    #     return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    def sql_get_table_names_with_count(
        self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None
    ) -> str:
        table_filter_expression = self.sql_table_include_exclude_filter(
            "table_name", "table_schema", include_tables, exclude_tables
        )
        where_clause = f"AND {table_filter_expression}" if table_filter_expression else ""
        sql = f"""
            SELECT table_name, row_count
            FROM information_schema.tables
            WHERE table_schema != 'INFORMATION_SCHEMA'
            {where_clause}
            """
        return sql

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()


class SnowflakeContractDataSource(FileClContractDataSource):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file)

    def _create_sql_dialect(self) -> SqlDialect:
        return SnowflakeSqlDialect()

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        from snowflake import connector
        self._log_connection_properties_excl_pwd("postgres", connection_yaml_dict)

        private_key = self.__get_private_key(connection_yaml_dict)
        if private_key:
            connection_yaml_dict["private_key"] = private_key

        self.connection = connector.connect(**connection_yaml_dict)
        return self.connection

    def __get_private_key(self, connection_yaml_dict: dict) -> bytes | None:
        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        private_key: str | None = connection_yaml_dict.get("private_key")
        private_key_path: str | None = connection_yaml_dict.get("private_key_path")
        private_key_passphrase: str | None = connection_yaml_dict.get("private_key_passphrase")

        if not (private_key_path or private_key):
            return None

        if private_key_passphrase:
            encoded_passphrase = private_key_passphrase.encode()
        else:
            encoded_passphrase = None

        pk_bytes = None
        if private_key:
            pk_bytes = private_key.encode()
        elif private_key_path:
            with open(private_key_path, "rb") as pk:
                pk_bytes = pk.read()

        p_key = serialization.load_pem_private_key(pk_bytes, password=encoded_passphrase, backend=default_backend())

        return p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
