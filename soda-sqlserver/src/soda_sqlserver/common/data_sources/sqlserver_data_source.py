import logging
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import (
    COUNT,
    CREATE_TABLE,
    CREATE_TABLE_IF_NOT_EXISTS,
    DISTINCT,
    DROP_TABLE,
    DROP_TABLE_IF_EXISTS,
    LENGTH,
    REGEX_LIKE,
    TUPLE,
)
from soda_core.common.sql_dialect import DBDataType, SqlDialect
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SQLServerDataSource as SQLServerDataSourceModel,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SQLServerDataSourceConnection,
)

logger: logging.Logger = soda_logger


class SQLServerDataSourceImpl(DataSourceImpl, model_class=SQLServerDataSourceModel):
    def __init__(self, data_source_model: SQLServerDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SQLServerSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SQLServerDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SQLServerSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "["  # Do not use this! Always use quote_default()

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return f"[{identifier}]" if isinstance(identifier, str) and len(identifier) > 0 else None

    def create_schema_if_not_exists_sql(self, schema_name: str) -> str:
        return f"""
        IF NOT EXISTS ( SELECT  *
                        FROM    sys.schemas
                        WHERE   name = N'{schema_name}' )
        EXEC('CREATE SCHEMA [{schema_name}]');
        """

    def build_drop_table_sql(self, drop_table: DROP_TABLE | DROP_TABLE_IF_EXISTS, add_semicolon: bool = True) -> str:
        if_exists_sql: str = (
            f"IF OBJECT_ID('{drop_table.fully_qualified_table_name}', 'U') IS NOT NULL"
            if isinstance(drop_table, DROP_TABLE_IF_EXISTS)
            else ""
        )
        return f"{if_exists_sql} DROP TABLE {drop_table.fully_qualified_table_name}" + (";" if add_semicolon else "")

    def _build_create_table_statement_sql(self, create_table: CREATE_TABLE | CREATE_TABLE_IF_NOT_EXISTS) -> str:
        if_not_exists_sql: str = (
            f"IF OBJECT_ID('{create_table.fully_qualified_table_name}', 'U') IS NULL"
            if isinstance(create_table, CREATE_TABLE_IF_NOT_EXISTS)
            else ""
        )
        create_table_sql: str = f"{if_not_exists_sql} CREATE TABLE {create_table.fully_qualified_table_name} "
        return create_table_sql

    def _build_length_sql(self, length: LENGTH) -> str:
        return f"LEN({self.build_expression_sql(length.expression)})"

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"'{datetime_in_iso8601}'"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"DATETRUNC(DAY, {timestamp_literal})"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"DATEADD(DAY, 1, {timestamp_literal})"

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return f"CHECKSUM{super()._build_tuple_sql(tuple)}"
        return super()._build_tuple_sql(tuple)

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"PATINDEX ('{matches.regex_pattern}', {expression}) > 0"

    def supports_regex_advanced(self) -> bool:
        return False

    def get_sql_type_dict(self) -> dict[str, str]:
        base_dict = super().get_sql_type_dict()
        base_dict[DBDataType.TEXT] = "varchar(255)"
        return base_dict

    def get_contract_type_dict(self) -> dict[str, str]:
        return {
            DBDataType.TEXT: "varchar",
            DBDataType.INTEGER: "int",
            DBDataType.DECIMAL: "float",
            DBDataType.DATE: "date",
            DBDataType.TIME: "time",
            DBDataType.TIMESTAMP: "datetime",
            DBDataType.TIMESTAMP_TZ: "datetimeoffset",
            DBDataType.BOOLEAN: "bit",
        }

    def literal_date(self, date: date):
        """Technically dates can be passed directly as strings, but this is more explicit."""
        date_string = date.strftime("%Y-%m-%d")
        return f"CAST('{date_string}' AS DATE)"
