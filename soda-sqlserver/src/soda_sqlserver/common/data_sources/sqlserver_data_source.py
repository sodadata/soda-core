import logging
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import COUNT, DISTINCT, LENGTH, REGEX_LIKE, TUPLE
from soda_core.common.sql_dialect import SqlDialect
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
