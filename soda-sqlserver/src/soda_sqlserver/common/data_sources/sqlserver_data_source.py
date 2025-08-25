import logging
from datetime import date
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_ast import (
    COLUMN,
    COUNT,
    CREATE_TABLE,
    CREATE_TABLE_IF_NOT_EXISTS,
    DISTINCT,
    DROP_TABLE,
    DROP_TABLE_IF_EXISTS,
    LENGTH,
    REGEX_LIKE,
    TUPLE,
    VALUES, SELECT, FROM, STAR, SqlExpressionStr, WHERE, AND, ORDER_BY_ASC, OFFSET, LIMIT,
)
from soda_core.common.sql_dialect import DBDataType, SqlDialect
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSource as SqlServerDataSourceModel,
)
from soda_sqlserver.common.data_sources.sqlserver_data_source_connection import (
    SqlServerDataSourceConnection,
)

logger: logging.Logger = soda_logger


class SqlServerDataSourceImpl(DataSourceImpl, model_class=SqlServerDataSourceModel):
    def __init__(self, data_source_model: SqlServerDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SqlServerSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SqlServerDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SqlServerSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "["  # Do not use this! Always use quote_default()

    def build_select_sql(self, select_elements: list, add_semicolon: bool = True) -> str:
        statement_lines: list[str] = []
        statement_lines.extend(self._build_cte_sql_lines(select_elements))
        statement_lines.extend(self._build_select_sql_lines(select_elements))
        statement_lines.extend(self._build_from_sql_lines(select_elements))
        statement_lines.extend(self._build_where_sql_lines(select_elements))
        statement_lines.extend(self._build_group_by_sql_lines(select_elements))
        statement_lines.extend(self._build_order_by_lines(select_elements))

        offset_line = self._build_offset_line(select_elements)
        if offset_line:
            statement_lines.append(offset_line)

        limit_line = self._build_limit_line(select_elements)
        if limit_line:
            statement_lines.append(limit_line)
        return "\n".join(statement_lines) + (";" if add_semicolon else "")

    def literal_date(self, date: date):
        """Technically dates can be passed directly as strings, but this is more explicit."""
        date_string = date.strftime("%Y-%m-%d")
        return f"CAST('{date_string}' AS DATE)"

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return f"[{identifier}]" if isinstance(identifier, str) and len(identifier) > 0 else None

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        schema_name: str = prefixes[1]
        return f"""
        IF NOT EXISTS ( SELECT  *
                        FROM    sys.schemas
                        WHERE   name = N'{schema_name}' )
        EXEC('CREATE SCHEMA [{schema_name}]')
        """ + (
            ";" if add_semicolon else ""
        )

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
        if tuple.check_context(VALUES):
            # in built_cte_values_sql, elements are dropped in top-level select statement, so can't use parentheses
            return ", ".join(self.build_expression_sql(e) for e in tuple.expressions)
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

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return "\nUNION ALL\n".join(["SELECT " + self.build_expression_sql(value) for value in values.values])

    def select_all_paginated_sql(
        self,
        dataset_identifier: DatasetIdentifier,
        columns: list[str],
        filter: Optional[str],
        order_by: list[str],
        limit: int,
        offset: int,
    ) -> str:
        where_clauses = []

        if filter:
            where_clauses.append(SqlExpressionStr(filter))

        statements = [
            SELECT(columns or [STAR()]),
            FROM(table_name=dataset_identifier.dataset_name, table_prefix=dataset_identifier.prefixes),
            WHERE.optional(AND.optional(where_clauses)),
            *[ORDER_BY_ASC(c) for c in order_by],
            OFFSET(offset),
            LIMIT(limit),
        ]

        return self.build_select_sql(statements)

    def _build_limit_sql(self, limit_element: LIMIT) -> str:
        return f"FETCH NEXT {limit_element.limit} ROWS ONLY"

    def _build_offset_sql(self, offset_element: OFFSET) -> str:
        return f"OFFSET {offset_element.offset} ROWS"
