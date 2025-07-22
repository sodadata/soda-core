import logging
from datetime import datetime

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_oracle.common.data_sources.oracle_data_source_connection import (
    OracleDataSource as OracleDataSourceModel,
)
from soda_oracle.common.data_sources.oracle_data_source_connection import (
    OracleDataSourceConnection,
)
from soda_oracle.common.statements.oracle_metadata_columns_query import (
    OracleMetadataColumnsQuery,
)
from soda_oracle.common.statements.oracle_metadata_tables_query import (
    OracleMetadataTablesQuery,
)

logger: logging.Logger = soda_logger


class OracleDataSourceImpl(DataSourceImpl, model_class=OracleDataSourceModel):
    def __init__(self, data_source_model: OracleDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return OracleSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return OracleDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        """Oracle-specific metadata tables query using ALL_TABLES view"""
        return OracleMetadataTablesQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            prefixes=[],  # Oracle doesn't use database prefixes like other systems
        )

    def create_metadata_columns_query(self) -> MetadataColumnsQuery:
        """Oracle-specific metadata columns query using ALL_TAB_COLUMNS view"""
        return OracleMetadataColumnsQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            prefixes=[],  # Oracle doesn't use database prefixes like other systems
        )


class OracleSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = '"'

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def create_schema_if_not_exists_sql(self, schema_name: str) -> str:
        casify = str.upper
        # Code lifted from soda-library
        return f"""
        declare
            userexist integer;
        begin
            select count(*) into userexist from dba_users where username='{casify(schema_name)}';
            if (userexist = 0) then
                execute immediate 'create user {casify(schema_name)}';
                execute immediate 'ALTER USER {casify(schema_name)} QUOTA UNLIMITED ON SYSTEM';
                execute immediate 'ALTER SESSION SET TIME_ZONE = ''+00:00''';

            end if;
        end;
        """

    def schema_information_schema(self):
        # Oracle just has top-level metadata views, no dedicated metadata schema
        return None

    def table_tables(self) -> str:
        return "ALL_TABLES"

    def table_columns(self) -> str:
        return "ALL_TAB_COLUMNS"

    def column_table_schema(self) -> str:
        return "OWNER"

    def column_table_name(self) -> str:
        return "TABLE_NAME"

    def column_table_catalog(self) -> str:
        """Oracle does not have database catalog concept, return None to skip filtering"""
        return None

    def column_column_name(self) -> str:
        return "COLUMN_NAME"

    def column_data_type(self) -> str:
        return "DATA_TYPE"

    def column_data_type_max_length(self) -> str:
        return "DATA_LENGTH"

    def literal_date(self, date_value) -> str:
        """Oracle-specific date literal format"""
        date_string = date_value.strftime("%Y-%m-%d")
        return f"DATE'{date_string}'".strip()

    def literal_datetime(self, datetime_value) -> str:
        """Oracle-specific timestamp literal format"""
        if datetime.tzinfo:
            datetime_str = datetime.strftime("%Y-%m-%d %H:%M:%S %z")
            datetime_str_formatted = datetime_str[:-2] + ":" + datetime_str[-2:]
        else:
            datetime_str_formatted = datetime.strftime("%Y-%m-%d %H:%M:%S")

        return f"TIMESTAMP '{datetime_str_formatted}'".strip()

    # todo check this
    # def build_select_sql(self, select_elements: list, add_semicolon: bool = False) -> str:
    #     """Oracle-specific SELECT SQL generation - Oracle doesn't like semicolons in some contexts"""
    #     return super().build_select_sql(select_elements, add_semicolon)

    # TODO check this
    # def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
    #     """Oracle-specific timestamp literal using TO_TIMESTAMP"""
    #     # Convert ISO8601 timestamp to Oracle format
    #     # Example: '2025-04-16T12:00:00+00:00' -> TO_TIMESTAMP('2025-04-16 12:00:00', 'YYYY-MM-DD HH24:MI:SS')

    #     # Handle special case for ${soda.NOW} placeholder - in tests this should get substituted
    #     # with actual timestamp values, so this is mainly for non-test usage
    #     if datetime_in_iso8601 == "${soda.NOW}":
    #         return "CAST(CURRENT_TIMESTAMP AS TIMESTAMP)"

    #     # Clean the datetime string for Oracle
    #     datetime_clean = datetime_in_iso8601
    #     if 'T' in datetime_clean:
    #         datetime_clean = datetime_clean.replace('T', ' ')
    #     if '+00:00' in datetime_clean:
    #         datetime_clean = datetime_clean.replace('+00:00', '')
    #     elif '+' in datetime_clean:
    #         # Remove any timezone offset like +01:00, -05:00, etc.
    #         datetime_clean = datetime_clean.split('+')[0].split('-')[0]

    #     return f"TO_TIMESTAMP('{datetime_clean}', 'YYYY-MM-DD HH24:MI:SS')"

    # TODO check this
    # def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
    #     """Oracle-specific date truncation using TRUNC"""
    #     return f"TRUNC({timestamp_literal})"

    # TODO check this
    # def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
    #     """Oracle-specific date addition using + 1"""
    #     return f"{timestamp_literal} + 1"

    # def qualify_dataset_name(self, dataset_prefix: list[str], dataset_name: str) -> str:
    #     """
    #     Oracle-specific table name qualification - Oracle only supports schema.table, not database.schema.table
    #     """
    #     if dataset_prefix:
    #         # Oracle uses only the last prefix (schema name), ignoring database name
    #         schema_name = dataset_prefix[-1] if dataset_prefix else None
    #         if schema_name:
    #             # Use Oracle's case handling for schema names
    #             schema_name_caseified = self.default_casify(schema_name)
    #             dataset_name_caseified = self.default_casify(dataset_name)
    #             return f"{self.quote_default(schema_name_caseified)}.{self.quote_default(dataset_name_caseified)}"
    #     return self.quote_default(self.default_casify(dataset_name))

    # def build_fully_qualified_sql_name(self, dataset_identifier) -> str:
    #     """
    #     Oracle-specific fully qualified table name - handle Oracle's two-part naming
    #     """
    #     return self.qualify_dataset_name(
    #         dataset_prefix=dataset_identifier.prefixes,
    #         dataset_name=dataset_identifier.dataset_name
    #     )

    # def _build_qualified_quoted_dataset_name(self, dataset_name: str, dataset_prefix: list[str] = None) -> str:
    #     """
    #     Oracle-specific qualified table name generation - use Oracle's two-part naming
    #     """
    #     return self.qualify_dataset_name(dataset_prefix, dataset_name)

    # def _build_from_part(self, from_part) -> str:
    #     """Oracle-specific FROM clause - Oracle doesn't always like AS with table aliases"""
    #     from soda_core.common.sql_ast import FROM

    #     from_parts: list[str] = [
    #         self._build_qualified_quoted_dataset_name(
    #             dataset_name=from_part.table_name, dataset_prefix=from_part.table_prefix
    #         )
    #     ]

    #     if isinstance(from_part.alias, str):
    #         # Oracle prefers table aliases without AS keyword in some contexts
    #         from_parts.append(f"{self.quote_default(from_part.alias)}")

    #     return " ".join(from_parts)

    # def _build_left_inner_join_part(self, left_inner_join) -> str:
    #     """Oracle-specific LEFT JOIN - Oracle doesn't like AS with table aliases in joins"""
    #     from soda_core.common.sql_ast import LEFT_INNER_JOIN

    #     from_parts: list[str] = []

    #     if isinstance(left_inner_join, LEFT_INNER_JOIN):
    #         from_parts.append("LEFT JOIN")

    #     from_parts.append(
    #         self._build_qualified_quoted_dataset_name(
    #             dataset_name=left_inner_join.table_name, dataset_prefix=left_inner_join.table_prefix
    #         )
    #     )

    #     if isinstance(left_inner_join.alias, str):
    #         # Oracle prefers table aliases without AS keyword
    #         from_parts.append(f"{self.quote_default(left_inner_join.alias)}")

    #     if isinstance(left_inner_join, LEFT_INNER_JOIN):
    #         from_parts.append(f"ON {self.build_expression_sql(left_inner_join.on_condition)}")

    #     return " ".join(from_parts)

    # def _build_cte_sql_lines(self, select_elements: list) -> list[str]:
    #     """Oracle-specific CTE handling with consistent case sensitivity"""
    #     from soda_core.common.sql_ast import WITH
    #     from textwrap import dedent, indent

    #     cte_lines: list[str] = []
    #     for select_element in select_elements:
    #         if isinstance(select_element, WITH):
    #             cte_query_sql_str: str | None = None
    #             if isinstance(select_element.cte_query, list):
    #                 cte_query_sql_str = self.build_select_sql(select_element.cte_query)
    #                 cte_query_sql_str = cte_query_sql_str.strip()
    #             elif isinstance(select_element.cte_query, str):
    #                 cte_query_sql_str = dedent(select_element.cte_query).strip()
    #             if cte_query_sql_str:
    #                 cte_query_sql_str = cte_query_sql_str.rstrip(";")
    #                 indented_nested_query: str = indent(cte_query_sql_str, "  ")
    #                 # Use Oracle's case handling consistently for CTE aliases
    #                 cte_alias_quoted = self.quote_default(self.default_casify(select_element.alias))
    #                 cte_lines.append(f"WITH {cte_alias_quoted} AS (")
    #                 cte_lines.extend(indented_nested_query.split("\n"))
    #                 cte_lines.append(f")")
    #     return cte_lines

    # def _build_distinct_sql(self, distinct) -> str:
    #     """Oracle-specific DISTINCT handling for multiple columns"""
    #     from soda_core.common.sql_ast import DISTINCT

    #     expressions = distinct.expression if isinstance(distinct.expression, list) else [distinct.expression]

    #     if len(expressions) > 1:
    #         # Oracle doesn't support DISTINCT(col1, col2), use DISTINCT col1, col2
    #         field_expression_str = ", ".join([self.build_expression_sql(e) for e in expressions])
    #         return f"DISTINCT {field_expression_str}"
    #     else:
    #         # Single column DISTINCT works normally
    #         field_expression_str = self.build_expression_sql(expressions[0])
    #         return f"DISTINCT({field_expression_str})"

    # def _build_count_sql(self, count) -> str:
    #     """Oracle-specific COUNT handling for DISTINCT with multiple columns"""
    #     from soda_core.common.sql_ast import COUNT, DISTINCT, TUPLE

    #     if isinstance(count.expression, DISTINCT):
    #         # Handle COUNT(DISTINCT(...))
    #         distinct_expr = count.expression
    #         if isinstance(distinct_expr.expression, TUPLE):
    #             # Oracle doesn't support COUNT(DISTINCT col1, col2, col3)
    #             # Use COUNT(DISTINCT concatenated_columns) approach
    #             expressions = distinct_expr.expression.expressions
    #             # Concatenate columns with a separator to ensure uniqueness
    #             concat_expr = " || '|' || ".join([self.build_expression_sql(e) for e in expressions])
    #             return f"COUNT(DISTINCT({concat_expr}))"
    #         else:
    #             # Single column DISTINCT - use standard format
    #             return f"COUNT(DISTINCT({self.build_expression_sql(distinct_expr.expression)}))"
    #     else:
    #         # Regular COUNT
    #         return f"COUNT({self.build_expression_sql(count.expression)})"
