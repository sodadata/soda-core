from __future__ import annotations

import re
from datetime import date, datetime
from numbers import Number

from soda_core.common.data_source import DataSource
from soda_core.common.statements.create_schema import CreateSchema
from soda_core.common.statements.create_table import CreateTable
from soda_core.common.statements.drop_schema import DropSchema
from soda_core.common.statements.drop_table import DropTable
from soda_core.common.statements.insert_into import InsertInto
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery


class SqlDialect:
    """
    Extends DataSource with all logic to builds the SQL queries.
    Specific DataSource's can customize their SQL queries by subclassing SqlDialect,
    overriding methods of SqlDialect and returning the customized SqlDialect in DataSource._create_sql_dialect()
    """

    def __init__(self):
        self.default_quote_char = self._get_default_quote_char()

    def create_create_schema(self, data_source: DataSource) -> CreateSchema:
        return CreateSchema(data_source)

    def create_drop_schema(self, data_source: DataSource) -> DropSchema:
        return DropSchema(data_source)

    def create_create_table(self, data_source: DataSource) -> CreateTable:
        return CreateTable(data_source)

    def create_drop_table(self, data_source: DataSource) -> DropTable:
        return DropTable(data_source)

    def create_insert_into(self, data_source: DataSource) -> InsertInto:
        return InsertInto(data_source)

    def create_metadata_tables_query(self, data_source: DataSource) -> MetadataTablesQuery:
        return MetadataTablesQuery(data_source)


    def _get_default_quote_char(self) -> str:
        return '"'

    def quote_default(self, identifier: str | None) -> str | None:
        return (
            f"{self.default_quote_char}{identifier}{self.default_quote_char}"
            if isinstance(identifier, str) and len(identifier) > 0
            else None
        )

    def default_casify(self, identifier: str) -> str:
        return identifier.lower()

    def qualify_table(
        self, database_name: str | None, schema_name: str | None, table_name: str
    ) -> str:
        """
        Creates a fully qualified table name, optionally quoting the table name
        """
        parts = [self.quote_default(database_name), self.quote_default(schema_name), self.quote_default(table_name)]
        return ".".join([p for p in parts if p])

    def literal(self, o: object) -> str:
        if o is None:
            return "NULL"
        elif isinstance(o, Number):
            return self.literal_number(o)
        elif isinstance(o, str):
            return self.literal_string(o)
        elif isinstance(o, datetime):
            return self.literal_datetime(o)
        elif isinstance(o, date):
            return self.literal_date(o)
        elif isinstance(o, list) or isinstance(o, set) or isinstance(o, tuple):
            return self.literal_list(o)
        elif isinstance(o, bool):
            return self.literal_boolean(o)
        raise RuntimeError(f"Cannot convert type {type(o)} to a SQL literal: {o}")

    def literal_number(self, value: Number):
        if value is None:
            return None
        return str(value)

    def literal_string(self, value: str):
        if value is None:
            return None
        return "'" + self.escape_string(value) + "'"

    def literal_list(self, l: list):
        if l is None:
            return None
        return "(" + (",".join([self.literal(e) for e in l])) + ")"

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"DATE '{date_string}'"

    def literal_datetime(self, datetime: datetime):
        return f"'{datetime.isoformat()}'"

    def literal_boolean(self, boolean: bool):
        return "TRUE" if boolean is True else "FALSE"

    def escape_string(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def escape_regex(self, value: str):
        return value


    # def stmt_drop_schema_if_exists(self, database_name: str, schema_name: str) -> str:
    #     schema_name_quoted: str = self.quote_default(schema_name)
    #     return f"DROP SCHEMA IF EXISTS {schema_name_quoted} CASCADE"
    #
    # def stmt_create_schema_if_not_exists(self, database_name: str, schema_name: str) -> str:
    #     schema_name_quoted: str = self.quote_default(schema_name)
    #     return f"CREATE SCHEMA IF NOT EXISTS {schema_name_quoted} AUTHORIZATION CURRENT_USER"
    #
    # def stmt_select_table_names(
    #     self,
    #     database_name: str | None = None,
    #     schema_name: str | None = None,
    #     table_name_like_filter: str | None = None,
    #     include_table_name_like_filters: list[str] = None,
    #     exclude_table_name_like_filters: list[str] = None,
    # ) -> str:
    #     """
    #     Builds the full SQL query to query table names from the data source metadata.
    #     """
    #     table_name_column: str = self.sql_information_schema_tables_table_column()
    #     # table_name_column_quoted: str = self.quote_default(table_name_column)
    #     information_schema_table_name_qualified: str = self.information_schema_table_name_qualified()
    #
    #     sql = f"SELECT {table_name_column} \n" f"FROM {information_schema_table_name_qualified}"
    #
    #     where_clauses: list[str] = self._filter_clauses_information_schema_table_name(
    #         database_name,
    #         schema_name,
    #         table_name_like_filter,
    #         include_table_name_like_filters,
    #         exclude_table_name_like_filters,
    #     )
    #
    #     if where_clauses:
    #         where_clauses_sql = "\n  AND ".join(where_clauses)
    #         sql += f"\nWHERE {where_clauses_sql}"
    #
    #     return sql
    #
    # def information_schema_table_name_qualified(self) -> str:
    #     information_schema_database_name: str = self.information_schema_database_name()
    #     information_schema_schema_name: str = self.information_schema_schema_name()
    #     information_schema_table_name: str = self.information_schema_table_name()
    #     return self.qualify_table(
    #         database_name=None, schema_name=information_schema_schema_name, table_name=information_schema_table_name
    #     )
    #
    # def information_schema_database_name(self) -> str | None:
    #     return None
    #
    # def information_schema_schema_name(self) -> str | None:
    #     return "information_schema"
    #
    # def information_schema_table_name(self) -> str:
    #     return "tables"
    #
    # def sql_information_schema_tables_database_column(self) -> str:
    #     """
    #     Name of the column in the information_schema table that contains the database name
    #     """
    #     return "table_catalog"
    #
    # def sql_information_schema_tables_schema_column(self) -> str:
    #     """
    #     Name of the column in the information_schema table that contains the schema name
    #     """
    #     return "table_schema"
    #
    # def sql_information_schema_tables_table_column(self) -> str:
    #     """
    #     Name of the column in the information_schema table that contains the table name
    #     """
    #     return "table_name"
    #
    # def _filter_clauses_information_schema_table_name(
    #     self,
    #     database_name: str | None = None,
    #     schema_name: str | None = None,
    #     table_name_like_filter: str | None = None,
    #     include_table_name_like_filters: list[str] = None,
    #     exclude_table_name_like_filters: list[str] = None,
    # ) -> list[str]:
    #     """
    #     Builds the list of where clauses to query table names from the data source metadata.
    #     All comparisons are case-insensitive by converting to lower case.
    #     All column names are quoted.
    #     """
    #
    #     where_clauses = []
    #
    #     if database_name:
    #         database_column_name: str | None = self.sql_information_schema_tables_database_column()
    #         if database_column_name:
    #             # database_column_name: str = self.quote_default(database_column_name)
    #             database_name_lower: str = database_name.lower()
    #             where_clauses.append(f"LOWER({database_column_name}) = '{database_name_lower}'")
    #
    #     if schema_name:
    #         schema_column_name: str | None = self.sql_information_schema_tables_schema_column()
    #         if schema_column_name:
    #             # schema_column_name: str = self.quote_default(schema_column_name)
    #             schema_name_lower: str = schema_name.lower()
    #             where_clauses.append(f"LOWER({schema_column_name}) = '{schema_name_lower}'")
    #
    #     table_name_column = self.sql_information_schema_tables_table_column()
    #     # table_name_column = self.quote_default(table_name_column)
    #
    #     if table_name_like_filter:
    #         where_clauses.append(f"LOWER({table_name_column}) LIKE '{table_name_like_filter.lower()}'")
    #
    #     def build_table_matching_conditions(table_expressions: list[str], comparison_operator: str):
    #         conditions = []
    #         if table_expressions:
    #             for table_expression in table_expressions:
    #                 table_expression_lower: str = table_expression.lower()
    #                 conditions.append(f"LOWER({table_name_column}) {comparison_operator} '{table_expression_lower}'")
    #         return conditions
    #
    #     if include_table_name_like_filters:
    #         sql_include_clauses = " OR ".join(build_table_matching_conditions(include_table_name_like_filters, "LIKE"))
    #         where_clauses.append(f"({sql_include_clauses})")
    #
    #     if exclude_table_name_like_filters:
    #         sql_exclude_clauses = build_table_matching_conditions(exclude_table_name_like_filters, "NOT LIKE")
    #         where_clauses.extend(sql_exclude_clauses)
    #
    #     return where_clauses
