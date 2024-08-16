from __future__ import annotations

import logging
import re
from datetime import datetime, date
from numbers import Number

from helpers.test_column import TestColumn
from helpers.test_table import TestTable
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class SqlDialect:
    """
    Extends ContractDataSource with all logic to builds the SQL queries.
    Specific ContractDataSource's can customize their SQL queries by subclassing SqlDialect,
    overriding methods of SqlDialect and returning the customized SqlDialect in ContractDataSource._create_sql_dialect()
    """

    def __init__(self):
        self.default_quote_char = self.get_default_quote_char()
        self.create_table_sql_type_dict: dict[str, str] = self.get_create_table_sql_type_dict()

    def get_default_quote_char(self) -> str:
        return '"'

    def get_create_table_sql_type_dict(self) -> dict[str, str]:
        return {
            DataType.TEXT: "VARCHAR(255)",
            DataType.INTEGER: "INT",
            DataType.DECIMAL: "FLOAT",
            DataType.DATE: "DATE",
            DataType.TIME: "TIME",
            DataType.TIMESTAMP: "TIMESTAMP",
            DataType.TIMESTAMP_TZ: "TIMESTAMPTZ",
            DataType.BOOLEAN: "BOOLEAN",
        }

    def stmt_drop_schema_if_exists(self, database_name: str, schema_name: str) -> str:
        schema_name_quoted: str = self.quote_default(schema_name)
        return f"DROP SCHEMA IF EXISTS {schema_name_quoted} CASCADE"

    def stmt_create_schema_if_exists(self, schema_name) -> str:
        schema_name_quoted: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name_quoted} AUTHORIZATION CURRENT_USER"

    def stmt_select_table_names(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        table_name_like_filter: str | None = None,
        include_table_name_like_filters: list[str] = None,
        exclude_table_name_like_filters: list[str] = None
    ) -> str:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """
        table_name_column: str = self.sql_information_schema_tables_table_column()
        table_name_column_quoted: str = self.quote_default(table_name_column)
        information_schema_table_name_qualified_quoted: str = self.information_schema_table_name_qualified_quoted()

        sql = (
            f"SELECT {table_name_column_quoted} \n"
            f"FROM {information_schema_table_name_qualified_quoted}"
        )

        where_clauses: list[str] = self._filter_clauses_information_schema_table_name(
            database_name, schema_name, table_name_like_filter, include_table_name_like_filters, exclude_table_name_like_filters
        )

        if where_clauses:
            where_clauses_sql = "\n  AND ".join(where_clauses)
            sql += f"\nWHERE {where_clauses_sql}"

        return sql

    def information_schema_table_name_qualified_quoted(self) -> str:
        information_schema_schema_name: str = self.information_schema_schema_name()
        information_schema_table_name: str = self.information_schema_table_name()
        return self.quote_table(
            database_name=None,
            schema_name=information_schema_schema_name,
            table_name=information_schema_table_name
        )

    def information_schema_schema_name(self) -> str:
        return "information_schema"

    def information_schema_table_name(self) -> str:
        return "tables"

    def sql_information_schema_tables_database_column(self) -> str:
        """
        Name of the column in the information_schema table that contains the database name
        """
        return "table_catalog"

    def sql_information_schema_tables_schema_column(self) -> str:
        """
        Name of the column in the information_schema table that contains the schema name
        """
        return "table_schema"

    def sql_information_schema_tables_table_column(self) -> str:
        """
        Name of the column in the information_schema table that contains the table name
        """
        return "table_name"

    def _filter_clauses_information_schema_table_name(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        table_name_like_filter: str | None = None,
        include_table_name_like_filters: list[str] = None,
        exclude_table_name_like_filters: list[str] = None,
    ) -> list[str]:
        """
        Builds the list of where clauses to query table names from the data source metadata.
        All comparisons are case-insensitive by converting to lower case.
        All column names are quoted.
        """

        where_clauses = []

        if database_name:
            database_column_name: str | None = self.sql_information_schema_tables_database_column()
            if database_column_name:
                database_column_name_quoted: str = self.quote_default(database_column_name)
                database_name_lower: str = database_name.lower()
                where_clauses.append(f"LOWER({database_column_name_quoted}) = '{database_name_lower}'")

        if schema_name:
            schema_column_name: str | None = self.sql_information_schema_tables_schema_column()
            if schema_column_name:
                schema_column_name_quoted: str = self.quote_default(schema_column_name)
                schema_name_lower: str = schema_name.lower()
                where_clauses.append(f"LOWER({schema_column_name_quoted}) = '{schema_name_lower}'")

        table_name_column = self.sql_information_schema_tables_table_column()
        table_name_column_quoted = self.quote_default(table_name_column)

        if table_name_like_filter:
            where_clauses.append(f"LOWER({table_name_column_quoted}) LIKE '{table_name_like_filter.lower()}'")

        def build_table_matching_conditions(table_expressions: list[str], comparison_operator: str):
            conditions = []
            if table_expressions:
                for table_expression in table_expressions:
                    table_expression_lower: str = table_expression.lower()
                    conditions.append(f"LOWER({table_name_column_quoted}) {comparison_operator} '{table_expression_lower}'")
            return conditions

        if include_table_name_like_filters:
            sql_include_clauses = " OR ".join(build_table_matching_conditions(include_table_name_like_filters, "LIKE"))
            where_clauses.append(f"({sql_include_clauses})")

        if exclude_table_name_like_filters:
            sql_exclude_clauses = build_table_matching_conditions(exclude_table_name_like_filters, "NOT LIKE")
            where_clauses.extend(sql_exclude_clauses)

        return where_clauses

    def stmt_drop_test_table(self, database_name: str | None, schema_name: str | None, table_name: str) -> str:
        qualified_table_name = self.quote_table(
            database_name=database_name,
            schema_name=schema_name,
            table_name=table_name
        )
        return f"DROP TABLE IF EXISTS {qualified_table_name}"

    def stmt_create_test_table(
            self,
            database_name: str | None,
            schema_name: str | None,
            test_table: TestTable
    ) -> str:
        table_name_qualified_quoted = self.quote_table(
            database_name=database_name,
            schema_name=schema_name,
            table_name=test_table.unique_table_name
        )

        test_columns = test_table.test_columns
        if test_table.quote_names:
            test_columns = [
                TestColumn(
                    name=self.quote_default(test_column.name),
                    data_type=test_column.data_type
                )
                for test_column in test_columns
            ]

        def get_create_table_sql_type(data_type: str) -> str:
            return self.create_table_sql_type_dict.get(data_type, data_type)

        columns_sql = ",\n".join(
            [
                f"  {test_column.name} {get_create_table_sql_type(test_column.data_type)}"
                for test_column in test_columns
            ]
        )
        return self.compose_create_table_statement(table_name_qualified_quoted, columns_sql)

    def compose_create_table_statement(self, qualified_table_name, columns_sql) -> str:
        return f"CREATE TABLE {qualified_table_name} ( \n{columns_sql} \n)"

    def _insert_test_table_sql(self, database_name: str | None, schema_name: str | None, test_table: TestTable) -> str:
        if test_table.values:
            table_name_qualified_quoted = self.quote_table(
                database_name=database_name,
                schema_name=schema_name,
                table_name=test_table.unique_table_name
            )
            def sql_test_table_row(row):
                return ",".join([self.literal(value) for value in row])

            rows_sql = ",\n".join([f"  ({sql_test_table_row(row)})" for row in test_table.values])
            return f"INSERT INTO {table_name_qualified_quoted} VALUES \n" f"{rows_sql};"

    def quote_table(self, database_name: str | None, schema_name: str, table_name: str) -> str:
        """
        Creates a fully qualified, quoted table name to be used when referencing an existing table in a sql statement (read mode).
        """
        return f"{self.quote_default(schema_name)}.{self.quote_default(table_name)}"

    def quote_default(self, table_name: str) -> str:
        return f'{self.default_quote_char}{table_name}{self.default_quote_char}'

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
