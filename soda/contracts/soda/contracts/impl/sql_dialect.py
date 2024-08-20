from __future__ import annotations

import logging
import re
from datetime import date, datetime
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
        self.default_quote_char = self._get_default_quote_char()
        self.create_table_sql_type_dict: dict[str, str] = self._get_create_table_sql_type_dict()
        self.schema_check_sql_type_dict: dict[str, str] = self._get_schema_check_sql_type_dict()

    def _get_default_quote_char(self) -> str:
        return '"'

    def _get_create_table_sql_type_dict(self) -> dict[str, str]:
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

    def _get_schema_check_sql_type_dict(self) -> dict[str, str]:
        return {
            DataType.TEXT: "character varying",
            DataType.INTEGER: "integer",
            DataType.DECIMAL: "double precision",
            DataType.DATE: "date",
            DataType.TIME: "time",
            DataType.TIMESTAMP: "timestamp without time zone",
            DataType.TIMESTAMP_TZ: "timestamp with time zone",
            DataType.BOOLEAN: "boolean",
        }

    def stmt_drop_schema_if_exists(self, database_name: str, schema_name: str) -> str:
        schema_name_quoted: str = self.quote_default(schema_name)
        return f"DROP SCHEMA IF EXISTS {schema_name_quoted} CASCADE"

    def stmt_create_schema_if_not_exists(self, database_name: str, schema_name: str) -> str:
        schema_name_quoted: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {schema_name_quoted} AUTHORIZATION CURRENT_USER"

    def stmt_select_table_names(
        self,
        database_name: str | None = None,
        schema_name: str | None = None,
        table_name_like_filter: str | None = None,
        include_table_name_like_filters: list[str] = None,
        exclude_table_name_like_filters: list[str] = None,
    ) -> str:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """
        table_name_column: str = self.sql_information_schema_tables_table_column()
        # table_name_column_quoted: str = self.quote_default(table_name_column)
        information_schema_table_name_qualified: str = self.information_schema_table_name_qualified()

        sql = f"SELECT {table_name_column} \n" f"FROM {information_schema_table_name_qualified}"

        where_clauses: list[str] = self._filter_clauses_information_schema_table_name(
            database_name,
            schema_name,
            table_name_like_filter,
            include_table_name_like_filters,
            exclude_table_name_like_filters,
        )

        if where_clauses:
            where_clauses_sql = "\n  AND ".join(where_clauses)
            sql += f"\nWHERE {where_clauses_sql}"

        return sql

    def information_schema_table_name_qualified(self) -> str:
        information_schema_database_name: str = self.information_schema_database_name()
        information_schema_schema_name: str = self.information_schema_schema_name()
        information_schema_table_name: str = self.information_schema_table_name()
        return self.qualify_table(
            database_name=None, schema_name=information_schema_schema_name, table_name=information_schema_table_name
        )

    def information_schema_database_name(self) -> str | None:
        return None

    def information_schema_schema_name(self) -> str | None:
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
                # database_column_name: str = self.quote_default(database_column_name)
                database_name_lower: str = database_name.lower()
                where_clauses.append(f"LOWER({database_column_name}) = '{database_name_lower}'")

        if schema_name:
            schema_column_name: str | None = self.sql_information_schema_tables_schema_column()
            if schema_column_name:
                # schema_column_name: str = self.quote_default(schema_column_name)
                schema_name_lower: str = schema_name.lower()
                where_clauses.append(f"LOWER({schema_column_name}) = '{schema_name_lower}'")

        table_name_column = self.sql_information_schema_tables_table_column()
        # table_name_column = self.quote_default(table_name_column)

        if table_name_like_filter:
            where_clauses.append(f"LOWER({table_name_column}) LIKE '{table_name_like_filter.lower()}'")

        def build_table_matching_conditions(table_expressions: list[str], comparison_operator: str):
            conditions = []
            if table_expressions:
                for table_expression in table_expressions:
                    table_expression_lower: str = table_expression.lower()
                    conditions.append(f"LOWER({table_name_column}) {comparison_operator} '{table_expression_lower}'")
            return conditions

        if include_table_name_like_filters:
            sql_include_clauses = " OR ".join(build_table_matching_conditions(include_table_name_like_filters, "LIKE"))
            where_clauses.append(f"({sql_include_clauses})")

        if exclude_table_name_like_filters:
            sql_exclude_clauses = build_table_matching_conditions(exclude_table_name_like_filters, "NOT LIKE")
            where_clauses.extend(sql_exclude_clauses)

        return where_clauses

    def stmt_drop_test_table(self, database_name: str | None, schema_name: str | None, table_name: str) -> str:
        qualified_table_name = self.qualify_table(
            database_name=database_name, schema_name=schema_name, table_name=table_name
        )
        return f"DROP TABLE IF EXISTS {qualified_table_name}"

    def stmt_create_test_table(self, database_name: str | None, schema_name: str | None, test_table: TestTable) -> str:

        table_name_qualified_quoted = self.qualify_table(
            database_name=database_name,
            schema_name=schema_name,
            table_name=test_table.unique_table_name,
            quote_table_name=test_table.quote_names,
        )

        test_columns = test_table.test_columns
        if test_table.quote_names:
            test_columns = [
                TestColumn(name=self.quote_default(test_column.name), data_type=test_column.data_type)
                for test_column in test_columns
            ]

        columns_sql = ",\n".join(
            [
                f"  {test_column.name} {self.get_create_table_sql_type(test_column.data_type)}"
                for test_column in test_columns
            ]
        )
        return self.compose_create_table_statement(table_name_qualified_quoted, columns_sql)

    def get_create_table_sql_type(self, data_type: str) -> str:
        return self.create_table_sql_type_dict.get(data_type)

    def get_schema_check_sql_type(self, data_type: str) -> str:
        return self.schema_check_sql_type_dict.get(data_type)

    def get_schema_check_sql_type_text(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.TEXT)

    def get_schema_check_sql_type_time(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.TIME)

    def get_schema_check_sql_type_date(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.DATE)

    def get_schema_check_sql_type_timestamp(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.TIMESTAMP)

    def get_schema_check_sql_type_timestamp_tz(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.TIMESTAMP_TZ)

    def get_schema_check_sql_type_integer(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.INTEGER)

    def get_schema_check_sql_type_decimal(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.DECIMAL)

    def get_schema_check_sql_type_boolean(self) -> str:
        return self.schema_check_sql_type_dict.get(DataType.BOOLEAN)

    def compose_create_table_statement(self, qualified_table_name, columns_sql) -> str:
        return f"CREATE TABLE {qualified_table_name} ( \n{columns_sql} \n)"

    def _insert_test_table_sql(self, database_name: str | None, schema_name: str | None, test_table: TestTable) -> str:
        if test_table.values:
            table_name_qualified_quoted = self.qualify_table(
                database_name=database_name,
                schema_name=schema_name,
                table_name=test_table.unique_table_name,
                quote_table_name=test_table.quote_names,
            )

            def sql_test_table_row(row):
                return ",".join([self.literal(value) for value in row])

            rows_sql = ",\n".join([f"  ({sql_test_table_row(row)})" for row in test_table.values])
            return f"INSERT INTO {table_name_qualified_quoted} VALUES \n" f"{rows_sql};"

    def qualify_table(
        self, database_name: str | None, schema_name: str | None, table_name: str, quote_table_name: bool = False
    ) -> str:
        """
        Creates a fully qualified table name, optionally quoting the table name
        """
        parts = [database_name, schema_name, self.quote_default(table_name) if quote_table_name else table_name]
        return ".".join([p for p in parts if p])

    def quote_default(self, identifier: str | None) -> str | None:
        return (
            f"{self.default_quote_char}{identifier}{self.default_quote_char}"
            if isinstance(identifier, str) and len(identifier) > 0
            else None
        )

    def default_casify(self, identifier: str) -> str:
        return identifier.lower()

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
