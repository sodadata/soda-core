from __future__ import annotations

from dataclasses import dataclass

from soda.common.data_source_connection import DataSourceConnection, QueryResult
from soda.common.sql_dialect import SqlDialect


@dataclass
class FullyQualifiedTableName:
    database_name: str
    schema_name: str
    table_name: str


class TableNamesQuery:

    def __init__(self, data_source_connection: DataSourceConnection, sql_dialect: SqlDialect):
        self.data_source_connection: DataSourceConnection = data_source_connection
        self.sql_dialect: SqlDialect = sql_dialect
        self.database_name: str | None = None
        self.schema_name: str | None = None
        self.include_table_name_like_filters: list[str] | None = None
        self.exclude_table_name_like_filters: list[str] | None = None

    def with_database_name(self, database_name: str | None) -> TableNamesQuery:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> TableNamesQuery:
        self.schema_name = schema_name
        return self

    def with_include_table_name_like_filters(self, include_table_name_like_filters: list[str] | None) -> TableNamesQuery:
        self.include_table_name_like_filters = include_table_name_like_filters
        return self

    def with_exclude_table_name_like_filters(self, exclude_table_name_like_filters: list[str] | None) -> TableNamesQuery:
        self.exclude_table_name_like_filters = exclude_table_name_like_filters
        return self

    def execute(self) -> list[FullyQualifiedTableName]:
        sql: str = self._build_sql()
        query_result: QueryResult = self.data_source_connection.execute_query(sql)
        return [
            FullyQualifiedTableName(
                database_name=database_name,
                schema_name=schema_name,
                table_name=table_name,
            )
            for database_name, schema_name, table_name in query_result.rows
        ]

    def _build_sql(self) -> str:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """
        sql = (
            f"SELECT "
            f" {self._column_name_information_schema_tables_table_catalog()} as database_name,"
            f" {self._column_name_information_schema_tables_table_schema()} as schema_name,"
            f" {self._column_name_information_schema_tables_table_name()} as table_name \n" 
            f"FROM {self._information_schema_table_name_qualified()}"
        )

        where_clauses: list[str] = self._filter_clauses_information_schema_table_name()
        if where_clauses:
            where_clauses_sql = "\n  AND ".join(where_clauses)
            sql += f"\nWHERE {where_clauses_sql}"

        return f"{sql};"

    def _information_schema_table_name_qualified(self) -> str:
        return self.sql_dialect.qualify_table(
            database_name=self.database_name,
            schema_name=self._schema_name_information_schema(),
            table_name=self._table_name_information_schema_tables()
        )

    def _schema_name_information_schema(self) -> str:
        """
        Name of the schema that has the metadata
        """
        return "information_schema"

    def _table_name_information_schema_tables(self) -> str:
        """
        Name of the table that has the table information in the metadata
        """
        return "tables"

    def _column_name_information_schema_tables_table_catalog(self) -> str:
        """
        Name of the column that has the database information in the tables metadata table
        """
        return "table_catalog"

    def _column_name_information_schema_tables_table_schema(self) -> str:
        """
        Name of the column that has the schema information in the tables metadata table
        """
        return "table_schema"

    def _column_name_information_schema_tables_table_name(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table
        """
        return "table_name"

    def _filter_clauses_information_schema_table_name(self) -> list[str]:
        """
        Builds the list of where clauses to query table names from the data source metadata.
        All comparisons are case-insensitive by converting to lower case.
        All column names are quoted.
        """

        where_clauses = []

        if self.database_name:
            database_column_name: str | None = self._column_name_information_schema_tables_table_catalog()
            if database_column_name:
                # database_column_name: str = self.quote_default(database_column_name)
                database_name_lower: str = self.database_name.lower()
                where_clauses.append(f"LOWER({database_column_name}) = '{database_name_lower}'")

        if self.schema_name:
            schema_column_name: str | None = self._column_name_information_schema_tables_table_schema()
            if schema_column_name:
                # schema_column_name: str = self.quote_default(schema_column_name)
                schema_name_lower: str = self.schema_name.lower()
                where_clauses.append(f"LOWER({schema_column_name}) = '{schema_name_lower}'")

        table_name_column = self._column_name_information_schema_tables_table_name()
        # table_name_column = self.quote_default(table_name_column)

        def build_table_matching_conditions(table_expressions: list[str], comparison_operator: str):
            conditions = []
            if table_expressions:
                for table_expression in table_expressions:
                    table_expression_lower: str = table_expression.lower()
                    conditions.append(f"LOWER({table_name_column}) {comparison_operator} '{table_expression_lower}'")
            return conditions

        if self.include_table_name_like_filters:
            sql_include_clauses = " OR ".join(build_table_matching_conditions(
                self.include_table_name_like_filters, "LIKE")
            )
            where_clauses.append(f"({sql_include_clauses})")

        if self.exclude_table_name_like_filters:
            sql_exclude_clauses = build_table_matching_conditions(
                self.exclude_table_name_like_filters, "NOT LIKE"
            )
            where_clauses.extend(sql_exclude_clauses)

        return where_clauses
