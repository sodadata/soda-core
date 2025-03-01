from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import *
from soda_core.common.sql_dialect import SqlDialect


@dataclass
class FullyQualifiedTableName:
    database_name: str
    schema_name: str
    table_name: str


class MetadataTablesQuery:

    def __init__(
        self,
        sql_dialect: SqlDialect,
        data_source_connection: DataSourceConnection
    ):
        self.sql_dialect = sql_dialect
        self.data_source_connection: DataSourceConnection = data_source_connection

    def execute(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list[FullyQualifiedTableName]:
        select_statement: list = self.build_sql_statement(
            database_name=database_name,
            schema_name=schema_name,
            include_table_name_like_filters=include_table_name_like_filters,
            exclude_table_name_like_filters=exclude_table_name_like_filters
        )
        sql: str = self.sql_dialect.build_select_sql(select_statement)
        query_result: QueryResult = self.data_source_connection.execute_query(sql)
        return self.get_results(query_result)

    def build_sql_statement(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list:
        """
        Builds the full SQL query statement to query table names from the data source metadata.
        """
        select: list = [
            FROM(self._table_tables(), table_prefix=[database_name, self._schema_information_schema()]),
            SELECT([self._column_table_catalog(), self._column_table_schema(), self._column_table_name()]),
        ]

        if database_name:
            database_column_name: Optional[str] = self._column_table_catalog()
            if database_column_name:
                database_name_lower: str = database_name.lower()
                select.append(WHERE(EQ(LOWER(database_column_name), LITERAL(database_name_lower))))

        if schema_name:
            schema_column_name: Optional[str] = self._column_table_schema()
            if schema_column_name:
                schema_name_lower: str = schema_name.lower()
                select.append(WHERE(EQ(LOWER(schema_column_name), LITERAL(schema_name_lower))))

        table_name_column = self._column_table_name()

        if include_table_name_like_filters:
            select.append(WHERE(OR([
                LIKE(LOWER(table_name_column), LITERAL(include_table_name_like_filter.lower()))
                for include_table_name_like_filter in include_table_name_like_filters
            ])))

        if exclude_table_name_like_filters:
            for exclude_table_name_like_filter in exclude_table_name_like_filters:
                select.append(WHERE(NOT_LIKE(LOWER(table_name_column), LITERAL(exclude_table_name_like_filter.lower()))))

        return select

    def get_results(self, query_result: QueryResult) -> list[FullyQualifiedTableName]:
        return [
            FullyQualifiedTableName(
                database_name=database_name,
                schema_name=schema_name,
                table_name=table_name,
            )
            for database_name, schema_name, table_name in query_result.rows
        ]

    def _schema_information_schema(self) -> str:
        """
        Name of the schema that has the metadata
        """
        return "information_schema"

    def _table_tables(self) -> str:
        """
        Name of the table that has the table information in the metadata
        """
        return "tables"

    def _column_table_catalog(self) -> str:
        """
        Name of the column that has the database information in the tables metadata table
        """
        return "table_catalog"

    def _column_table_schema(self) -> str:
        """
        Name of the column that has the schema information in the tables metadata table
        """
        return "table_schema"

    def _column_table_name(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table
        """
        return "table_name"
