from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import *
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.table_types import (
    FullyQualifiedObjectName,
    FullyQualifiedTableName,
    FullyQualifiedViewName,
    TableType,
)


class MetadataTablesQuery:
    def __init__(
        self,
        sql_dialect: SqlDialect,
        data_source_connection: DataSourceConnection,
        prefixes: Optional[list[str]] = None,
    ):
        self.sql_dialect = sql_dialect
        self.data_source_connection: DataSourceConnection = data_source_connection
        self.prefixes = prefixes

    def execute(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
        types_to_return: list[TableType] = [
            TableType.TABLE,
        ],  # To make it backwards compatible with the old behavior TODO: refactor this so we support views everywhere?
    ) -> list[FullyQualifiedObjectName]:
        select_statement: list = self.build_sql_statement(
            database_name=database_name,
            schema_name=schema_name,
            include_table_name_like_filters=include_table_name_like_filters,
            exclude_table_name_like_filters=exclude_table_name_like_filters,
        )
        sql: str = self.sql_dialect.build_select_sql(select_statement)
        query_result: QueryResult = self.data_source_connection.execute_query(sql)
        return self.get_results(query_result, types_to_return)

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

        join_prefixes = lambda prefixes, schema: [*prefixes, schema] if schema else prefixes
        if self.prefixes is not None:
            prefixes = self.prefixes
        else:
            prefixes = [database_name] if database_name else []
        select: list = [
            FROM(
                self.sql_dialect.table_tables(),
                table_prefix=join_prefixes(prefixes, self.sql_dialect.schema_information_schema()),
            ),
            SELECT(
                [
                    self.sql_dialect.column_table_catalog() or COLUMN(LITERAL(None), field_alias="database_name"),
                    self.sql_dialect.column_table_schema(),
                    self.sql_dialect.column_table_name(),
                    self.sql_dialect.column_table_type() or COLUMN(LITERAL(None), field_alias="table_type"),
                ]
            ),
        ]

        if database_name:
            database_column_name: Optional[str] = self.sql_dialect.column_table_catalog()
            if database_column_name:
                database_name_lower: str = database_name.lower()
                select.append(WHERE(EQ(LOWER(database_column_name), LITERAL(database_name_lower))))

        if schema_name:
            schema_column_name: Optional[str] = self.sql_dialect.column_table_schema()
            if schema_column_name:
                schema_name_lower: str = schema_name.lower()
                select.append(WHERE(EQ(LOWER(schema_column_name), LITERAL(schema_name_lower))))

        table_name_column = self.sql_dialect.column_table_name()

        if include_table_name_like_filters:
            select.append(
                WHERE(
                    OR(
                        [
                            LIKE(LOWER(table_name_column), LITERAL(include_table_name_like_filter.lower()))
                            for include_table_name_like_filter in include_table_name_like_filters
                        ]
                    )
                )
            )

        if exclude_table_name_like_filters:
            for exclude_table_name_like_filter in exclude_table_name_like_filters:
                select.append(
                    WHERE(NOT_LIKE(LOWER(table_name_column), LITERAL(exclude_table_name_like_filter.lower())))
                )

        return select

    def get_results(
        self, query_result: QueryResult, types_to_return: list[TableType] = [TableType.TABLE]
    ) -> list[FullyQualifiedObjectName]:
        result: list[FullyQualifiedObjectName] = []
        for database_name, schema_name, table_name, table_type in query_result.rows:
            converted_table_type = self.sql_dialect.convert_table_type_to_enum(table_type)
            # If we don't have to return this table table, skip it
            if not converted_table_type in types_to_return:
                continue

            # Create the fully qualified name object
            if converted_table_type == TableType.TABLE:
                result.append(
                    FullyQualifiedTableName(database_name=database_name, schema_name=schema_name, table_name=table_name)
                )
            elif converted_table_type == TableType.VIEW:
                result.append(
                    FullyQualifiedViewName(database_name=database_name, schema_name=schema_name, view_name=table_name)
                )
            else:
                logger.warning(f"Unexpected table type: {table_type}")

        return result
