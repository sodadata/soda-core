from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.sql_ast import (
    EQ,
    FROM,
    LIKE,
    LITERAL,
    LOWER,
    NOT_LIKE,
    OR,
    SELECT,
    WHERE,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery


class BigQueryMetadataTablesQuery(MetadataTablesQuery):
    def __init__(self, sql_dialect: SqlDialect, data_source_connection: DataSourceConnection, location: str):
        super().__init__(sql_dialect, data_source_connection)
        self.location = location

    def build_sql_statement(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list:
        """
        Builds the full SQL query statement to query table names from the data source metadata.

        BigQuery specific:
        - location is a prefix of the database name
        """
        select: list = [
            FROM(
                self.sql_dialect.table_tables(),
                table_prefix=[
                    f"region-{self.location}",
                    self.sql_dialect.schema_information_schema(),
                ],
            ),
            SELECT(
                [
                    self.sql_dialect.column_table_catalog(),
                    self.sql_dialect.column_table_schema(),
                    self.sql_dialect.column_table_name(),
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
