from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import (
    COLUMN,
    EQ,
    FROM,
    LIKE,
    LITERAL,
    LOWER,
    NOT_LIKE,
    OR,
    RAW_SQL,
    SELECT,
    UNION_ALL,
    WHERE,
)
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.statements.table_types import FullyQualifiedObjectName, TableType


class RedshiftMetadataTablesQuery(MetadataTablesQuery):
    def execute(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
        types_to_return: Optional[
            list[TableType]
        ] = None,  # To make sure it's backwards compatible with the old behavior, when we use None it should default to [TableType.TABLE]
    ) -> list[FullyQualifiedObjectName]:
        if types_to_return is None:
            types_to_return = [TableType.TABLE]
        select_statement: UNION_ALL = self.build_sql_statement(
            database_name=database_name,
            schema_name=schema_name,
            include_table_name_like_filters=include_table_name_like_filters,
            exclude_table_name_like_filters=exclude_table_name_like_filters,
        )
        sql: str = self.sql_dialect.build_union_sql(select_statement)
        query_result: QueryResult = self.data_source_connection.execute_query(sql)
        return self.get_results(query_result, types_to_return)

    def build_sql_statement(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> UNION_ALL:
        """
        Builds the full SQL query statement to query table names from the data source metadata.

        Redshift specific implementation that queries both regular tables and materialized views.
        svv_tables does include materialized views, but does not indicate them as such, only as 'VIEW' in the table_type column.
        Therefore, we need to query svv_mv_info separately to get the materialized views to be consistent with other datasources that support materialized views.
        """
        table_select = [
            SELECT(
                [
                    COLUMN("table_catalog"),
                    COLUMN("table_schema"),
                    COLUMN("table_name"),
                    COLUMN("table_type"),
                ]
            ),
            FROM("svv_tables"),
        ]
        matview_select = [
            SELECT(
                [
                    COLUMN("database_name", field_alias="table_catalog"),
                    COLUMN("schema_name", field_alias="table_schema"),
                    COLUMN("name", field_alias="table_name"),
                    RAW_SQL("'MATERIALIZED VIEW' AS TABLE_TYPE"),
                ]
            ),
            FROM("svv_mv_info"),
        ]

        statement = UNION_ALL([table_select, matview_select])

        if database_name:
            database_name_lower: str = database_name.lower()
            table_select.append(WHERE(EQ(LOWER("table_catalog"), LITERAL(database_name_lower))))
            matview_select.append(WHERE(EQ(LOWER("database_name"), LITERAL(database_name_lower))))

        if schema_name:
            table_select.append(WHERE(EQ(LOWER("table_schema"), LITERAL(schema_name.lower()))))
            matview_select.append(WHERE(EQ(LOWER("schema_name"), LITERAL(schema_name.lower()))))

        if include_table_name_like_filters:
            table_select.append(
                WHERE(
                    OR(
                        [
                            LIKE(LOWER(COLUMN("table_name")), LITERAL(include_table_name_like_filter.lower()))
                            for include_table_name_like_filter in include_table_name_like_filters
                        ]
                    )
                )
            )
            matview_select.append(
                WHERE(
                    OR(
                        [
                            LIKE(LOWER(COLUMN("name")), LITERAL(include_table_name_like_filter.lower()))
                            for include_table_name_like_filter in include_table_name_like_filters
                        ]
                    )
                )
            )

        if exclude_table_name_like_filters:
            for exclude_table_name_like_filter in exclude_table_name_like_filters:
                table_select.append(
                    WHERE(NOT_LIKE(LOWER(COLUMN("table_name")), LITERAL(exclude_table_name_like_filter.lower())))
                )
                matview_select.append(
                    WHERE(NOT_LIKE(LOWER(COLUMN("name")), LITERAL(exclude_table_name_like_filter.lower())))
                )

        return statement
