from __future__ import annotations

from typing import Optional

from soda_core.common.sql_ast import (
    COLUMN,
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
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.statements.table_types import (
    FullyQualifiedMaterializedViewName,
    FullyQualifiedObjectName,
    TableType,
)


class TrinoMetadataTablesQuery(MetadataTablesQuery):
    """Trino-specific metadata query that discovers materialized views via
    ``system.metadata.materialized_views`` because Trino's
    ``information_schema.tables`` does not list them.

    See: https://trino.io/docs/current/connector/system.html
    See: https://github.com/trinodb/trino/issues/8207
    """

    def execute(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
        types_to_return: Optional[list[TableType]] = None,
    ) -> list[FullyQualifiedObjectName]:
        if types_to_return is None:
            types_to_return = [TableType.TABLE]

        results: list[FullyQualifiedObjectName] = []

        non_mv_types = [t for t in types_to_return if t != TableType.MATERIALIZED_VIEW]
        if non_mv_types:
            results.extend(
                super().execute(
                    database_name=database_name,
                    schema_name=schema_name,
                    include_table_name_like_filters=include_table_name_like_filters,
                    exclude_table_name_like_filters=exclude_table_name_like_filters,
                    types_to_return=non_mv_types,
                )
            )

        if TableType.MATERIALIZED_VIEW in types_to_return:
            results.extend(
                self._query_materialized_views(
                    database_name=database_name,
                    schema_name=schema_name,
                    include_table_name_like_filters=include_table_name_like_filters,
                    exclude_table_name_like_filters=exclude_table_name_like_filters,
                )
            )

        return results

    def _query_materialized_views(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
    ) -> list[FullyQualifiedMaterializedViewName]:
        select: list = [
            SELECT([COLUMN("catalog_name"), COLUMN("schema_name"), COLUMN("name")]),
            FROM("materialized_views", table_prefix=["system", "metadata"]),
        ]

        if database_name:
            select.append(WHERE(EQ(LOWER("catalog_name"), LITERAL(database_name.lower()))))

        if schema_name:
            select.append(WHERE(EQ(LOWER("schema_name"), LITERAL(schema_name.lower()))))

        if include_table_name_like_filters:
            select.append(WHERE(OR([LIKE(LOWER("name"), LITERAL(f.lower())) for f in include_table_name_like_filters])))

        if exclude_table_name_like_filters:
            for f in exclude_table_name_like_filters:
                select.append(WHERE(NOT_LIKE(LOWER("name"), LITERAL(f.lower()))))

        sql = self.sql_dialect.build_select_sql(select)
        query_result = self.data_source_connection.execute_query(sql)

        return [
            FullyQualifiedMaterializedViewName(
                database_name=catalog_name,
                schema_name=mv_schema_name,
                materialized_view_name=name,
            )
            for catalog_name, mv_schema_name, name in query_result.rows
        ]
