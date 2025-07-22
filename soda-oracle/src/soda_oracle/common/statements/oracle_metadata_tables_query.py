# from __future__ import annotations

# from typing import Optional

# from soda_core.common.data_source_results import QueryResult
# from soda_core.common.sql_ast import *
# from soda_core.common.statements.metadata_tables_query import (
#     FullyQualifiedTableName,
#     MetadataTablesQuery,
# )


# class OracleMetadataTablesQuery(MetadataTablesQuery):
#     def build_sql_statement(
#         self,
#         database_name: Optional[str] = None,
#         schema_name: Optional[str] = None,
#         include_table_name_like_filters: Optional[list[str]] = None,
#         exclude_table_name_like_filters: Optional[list[str]] = None,
#     ) -> list:
#         """
#         Oracle-specific metadata tables query.
#         Oracle doesn't have database catalogs, so we select NULL as the database name,
#         OWNER as schema, and TABLE_NAME as table name.
#         """

#         if self.prefixes is not None:
#             prefixes = self.prefixes
#         else:
#             prefixes = []

#         select: list = [
#             FROM(
#                 self.sql_dialect.table_tables(),
#                 # Oracle doesn't use information_schema, just query ALL_TABLES directly
#                 table_prefix=prefixes,
#             ),
#             SELECT(
#                 [
#                     # Oracle doesn't have database catalogs, use literal NULL
#                     LITERAL(None),
#                     self.sql_dialect.column_table_schema(),  # OWNER
#                     self.sql_dialect.column_table_name(),    # TABLE_NAME
#                 ]
#             ),
#         ]

#         # Oracle doesn't have database catalogs, so skip database_name filtering

#         if schema_name:
#             schema_column_name: str = self.sql_dialect.column_table_schema()
#             schema_name_upper: str = schema_name.upper()  # Oracle uses uppercase
#             select.append(
#                 WHERE(EQ(schema_column_name, LITERAL(schema_name_upper)))
#             )

#         table_name_column = self.sql_dialect.column_table_name()

#         if include_table_name_like_filters:
#             select.append(
#                 WHERE(
#                     OR(
#                         [
#                             LIKE(
#                                 table_name_column,
#                                 LITERAL(include_table_name_like_filter.upper()),  # Oracle stores names in uppercase
#                             )
#                             for include_table_name_like_filter in include_table_name_like_filters
#                         ]
#                     )
#                 )
#             )

#         if exclude_table_name_like_filters:
#             for exclude_table_name_like_filter in exclude_table_name_like_filters:
#                 select.append(
#                     WHERE(
#                         NOT_LIKE(
#                             table_name_column,
#                             LITERAL(exclude_table_name_like_filter.upper()),  # Oracle stores names in uppercase
#                         )
#                     )
#                 )

#         return select

#     def get_results(self, query_result: QueryResult) -> list[FullyQualifiedTableName]:
#         return [
#             FullyQualifiedTableName(
#                 database_name=None,  # Oracle doesn't have database concept
#                 schema_name=schema_name,
#                 table_name=table_name,
#             )
#             for _, schema_name, table_name in query_result.rows  # Skip the NULL database_name
#         ]
