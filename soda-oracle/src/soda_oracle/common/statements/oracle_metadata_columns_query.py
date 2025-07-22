# from __future__ import annotations

# from typing import Optional

# from soda_core.common.data_source_results import QueryResult
# from soda_core.common.sql_ast import *
# from soda_core.common.statements.metadata_columns_query import (
#     ColumnMetadata,
#     MetadataColumnsQuery,
# )


# class OracleMetadataColumnsQuery(MetadataColumnsQuery):
#     def build_sql(self, dataset_prefix: Optional[list[str]], dataset_name: str) -> Optional[str]:
#         """
#         Oracle-specific metadata columns query using ALL_TAB_COLUMNS.
#         Oracle doesn't have database catalogs, so we select NULL as the database name,
#         OWNER as schema, TABLE_NAME as table, and COLUMN_NAME, DATA_TYPE, DATA_LENGTH.
#         """
#         if self.prefixes is not None:
#             prefixes = self.prefixes
#         else:
#             prefixes = []

#         schema_name = dataset_prefix[-1] if dataset_prefix else None

#         select_statement = [
#             FROM(
#                 self.sql_dialect.table_columns(),
#                 # Oracle doesn't use information_schema, just query ALL_TAB_COLUMNS directly  
#                 table_prefix=prefixes,
#             ),
#             SELECT(
#                 [
#                     self.sql_dialect.column_column_name(),    # COLUMN_NAME
#                     # Oracle-specific: Combine DATA_TYPE with precision for VARCHAR2 only
#                     SqlExpressionStr(
#                         f"CASE "
#                         f"WHEN {self.sql_dialect.column_data_type()} = 'VARCHAR2' THEN "
#                         f"    {self.sql_dialect.column_data_type()} || '(' || {self.sql_dialect.column_data_type_max_length()} || ')' "
#                         f"ELSE {self.sql_dialect.column_data_type()} "
#                         f"END"
#                     ),
#                     self.sql_dialect.column_data_type_max_length(),  # DATA_LENGTH
#                 ]
#             ),
#         ]

#         if schema_name:
#             schema_column_name = self.sql_dialect.column_table_schema()  # OWNER
#             schema_name_upper = self.sql_dialect.default_casify(schema_name)  # Oracle uses uppercase
#             select_statement.append(
#                 WHERE(EQ(schema_column_name, LITERAL(schema_name_upper)))
#             )

#         table_name_column = self.sql_dialect.column_table_name()  # TABLE_NAME
#         dataset_name_upper = self.sql_dialect.default_casify(dataset_name)  # Oracle uses uppercase
#         select_statement.append(
#             WHERE(EQ(table_name_column, LITERAL(dataset_name_upper)))
#         )

#         return self.sql_dialect.build_select_sql(select_statement)