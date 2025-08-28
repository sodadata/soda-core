from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import ColumnMetadata
from soda_core.common.sql_dialect import AND, EQ, FROM, LITERAL, LOWER, SELECT, WHERE
from soda_core.common.statements.metadata_columns_query import MetadataColumnsQuery


class DatabricksMetadataColumnsQuery(MetadataColumnsQuery):
    def build_sql(self, dataset_prefix: Optional[list[str]], dataset_name: str) -> Optional[str]:
        """
        Builds the full SQL query to query table names from the data source metadata.

        Databricks specific:
        - type and char length are in the same column, full_data_type, e.g. varchar(255), we have to parse that from the result
        - everything is forced to lowercase in Databricks
        """
        return super().build_sql([prefix.lower() for prefix in dataset_prefix], dataset_name.lower())
        

    def get_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        return super().get_result(query_result)
