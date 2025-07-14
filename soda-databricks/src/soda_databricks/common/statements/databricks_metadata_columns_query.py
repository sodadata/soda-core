from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import *
from soda_core.common.statements.metadata_columns_query import (
    ColumnMetadata,
    MetadataColumnsQuery,
)


class DatabricksMetadataColumnsQuery(MetadataColumnsQuery):
    def build_sql(self, dataset_prefix: Optional[list[str]], dataset_name: str) -> Optional[str]:
        """
        Builds the full SQL query to query table names from the data source metadata.

        Databricks specific:
        - type and char length are in the same column, full_data_type, e.g. varchar(255), we have to parse that from the result
        - everything is forced to lowercase in Databricks
        """
        database_name: str = dataset_prefix[0]
        schema_name: str = dataset_prefix[1]
        return self.sql_dialect.build_select_sql(
            [
                SELECT(
                    [
                        self.sql_dialect.column_column_name(),
                        self.sql_dialect.column_data_type(),
                    ]
                ),
                FROM(self.sql_dialect.table_columns()).IN(
                    [database_name, self.sql_dialect.schema_information_schema()]
                ),
                WHERE(
                    AND(
                        [
                            EQ(LOWER(self.sql_dialect.column_table_catalog()), LOWER(LITERAL(database_name))),
                            EQ(LOWER(self.sql_dialect.column_table_schema()), LOWER(LITERAL(schema_name))),
                            EQ(LOWER(self.sql_dialect.column_table_name()), LOWER(LITERAL(dataset_name))),
                        ]
                    )
                ),
            ]
        )

    def get_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        metadata = []
        for column_name, full_data_type in query_result.rows:
            data_type = full_data_type.split("(")[0]

            character_maximum_length = None
            if "(" in full_data_type and ")" in full_data_type:
                character_maximum_length = int(full_data_type.split("(")[1].split(")")[0])

            if character_maximum_length == 0:
                character_maximum_length = None

            metadata.append(
                ColumnMetadata(
                    column_name=column_name, data_type=data_type, character_maximum_length=character_maximum_length
                )
            )

        return metadata
