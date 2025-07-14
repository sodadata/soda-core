from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import *
from soda_core.common.statements.metadata_columns_query import (
    ColumnMetadata,
    MetadataColumnsQuery,
)


class BigQueryMetadataColumnsQuery(MetadataColumnsQuery):
    def __init__(self, sql_dialect: SqlDialect, data_source_connection: DataSourceConnection, location: str):
        super().__init__(sql_dialect, data_source_connection)
        self.location = location

    def build_sql(self, dataset_prefix: Optional[list[str]], dataset_name: str) -> Optional[str]:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """

        database_name: Optional[str] = None
        if (db_index := self.sql_dialect.get_database_prefix_index()) is not None:
            database_name = dataset_prefix[db_index]

        schema_name: Optional[str] = None
        if (schema_index := self.sql_dialect.get_schema_prefix_index()) is not None:
            schema_name = dataset_prefix[schema_index]

        return self.sql_dialect.build_select_sql(
            [
                SELECT(
                    [
                        self.sql_dialect.column_column_name(),
                        self.sql_dialect.column_data_type(),
                    ]
                ),
                FROM(self.sql_dialect.table_columns()).IN(
                    [
                        f"region-{self.location}",
                        self.sql_dialect.schema_information_schema(),
                    ]
                ),
                WHERE(
                    AND(
                        [
                            *(
                                [EQ(self.sql_dialect.column_table_catalog(), LITERAL(database_name))]
                                if database_name
                                else []
                            ),
                            EQ(self.sql_dialect.column_table_schema(), LITERAL(schema_name)),
                            EQ(self.sql_dialect.column_table_name(), LITERAL(dataset_name)),
                        ]
                    )
                ),
                ORDER_BY_ASC(ORDINAL_POSITION()),
            ]
        )

    def get_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        return [
            ColumnMetadata(
                column_name=column_name,
                data_type=data_type,
                character_maximum_length=None,  # BigQuery does not support character_maximum_length
            )
            for column_name, data_type in query_result.rows
        ]
