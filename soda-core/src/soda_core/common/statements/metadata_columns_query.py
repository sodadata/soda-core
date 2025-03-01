from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_dialect import *


@dataclass
class ColumnMetadata:
    column_name: str
    data_type: str
    character_maximum_length: Optional[int]


class MetadataColumnsQuery:

    def __init__(
        self,
        sql_dialect: SqlDialect,
        data_source_connection: DataSourceConnection
    ):
        self.sql_dialect = sql_dialect
        self.data_source_connection: DataSourceConnection = data_source_connection

    def build_sql(
        self,
        dataset_prefix: Optional[list[str]],
        dataset_name: str
    ) -> Optional[str]:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """
        database_name: str = dataset_prefix[0]
        schema_name: str = dataset_prefix[1]
        return self.sql_dialect.build_select_sql([
            SELECT([
                self._column_column_name(),
                self._column_data_type(),
                self._column_data_type_max_length()]),
            FROM(self._table_columns()).IN([database_name, self._schema_information_schema()]),
            WHERE(AND([
                EQ(self._column_table_catalog(), LITERAL(database_name)),
                EQ(self._column_table_schema(), LITERAL(schema_name)),
                EQ(self._column_table_name(), LITERAL(dataset_name))
            ])),
        ])

    def get_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        return [
            ColumnMetadata(
                column_name=column_name,
                data_type=data_type,
                character_maximum_length=character_maximum_length
            )
            for column_name, data_type, character_maximum_length in query_result.rows
        ]

    def _schema_information_schema(self) -> str:
        """
        Name of the schema that has the metadata.
        Purpose of this method is to allow specific data source to override.
        """
        return "information_schema"

    def _table_columns(self) -> str:
        """
        Name of the table that has the columns information in the metadata.
        Purpose of this method is to allow specific data source to override.
        """
        return "columns"

    def _column_table_catalog(self) -> str:
        """
        Name of the column that has the database information in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return "table_catalog"

    def _column_table_schema(self) -> str:
        """
        Name of the column that has the schema information in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return "table_schema"

    def _column_table_name(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return "table_name"

    def _column_column_name(self) -> str:
        """
        Name of the column that has the column name in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return "column_name"

    def _column_data_type(self) -> str:
        """
        Name of the column that has the data type in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return "data_type"

    def _column_data_type_max_length(self) -> str:
        """
        Name of the column that has the mas data type length in the tables metadata table.
        Purpose of this method is to allow specific data source to override.
        """
        return "character_maximum_length"
