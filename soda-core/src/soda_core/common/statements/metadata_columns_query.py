from __future__ import annotations

from dataclasses import dataclass

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_connection import QueryResult, DataSourceConnection
from soda_core.common.sql_dialect import SqlDialect


@dataclass
class MetadataColumn:
    name: str
    data_type: str


class MetadataColumnsQuery:

    def __init__(self, sql_dialect: SqlDialect, data_source_connection: DataSourceConnection):
        self.sql_dialect = sql_dialect
        self.data_source_connection = data_source_connection
        self.database_name: str | None = None
        self.schema_name: str | None = None
        self.dataset_name: str | None = None

    def with_database_name(self, database_name: str | None) -> MetadataColumnsQuery:
        self.database_name = database_name
        return self

    def with_schema_name(self, schema_name: str | None) -> MetadataColumnsQuery:
        self.schema_name = schema_name
        return self

    def with_dataset_name(self, dataset_name: str | None) -> MetadataColumnsQuery:
        self.dataset_name = dataset_name
        return self

    def execute(self, data_source: DataSource) -> list[MetadataColumn]:
        sql: str = self.build_sql()
        query_result: QueryResult = self.data_source.data_source_connection.execute_query(sql)
        return [
            MetadataColumn(
                name=column_name,
                data_type=data_type
            )
            for column_name, data_type in query_result.rows
        ]

    def build_sql(self) -> str:
        """
        Builds the full SQL query to query table names from the data source metadata.
        """
        return (
            f"SELECT {self._column_name_information_schema_columns_column_name()}, \n"
            f"       {self._column_name_information_schema_columns_data_type()} \n"
            f"FROM {self._information_schema_columns_table_qualified()} \n"
            f"WHERE {self._column_name_information_schema_columns_table_catalog()} = {self.sql_dialect.literal(self.database_name)} \n"
            f"  AND {self._column_name_information_schema_columns_table_schema()} = {self.sql_dialect.literal(self.schema_name)} \n"
            f"  AND {self._column_name_information_schema_columns_table_name()} = {self.sql_dialect.literal(self.dataset_name)};"
        )

    def _information_schema_columns_table_qualified(self) -> str:
        return self.sql_dialect.qualify_table(
            database_name=self.database_name,
            schema_name=self._schema_name_information_schema(),
            table_name=self._table_name_information_schema_columns()
        )

    def _schema_name_information_schema(self) -> str:
        """
        Name of the schema that has the metadata
        """
        return "information_schema"

    def _table_name_information_schema_columns(self) -> str:
        """
        Name of the table that has the columns information in the metadata
        """
        return "columns"

    def _column_name_information_schema_columns_table_catalog(self) -> str:
        """
        Name of the column that has the database information in the tables metadata table
        """
        return "table_catalog"

    def _column_name_information_schema_columns_table_schema(self) -> str:
        """
        Name of the column that has the schema information in the tables metadata table
        """
        return "table_schema"

    def _column_name_information_schema_columns_table_name(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table
        """
        return "table_name"

    def _column_name_information_schema_columns_column_name(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table
        """
        return "column_name"

    def _column_name_information_schema_columns_data_type(self) -> str:
        """
        Name of the column that has the table name in the tables metadata table
        """
        return "data_type"
