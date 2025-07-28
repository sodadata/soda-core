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

    def get_data_type_ddl(self) -> str:
        if self.character_maximum_length is None:
            return self.data_type
        else:
            return f"{self.data_type}({self.character_maximum_length})"


class MetadataColumnsQuery:
    def __init__(
        self,
        sql_dialect: SqlDialect,
        data_source_connection: DataSourceConnection,
        prefixes: Optional[list[str]] = None,  # Note: if we use prefixes, we will not use the database_name
    ):
        self.sql_dialect = sql_dialect
        self.data_source_connection: DataSourceConnection = data_source_connection
        self.prefixes = prefixes

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

        if self.prefixes is not None:
            prefixes = self.prefixes
        else:
            prefixes = [database_name] if database_name else []

        return self.sql_dialect.build_select_sql(
            [
                SELECT(
                    [
                        self.sql_dialect.column_column_name(),
                        self.sql_dialect.column_data_type(),
                        *(
                            [self.sql_dialect.column_data_type_max_length()]
                            if self.sql_dialect.supports_varchar_length()
                            else []
                        ),
                    ]
                ),
                FROM(self.sql_dialect.table_columns()).IN(
                    [
                        id
                        for id in [  # prefixes can be None if information schema is top-level, e.g. Oracle
                            *prefixes,
                            self.sql_dialect.schema_information_schema(),
                        ]
                        if id
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
        if self.sql_dialect.supports_varchar_length():
            return [
                ColumnMetadata(
                    column_name=column_name, data_type=data_type, character_maximum_length=character_maximum_length
                )
                for column_name, data_type, character_maximum_length in query_result.rows
            ]
        else:
            return [
                ColumnMetadata(column_name=column_name, data_type=data_type, character_maximum_length=None)
                for column_name, data_type in query_result.rows
            ]
