from __future__ import annotations

from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.metadata_types import ColumnMetadata, SqlDataType
from soda_core.common.sql_ast import (
    AND,
    EQ,
    FROM,
    LITERAL,
    ORDER_BY_ASC,
    ORDINAL_POSITION,
    SELECT,
    WHERE,
)
from soda_core.common.sql_dialect import SqlDialect


class AthenaMetadataColumnsQuery:
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

        join_prefixes = lambda prefixes, schema: [*prefixes, schema] if schema else prefixes
        if self.prefixes is not None:
            prefixes = self.prefixes
        else:
            prefixes = [database_name] if database_name else []

        return self.sql_dialect.build_select_sql(
            [
                SELECT([self.sql_dialect.column_column_name(), self.sql_dialect.column_data_type()]),
                FROM(self.sql_dialect.table_columns()).IN(
                    join_prefixes(prefixes, self.sql_dialect.schema_information_schema())
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
                            EQ(
                                self.sql_dialect.column_table_name(),
                                LITERAL(self.sql_dialect.default_casify(dataset_name)),
                            ),
                        ]
                    )
                ),
                ORDER_BY_ASC(ORDINAL_POSITION()),
            ]
        )

    def get_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        column_metadatas: list[ColumnMetadata] = []
        for row in query_result.rows:
            column_name: str = row[0]
            data_type_name: str = self.sql_dialect.format_metadata_data_type(row[1])

            # Varchars are a special case, they may contain a length parameter, but not always!
            if self.sql_dialect.data_type_has_parameter_character_maximum_length(data_type_name):
                try:
                    data_type_tuple = row[1][len(data_type_name) + 1 : -1].split(",")
                    character_maximum_length = int(data_type_tuple[0])
                except ValueError:
                    character_maximum_length = None
            else:
                character_maximum_length = None

            # Athena returns decimal(10,0) for NUMERIC, so we need to extract the precision and scale at the same time
            if self.sql_dialect.data_type_has_parameter_numeric_precision(
                data_type_name
            ) and self.sql_dialect.data_type_has_parameter_numeric_scale(data_type_name):
                data_type_tuple = row[1][len(data_type_name) + 1 : -1].split(",")
                numeric_precision = int(data_type_tuple[0])
                numeric_scale = int(data_type_tuple[1])
            else:
                numeric_precision = None
                numeric_scale = None

            # Athena returns timestamp(3) for TIMESTAMP and TIMESTAMP_TZ, so we need to extract the precision at the same time
            # We can't modify the time precision with CREATE TABLE, so we need to ignore it
            if (
                self.sql_dialect.data_type_has_parameter_datetime_precision(data_type_name)
                and self.sql_dialect.supports_data_type_datetime_precision()
            ):
                data_type_tuple = row[1][len(data_type_name) + 1 : -1].split(",")
                datetime_precision = int(data_type_tuple[0])
            else:
                datetime_precision = None

            column_metadatas.append(
                ColumnMetadata(
                    column_name=column_name,
                    sql_data_type=SqlDataType(
                        name=data_type_name,
                        character_maximum_length=character_maximum_length,
                        numeric_precision=numeric_precision,
                        numeric_scale=numeric_scale,
                        datetime_precision=datetime_precision,
                    ),
                )
            )
        return column_metadatas
