from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_results import QueryResult
from soda_core.common.sql_ast import (
    AND,
    EQ,
    FROM,
    LITERAL,
    ORDER_BY_ASC,
    ORDINAL_POSITION,
    SELECT,
    WHERE,
    SqlDataType,
)
from soda_core.common.sql_dialect import SqlDialect


@dataclass
class ColumnMetadata:
    column_name: str

    # Deprecated. Replaced by sql_data_type below
    data_type: str

    # Deprecated. Replaced by sql_data_type below
    character_maximum_length: Optional[int]

    sql_data_type: Optional[SqlDataType] = None

    # Deprecated. Replaced by SqlDataType.to_create_table_column_type above
    def get_data_type_ddl(self) -> str:
        if self.sql_data_type is not None:
            return self.sql_data_type.get_create_table_column_type()
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

        join_prefixes = lambda prefixes, schema: [*prefixes, schema] if schema else prefixes
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
                            if self.sql_dialect.supports_data_type_character_maximun_length()
                            else []
                        ),
                        *(
                            [self.sql_dialect.column_data_type_numeric_precision()]
                            if self.sql_dialect.supports_data_type_numeric_precision()
                            else []
                        ),
                        *(
                            [self.sql_dialect.column_data_type_numeric_scale()]
                            if self.sql_dialect.supports_data_type_numeric_scale()
                            else []
                        ),
                        *(
                            [self.sql_dialect.column_data_type_datetime_precision()]
                            if self.sql_dialect.supports_data_type_datetime_precision()
                            else []
                        ),
                    ]
                ),
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
                            EQ(self.sql_dialect.column_table_name(), LITERAL(dataset_name)),
                        ]
                    )
                ),
                ORDER_BY_ASC(ORDINAL_POSITION()),
            ]
        )

    def get_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        character_maximum_length_index: Optional[int] = None
        numeric_precision_index: Optional[int] = None
        numeric_scale_index: Optional[int] = None
        datetime_precision_index: Optional[int] = None

        optional_values_index: int = 2
        if self.sql_dialect.supports_data_type_character_maximun_length():
            character_maximum_length_index = optional_values_index
            optional_values_index += 1

        if self.sql_dialect.supports_data_type_numeric_precision():
            numeric_precision_index = optional_values_index
            optional_values_index += 1

        if self.sql_dialect.supports_data_type_numeric_scale():
            numeric_scale_index = optional_values_index
            optional_values_index += 1

        if self.sql_dialect.supports_data_type_datetime_precision():
            datetime_precision_index = optional_values_index
            optional_values_index += 1

        column_metadatas: list[ColumnMetadata] = []
        for row in query_result.rows:
            column_name: str = row[0]
            data_type_name: str = self.sql_dialect.format_metadata_data_type(row[1])
            character_maximum_length: Optional[int] = (
                row[character_maximum_length_index] if character_maximum_length_index else None
            )
            numeric_precision: Optional[int] = row[numeric_precision_index] if numeric_precision_index else None
            numeric_scale: Optional[int] = row[numeric_scale_index] if numeric_scale_index else None
            datetime_precision: Optional[int] = row[datetime_precision_index] if datetime_precision_index else None
            column_metadatas.append(
                ColumnMetadata(
                    # Format data_type value here if needed -- default no-op
                    column_name=column_name,
                    data_type=data_type_name,
                    character_maximum_length=character_maximum_length,
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
