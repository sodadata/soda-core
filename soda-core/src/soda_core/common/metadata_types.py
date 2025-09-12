from __future__ import annotations

import enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional


@dataclass
class DataSourceNamespace(ABC):
    @abstractmethod
    def get_namespace_elements(self) -> list[str]:
        pass

    def get_database_for_metadata_query(self) -> str | None:
        return None

    @abstractmethod
    def get_schema_for_metadata_query(self) -> str:
        pass


@dataclass
class SchemaDataSourceNamespace(DataSourceNamespace):
    schema: str

    def get_namespace_elements(self) -> list[str]:
        return [self.schema]

    def get_schema_for_metadata_query(self) -> str:
        return self.schema


@dataclass
class DbSchemaDataSourceNamespace(DataSourceNamespace):
    database: str
    schema: str

    def get_namespace_elements(self) -> list[str]:
        return [self.database, self.schema]

    def get_database_for_metadata_query(self) -> str | None:
        return self.database

    def get_schema_for_metadata_query(self) -> str:
        return self.schema


@dataclass
class SqlDataType:
    name: str
    character_maximum_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    datetime_precision: Optional[int] = None

    def __post_init__(self):
        self.name = self.name.lower()

    def __str__(self) -> str:
        return self.get_sql_data_type_str_with_parameters()

    def get_sql_data_type_str_with_parameters(self) -> str:
        """
        Returns the SQL data type including the parameters in round brackets for usage in a CREATE TABLE statement.
        """
        if self.character_maximum_length is not None:
            return f"{self.name}({self.character_maximum_length})"
        elif self.numeric_precision is not None and self.numeric_scale is not None:
            return f"{self.name}({self.numeric_precision},{self.numeric_scale})"
        elif self.numeric_precision is not None:
            return f"{self.name}({self.numeric_precision})"
        elif self.datetime_precision is not None:
            return f"{self.name}({self.datetime_precision})"
        else:
            return self.name

    def replace_data_type_name(self, new_data_type_name: str) -> SqlDataType:
        return type(self)(
            name=new_data_type_name,
            character_maximum_length=self.character_maximum_length,
            numeric_precision=self.numeric_precision,
            numeric_scale=self.numeric_scale,
            datetime_precision=self.datetime_precision,
        )


@dataclass
class ColumnMetadata:
    column_name: str

    # Can be None in case of schema check expected columns
    # without data type expectations
    sql_data_type: Optional[SqlDataType] = None


class SodaDataTypeName(str, enum.Enum):
    """
    SodaDataTypeNames contain common data source-neutral constants for referring to the basic, common column data types.
    Its used to create test tables for tests that need to be created on each type of data source.
    And in the DWH it's used to map common data types to concrete ones for the Soda columns.
    """

    CHAR = "char"  # fixed length string
    VARCHAR = "varchar"  # bounded variable length string
    TEXT = "text"  # unbounded variable length string

    SMALLINT = "smallint"  # integer, 2 bytes
    INTEGER = "integer"  # integer, 4 bytes
    BIGINT = "bigint"  # integer, 8 bytes

    DECIMAL = "decimal"  # exact values with configured precision
    NUMERIC = "numeric"  # exact values with configured precision

    FLOAT = "float"  # 4 bytes floating point number
    DOUBLE = "double"  # 8 bytes floating point number

    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamp_tz"

    DATE = "date"
    TIME = "time"

    BOOLEAN = "boolean"

    def __eq__(self, value: object) -> bool:
        return self is value or self.value == value

    def __hash__(self) -> int:
        return hash(self.value)
