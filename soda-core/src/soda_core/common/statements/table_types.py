from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


# Create a enum of table types
class TableType(Enum):
    TABLE = "BASE TABLE"
    VIEW = "VIEW"


@dataclass
class FullyQualifiedObjectName:
    database_name: str
    schema_name: str

    def get_object_name(self) -> str:
        # Returns the specific object name (e.g. the table name, or the view name)
        raise NotImplementedError("Subclasses must implement this method")


@dataclass
class FullyQualifiedTableName(FullyQualifiedObjectName):
    table_name: str

    def get_object_name(self) -> str:
        return self.table_name


@dataclass
class FullyQualifiedViewName(FullyQualifiedObjectName):
    view_name: str

    def get_object_name(self) -> str:
        return self.view_name
