from __future__ import annotations

import enum


class DBDataType(str, enum.Enum):
    """
    DBDataTypes contains data source-neutral constants for referring to the basic, common column data types.
    """

    VARCHAR = "varchar"
    TEXT = "text"
    INTEGER = "integer"
    DECIMAL = "decimal"
    NUMERIC = "numeric"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamptz"
    BOOLEAN = "boolean"

    def __eq__(self, value: object) -> bool:
        return self is value or self.value == value

    def __hash__(self) -> int:
        return hash(self.value)
