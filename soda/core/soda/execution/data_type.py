from __future__ import annotations


class DataType:
    """
    Generic data types that data_sources can translate to specific types
    """

    TEXT = "text"
    INTEGER = "integer"
    DECIMAL = "decimal"
    DATE = "date"
    TIME = "time"
    TIMESTAMP = "timestamp"
    TIMESTAMP_TZ = "timestamptz"
    BOOLEAN = "boolean"
