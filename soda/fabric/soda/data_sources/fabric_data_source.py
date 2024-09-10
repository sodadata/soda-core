from soda.data_sources.sqlserver_data_source import SQLServerDataSource
from soda.execution.data_type import DataType


class FabricDataSource(SQLServerDataSource):
    TYPE = "fabric"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {"TEXT": ["varchar", "char"]}

    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "varchar(255)",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "float",
        DataType.DATE: "date",
        DataType.TIME: "time(6)",
        DataType.TIMESTAMP: "datetime2(6)",
        DataType.TIMESTAMP_TZ: "datetime2(6)",
        DataType.BOOLEAN: "bit",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP: dict = {
        DataType.TEXT: "varchar",
        DataType.INTEGER: "int",
        DataType.DECIMAL: "float",
        DataType.DATE: "date",
        DataType.TIME: "time(6)",
        DataType.TIMESTAMP: "datetime2(6)",
        DataType.TIMESTAMP_TZ: "datetime2(6)",
        DataType.BOOLEAN: "bit",
    }
    
    NUMERIC_TYPES_FOR_PROFILING = [
        "bigint",
        "numeric",
        "bit",
        "smallint",
        "decimal",
        "int",
        "float",
        "real",
    ]

    TEXT_TYPES_FOR_PROFILING = ["char", "varchar"]