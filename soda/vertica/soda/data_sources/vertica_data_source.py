from typing import Optional

import vertica_python
from soda.common.exceptions import DataSourceConnectionError
from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType


class VerticaDataSource(DataSource):

    TYPE: str = "vertica"

    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        "VARCHAR": ["varchar"],
        "CHAR": ["char"],
        "LONG VARCHAR": ["text"],
        "BOOLEAN": ["boolean", "bool"],
        "BINARY": ["binary", "bin"],
        "VARBINARY": ["varbinary", "varbin"],
        "LONG VARBINARY": ["long varbinary", "long varbin"],
        "DATE": ["date"],
        "TIME": ["time"],
        "TIMESTAMP": ["timestamp"],
        "TIME WITH TIMEZONE": ["time with tz", "time"],
        "TIMESTAMP WITH TIMEZONE": ["timestamp with tz", "timestamp"],
        "INTERVAL": ["interval"],
        "INTERVAL DAY TO SECOND": ["interval day to second"],
        "INTERVAL YEAR TO MONTH": ["interval year to month"],
        "FLOAT": ["float", "float8", "real", "double precision", "double"],
        "INTEGER": ["integer", "int", "bigint", "int8", "smallint", "tinyint"],
        "DECIMAL": ["decimal", "numeric", "number", "money"],
    }

    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "VARCHAR(255)",
        DataType.INTEGER: "INT",
        DataType.DECIMAL: "DECIMAL",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMP_TZ: "TIMESTAMP WITH TIMEZONE",
        DataType.BOOLEAN: "BOOLEAN",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP: dict = {
        DataType.TEXT: "Varchar",
        DataType.INTEGER: "Integer",
        DataType.DECIMAL: "Numeric",
        DataType.DATE: "Date",
        DataType.TIME: "Time",
        DataType.TIMESTAMP: "Timestamp",
        DataType.TIMESTAMP_TZ: "TimestampTz",
        DataType.BOOLEAN: "Boolean",
    }

    NUMERIC_TYPES_FOR_PROFILING: list = [
        "float",
        "float8",
        "real",
        "double precision",
        "double",
        "integer",
        "int",
        "bigint",
        "int8",
        "smallint",
        "tinyint",
        "decimal",
        "numeric",
        "number",
        "money",
    ]

    TEXT_TYPES_FOR_PROFILING: list = ["varchar", "char", "text"]

    LIMIT_KEYWORD: str = "LIMIT"

    def __init__(self, logs: Logs, data_source_name: str, data_source_properties: dict):
        super().__init__(logs, data_source_name, data_source_properties)

        self.host = data_source_properties.get("host", "localhost")
        self.port = data_source_properties.get("port", "5433")
        self.username = data_source_properties.get("username", "dbadmin")
        self.password = data_source_properties.get("password", "password")
        self.database = data_source_properties.get("database", "vmart")  # TODO: find default DB name
        self.schema = data_source_properties.get("schema", "public")

    def connect(self):
        try:
            self.connection = vertica_python.connect(
                user=self.username, password=self.password, host=self.host, port=self.port, database=self.database
            )
            return self.connection
        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

    def safe_connection_data(self):
        return [self.type, self.host, self.port, self.username, self.database]

    def sql_get_table_names_with_count(
        self, include_tables: Optional[list[str]] = None, exclude_tables: Optional[list[str]] = None
    ) -> str:

        table_filter_expression = self.sql_table_include_exclude_filter(
            "anchor_table_name", "schema_name", include_tables, exclude_tables
        )

        where_clause = f"AND {table_filter_expression}" if table_filter_expression else ""

        sql = f"""
        with

        num_rows as (
            select
                schema_name,
                anchor_table_name as table_name,
                sum(total_row_count) as rows
            from v_monitor.storage_containers sc
                join v_catalog.projections p
                    on sc.projection_id = p.projection_id
                        and p.is_super_projection = true
            where True
                {where_clause}
            group by schema_name  anchor_table_name, sc.projection_id
        )

        select
            table_name,
            max(rows) as row_count
        from num_rows
        group by table_name
        order by rows desc
        ;
        """

        return sql

    @staticmethod
    def column_metadata_catalog_column() -> str:
        return "v_catalog"

    def validate_configuration(self, logs: Logs) -> None:
        pass

    def sql_information_schema_tables(self) -> str:
        return "v_catalog.tables"

    def sql_information_schema_columns(self) -> str:
        return "v_catalog.columns"
