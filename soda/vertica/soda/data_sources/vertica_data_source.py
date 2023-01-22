from typing import List, Optional

import vertica_python
from soda.common.exceptions import DataSourceConnectionError
from soda.common.logs import Logs
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType
from soda.execution.query.query import Query


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

    @staticmethod
    def column_metadata_columns() -> list:
        return ["column_name", "data_type_id", "is_nullable"]

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
        pass

    def validate_configuration(self, logs: Logs) -> None:
        pass

    def sql_information_schema_tables(self) -> str:
        return "v_catalog.tables"

    def sql_information_schema_columns(self) -> str:
        return "v_catalog.columns"

    def sql_get_table_columns(
        self,
        table_name: str,
        included_columns: list[str] | None = None,
        excluded_columns: list[str] | None = None,
    ) -> str:

        table_name_default_case = self.default_casify_table_name(table_name)

        unquoted_table_name_default_case = (
            table_name_default_case[1:-1] if self.is_quoted(table_name_default_case) else table_name_default_case
        )

        filter_clauses = [f"table_name = '{unquoted_table_name_default_case}'"]

        if self.schema:
            filter_clauses.append(f"table_schema = '{self.schema}'")

        if included_columns:
            include_clauses = []
            for col in included_columns:
                include_clauses.append(f"column_name LIKE '{col}'")
            include_causes_or = " OR ".join(include_clauses)
            filter_clauses.append(f"({include_causes_or})")

        if excluded_columns:
            for col in excluded_columns:
                filter_clauses.append(f"column_name NOT LIKE '{col}'")

        where_filter = " \n  AND ".join(filter_clauses)

        sql = (
            f"SELECT {', '.join(self.column_metadata_columns())} \n"
            f"FROM {self.sql_information_schema_columns()} \n"
            f"WHERE {where_filter}"
            f"\nORDER BY {self.get_ordinal_position_name()}"
        )

        return sql

    def default_casify_system_name(self, identifier: str) -> str:
        return identifier

    def default_casify_table_name(self, identifier: str) -> str:
        return identifier

    def default_casify_column_name(self, identifier: str) -> str:
        return identifier

    def default_casify_type_name(self, identifier: str) -> str:
        return identifier

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: Optional[List[object]], expr: str):

        if metric_name in [
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "variance",
            "var_pop",
            "var_samp",
        ]:
            return f"{metric_name.upper()}({expr})"

        if metric_name == "percentile":
            return f"APPROXIMATE_PERCENTILE ({expr} USING PARAMETERS percentile = {metric_args[1]})"

        return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    def regex_replace_flags(self) -> str:
        return ""

    def get_type_name(self, type_code):
        type_code_str = str(type_code)

        return (
            self.type_names_by_type_code[type_code_str]
            if type_code_str in self.type_names_by_type_code
            else type_code_str
        )

    type_names_by_type_code = {
        "5": "Boolean",
        "6": "Integer",
        "7": "Float",
        "8": "Char",
        "9": "Varchar",
        "17": "Varbinary",
        "115": "Long Varchar",
        "116": "Long Varbinary",
        "117": "Binary",
        "16": "Numeric",
        "10": "Date",
        "11": "Time",
        "15": "TimeTz",
        "12": "Timestamp",
        "13": "TimestampTz",
        "20": "Uuid",
        "1505": "Array[Boolean]",
        "1506": "Array[Int8]",
        "1507": "Array[Float8]",
        "1508": "Array[Char]",
        "1509": "Array[Varchar]",
        "1517": "Array[Varbinary]",
        "1522": "Array[Binary]",
        "1516": "Array[Numeric]",
        "1510": "Array[Date]",
        "1511": "Array[Time]",
        "1515": "Array[TimeTz]",
        "1512": "Array[Timestamp]",
        "1513": "Array[TimestampTz]",
        "1520": "Array[Uuid]",
        "2705": "Set[Boolean]",
        "2706": "Set[Int8]",
        "2707": "Set[Float8]",
        "2708": "Set[Char]",
        "2709": "Set[Varchar]",
        "2717": "Set[Varbinary]",
        "2722": "Set[Binary]",
        "2716": "Set[Numeric]",
        "2710": "Set[Date]",
        "2711": "Set[Time]",
        "2715": "Set[TimeTz]",
        "2712": "Set[Timestamp]",
        "2713": "Set[TimestampTz]",
        "2720": "Set[Uuid]",
        "45035996273705976": "geometry",
        "45035996273705978": "geography",
    }

    def get_table_columns(
        self,
        table_name: str,
        query_name: str,
        included_columns: list[str] | None = None,
        excluded_columns: list[str] | None = None,
    ) -> dict[str, str] | None:

        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=query_name,
            sql=self.sql_get_table_columns(
                table_name, included_columns=included_columns, excluded_columns=excluded_columns
            ),
        )

        query.execute()

        if query.rows and len(query.rows) > 0:
            return {row[0]: self.get_type_name(row[1]) for row in query.rows}

        return None
