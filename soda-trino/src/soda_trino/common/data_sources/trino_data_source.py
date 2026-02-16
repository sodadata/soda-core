import logging
import re
from datetime import datetime, time
from typing import Any, Optional, Tuple

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import STRING_HASH
from soda_core.common.sql_dialect import SqlDialect
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSource as TrinoDataSourceModel,
)
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSourceConnection,
)

logger: logging.Logger = soda_logger




class TrinoDataSourceImpl(DataSourceImpl, model_class=TrinoDataSourceModel):
    def __init__(self, data_source_model: TrinoDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return TrinoSqlDialect(data_source_impl=self)

    def _create_data_source_connection(self) -> DataSourceConnection:
        return TrinoDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class TrinoSqlDataType(SqlDataType):
    def get_sql_data_type_str_with_parameters(self) -> str:
        if isinstance(self.datetime_precision, int) and self.name == "timestamp without time zone":
            return f"timestamp({self.datetime_precision}) without time zone"
        if isinstance(self.datetime_precision, int) and self.name == "timestamp with time zone":
            return f"timestamp({self.datetime_precision}) with time zone"
        return super().get_sql_data_type_str_with_parameters()


class TrinoSqlDialect(SqlDialect):
    USES_SEMICOLONS_BY_DEFAULT: bool = False
    SUPPORTS_DROP_TABLE_CASCADE: bool = False

    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
    )

    def supports_materialized_views(self) -> bool:
        # TODO  this should inherit from the connection
        return False

    def supports_views(self) -> bool:  # Default to True, but can be overridden by specific data sources if they don't support views (Dremio)
        # TODO  this should inherit from the connection
        return False

    def supports_case_sensitive_column_names(self) -> bool:
        # TODO -- it's possible that some connectors do support case sensitive names, investigate
        # This open issue seems to suggest that support is still pending https://github.com/trinodb/trino/issues/17
        return False

    def get_sql_data_type_class(self) -> type:
        return TrinoSqlDataType

    @staticmethod
    def _extract_type_parameter(raw: str) -> Optional[int]:
        """Extract the integer parameter from a raw type string like 'varchar(255)' or 'timestamp(3)'."""
        match = re.search(r'\((\d+)\)', raw)
        return int(match.group(1)) if match else None

    def extract_data_type_name(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> str:
        raw = row[1]
        # "timestamp(3) with time zone" → "timestamp with time zone"
        return re.sub(r'\(\d+\)', '', raw).replace('  ', ' ').strip()

    def extract_character_maximum_length(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_character_maximum_length(data_type_name):
            return None
        return self._extract_type_parameter(row[1])

    def extract_numeric_precision(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_precision(data_type_name):
            return None
        formatted_data_type_name: str = self.format_metadata_data_type(data_type_name)
        data_type_tuple = data_type_name[len(formatted_data_type_name) + 1 : -1].split(",")
        return int(data_type_tuple[0])

    def extract_numeric_scale(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_scale(data_type_name):
            return None

        formatted_data_type_name: str = self.format_metadata_data_type(data_type_name)
        data_type_tuple = data_type_name[len(formatted_data_type_name) + 1 : -1].split(",")
        return int(data_type_tuple[1])

    def extract_datetime_precision(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        data_type_name = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_datetime_precision(data_type_name):
            return None
        return self._extract_type_parameter(row[1])

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [["INTEGER", "INT"]]

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[SodaDataTypeName, str]:

        return {
            SodaDataTypeName.CHAR: "char",
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "varchar",
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.NUMERIC: "decimal",
            SodaDataTypeName.FLOAT: "real",
            SodaDataTypeName.DOUBLE: "double",
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp with time zone",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "boolean": SodaDataTypeName.BOOLEAN,
            "tinyint": SodaDataTypeName.SMALLINT,
            "smallint": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "real": SodaDataTypeName.FLOAT,
            "double": SodaDataTypeName.DOUBLE,
            "decimal": SodaDataTypeName.DECIMAL,
            "varchar": SodaDataTypeName.VARCHAR,
            "char": SodaDataTypeName.CHAR,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp with time zone": SodaDataTypeName.TIMESTAMP_TZ,
        }


    def column_data_type_max_length(self) -> Optional[str]:
        return None

    def supports_data_type_character_maximum_length(self) -> bool:
        return True

    def column_data_type_numeric_precision(self) -> Optional[str]:
        return None

    def supports_data_type_numeric_precision(self) -> bool:
        return True

    def column_data_type_numeric_scale(self) -> Optional[str]:
        return None

    def default_numeric_precision(self) -> Optional[int]:
        return 38

    def default_numeric_scale(self) -> Optional[int]:
        return 0


    def supports_data_type_numeric_scale(self) -> bool:
        return True

    def column_data_type_datetime_precision(self) -> Optional[str]:
        return None

    def supports_data_type_datetime_precision(self) -> bool:
        return True

    def supports_datetime_microseconds(self) -> bool:
        return True

    def metadata_casify(self, identifier: str) -> str:
        # trino lower-cases metadata identifiers
        return identifier.lower()

    def format_metadata_data_type(self, data_type: str) -> str:
        """Strip modifiers
        """
        paranthesis_index = data_type.find("(")
        if paranthesis_index != -1:
            return data_type[:paranthesis_index]
        return data_type



    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in [
            "varchar",
            "char"]

    def data_type_has_parameter_numeric_precision(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() == "decimal"

    def data_type_has_parameter_numeric_scale(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() == "decimal"

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return self.format_metadata_data_type(data_type_name).lower() in [
            "timestamp",
            "timestamp with time zone",
            "time",
            "time with time zone" # unsupported in Soda for now
        ]
    def literal_datetime(self, datetime: datetime):
        return f"TIMESTAMP '{datetime.strftime('%Y-%m-%d %H:%M:%S.%f')}'"

    def literal_datetime_with_tz(self, datetime: datetime):
        return f"TIMESTAMP '{datetime.strftime('%Y-%m-%d %H:%M:%S.%f%z')}'"

    def literal_time(self, time: time):
        return f"TIME '{time.strftime("%H:%M:%S.%f")}'"

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"TIMESTAMP '{datetime_in_iso8601.replace("T", " ")}'"

    def convert_str_to_datetime(self, datetime_str: str) -> datetime:
        datetime_iso_str = datetime.strptime(
            datetime_str, "%Y-%m-%d %H:%M:%S.%f%z"
        ).isoformat()
        return convert_str_to_datetime(datetime_iso_str)

    def convert_datetime_to_str(self, datetime: datetime) -> str:
        datetime_iso_str: str = convert_datetime_to_str(datetime)
        return datetime_iso_str.replace("T", " ")

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"DATE_ADD('day', 1, {timestamp_literal})"



    def _build_string_hash_sql(self, string_hash: STRING_HASH) -> str:
        # all Trino hash methods operate on binary data - TO_UTF8 converts string to binary
        return f"TO_HEX(MD5(TO_UTF8({self.build_expression_sql(string_hash.expression)})))"


    # def default_casify_table_name(self, identifier: str) -> str:
    #     """Formats table identifier to e.g. a default case for a given data source."""
    #     return identifier.lower()

    # def default_casify_column_name(self, identifier: str) -> str:
    #     return identifier.lower()

    # def default_casify_type_name(self, identifier: str) -> str:
    #     return identifier.lower()

    # def expr_percentile(self, expr: str, percentile: float):
    #     return f"approx_percentile({expr}, {percentile})"

    # def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: Optional[List[object]], expr: str):
    #     # TODO add all of these postgres specific statistical aggregate functions: https://www.postgresql.org/docs/9.6/functions-aggregate.html
    #     if metric_name in [
    #         "stddev",
    #         "stddev_pop",
    #         "stddev_samp",
    #         "variance",
    #         "var_pop",
    #         "var_samp",
    #     ]:
    #         return f"{metric_name.upper()}({expr})"
    #     return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    # @classmethod
    # def get_time_unit_sql(cls, time_unit: TimeUnit) -> str:
    #     return time_unit.name

    # @classmethod
    # def get_time_delta_sql(cls, start: str, end: str, time_unit: TimeUnit, time_count: int = 1) -> str:
    #     secs_per_interval = convert_to_timedelta(time_unit, time_count).total_seconds()
    #     return f"cast(floor(date_diff('second', {start}, {end}) / {secs_per_interval}) as int)"

    # @classmethod
    # def get_interval_sql(cls, interval_unit: TimeUnit, interval_count: int | str) -> str:
    #     """Get the SQL syntax for an interval"""
    #     # Opting to multiply the interval in case `interval_count` refers to a column
    #     return f"INTERVAL '1' {cls.get_time_unit_sql(interval_unit)} * ({interval_count})"

    # @classmethod
    # def get_add_interval_sql(cls, timestamp: str, interval_unit: TimeUnit, interval_count: int | str) -> str:
    #     return f"date_add('{cls.get_time_unit_sql(interval_unit)}', {interval_count}, {timestamp})"

    # def sql_paginate_query(self, query: str, offset: int, page_size: int) -> str:
    #     return f"{query} \n OFFSET {offset} LIMIT {page_size}"

    # def expr_char_length(self, expr):
    #     return f"LENGTH({expr})"

    # def cast_to_float(self, expr: str) -> str:
    #     return f"CAST({expr} AS DOUBLE)"
