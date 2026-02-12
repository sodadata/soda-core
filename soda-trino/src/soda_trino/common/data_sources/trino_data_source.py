import logging
from typing import Optional
from datetime import datetime

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger

from soda_core.common.metadata_types import (
    SodaDataTypeName,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSource as TrinoDataSourceModel,
)
from soda_trino.common.data_sources.trino_data_source_connection import (
    TrinoDataSourceConnection,
)

from soda_core.common.sql_ast import (
    CREATE_TABLE,
    CREATE_TABLE_IF_NOT_EXISTS,
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


class TrinoSqlDialect(SqlDialect):    
    USES_SEMICOLONS_BY_DEFAULT: bool = False
    SUPPORTS_DROP_TABLE_CASCADE: bool = False

    def supports_materialized_views(self) -> bool:
        return True

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [["INTEGER", "INT"]]

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[SodaDataTypeName, str]:
        
        return {
            SodaDataTypeName.CHAR: "CHAR",
            SodaDataTypeName.VARCHAR: "VARCHAR",
            SodaDataTypeName.TEXT: "VARCHAR",
            SodaDataTypeName.SMALLINT: "SMALLINT",
            SodaDataTypeName.INTEGER: "INTEGER",
            SodaDataTypeName.DECIMAL: "DECIMAL",
            SodaDataTypeName.BIGINT: "BIGINT",
            SodaDataTypeName.NUMERIC: "NUMERIC",
            SodaDataTypeName.DECIMAL: "NUMERIC",
            SodaDataTypeName.FLOAT: "REAL",
            SodaDataTypeName.DOUBLE: "DOUBLE",
            SodaDataTypeName.TIMESTAMP: "TIMESTAMP",
            SodaDataTypeName.TIMESTAMP_TZ: "TIMESTAMP WITH TIME ZONE",
            SodaDataTypeName.DATE: "DATE",
            SodaDataTypeName.TIME: "TIME",
            SodaDataTypeName.BOOLEAN: "BOOLEAN",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "BOOLEAN": SodaDataTypeName.BOOLEAN,
            "TINYINT": SodaDataTypeName.SMALLINT,
            "SMALLINT": SodaDataTypeName.SMALLINT,
            "INTEGER": SodaDataTypeName.INTEGER,
            "BIGINT": SodaDataTypeName.BIGINT,
            "REAL": SodaDataTypeName.FLOAT,
            "DOUBLE": SodaDataTypeName.DOUBLE,
            "DECIMAL": SodaDataTypeName.DECIMAL,
            "VARCHAR": SodaDataTypeName.VARCHAR,
            "CHAR": SodaDataTypeName.CHAR,
            "TEXT": SodaDataTypeName.VARCHAR,
            "DATE": SodaDataTypeName.DATE,
            "TIME": SodaDataTypeName.TIME,
            "TIMESTAMP": SodaDataTypeName.TIMESTAMP,
            "TIMESTAMP WITH TIME ZONE": SodaDataTypeName.TIMESTAMP_TZ,
        }

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return False

    def data_type_has_parameter_numeric_precision(self, data_type_name) -> bool:
        return False

    def data_type_has_parameter_numeric_scale(self, data_type_name) -> bool:
        return False

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return False

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = False) -> str:
        # default False for T
        return super().create_schema_if_not_exists_sql(prefixes, add_semicolon=add_semicolon)



    #def cast_to_float(self, expr: str) -> str:
    # def literal_datetime(self, datetime: datetime):
    #     if datetime.tzinfo is None:
    #         return f"TIMESTAMP '{datetime.strftime('%Y-%m-%d %H:%M:%S')}'"
    #     else:
    #         return f"TIMESTAMP '{datetime.strftime('%Y-%m-%d %H:%M:%S%z')}'"

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