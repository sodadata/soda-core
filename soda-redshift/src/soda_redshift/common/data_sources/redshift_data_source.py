import logging

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.sql_ast import COLUMN, COUNT, DISTINCT, REGEX_LIKE, TUPLE, VALUES
from soda_core.common.sql_dialect import SqlDialect
from soda_redshift.common.data_sources.redshift_data_source_connection import (
    RedshiftDataSource as RedshiftDataSourceModel,
)
from soda_redshift.common.data_sources.redshift_data_source_connection import (
    RedshiftDataSourceConnection,
)

logger: logging.Logger = soda_logger


REDSHIFT_DOUBLE_PRECISION = "double precision"
REDSHIFT_CHARACTER_VARYING = "character varying"


class RedshiftDataSourceImpl(DataSourceImpl, model_class=RedshiftDataSourceModel):
    def __init__(self, data_source_model: RedshiftDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return RedshiftSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return RedshiftDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class RedshiftSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        return {
            SodaDataTypeName.CHAR: "char",
            SodaDataTypeName.VARCHAR: "varchar",
            SodaDataTypeName.TEXT: "varchar",  # Redshift treats text as varchar(max)
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.NUMERIC: "numeric",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.FLOAT: "real",  # Redshift float = real (float4)
            SodaDataTypeName.DOUBLE: REDSHIFT_DOUBLE_PRECISION,  # Redshift uses double precision (float8)
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamptz",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            # Character types
            "char": SodaDataTypeName.CHAR,
            "character": SodaDataTypeName.CHAR,
            "nchar": SodaDataTypeName.CHAR,
            "varchar": SodaDataTypeName.VARCHAR,
            REDSHIFT_CHARACTER_VARYING: SodaDataTypeName.VARCHAR,
            "nvarchar": SodaDataTypeName.VARCHAR,
            "text": SodaDataTypeName.TEXT,  # synonym, stored as varchar(max)
            # Integer types
            "smallint": SodaDataTypeName.SMALLINT,
            "int2": SodaDataTypeName.SMALLINT,
            "integer": SodaDataTypeName.INTEGER,
            "int": SodaDataTypeName.INTEGER,
            "int4": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "int8": SodaDataTypeName.BIGINT,
            # Exact numeric types
            "numeric": SodaDataTypeName.NUMERIC,
            "decimal": SodaDataTypeName.DECIMAL,
            # Approximate numeric types
            "real": SodaDataTypeName.FLOAT,  # float4
            "float4": SodaDataTypeName.FLOAT,
            REDSHIFT_DOUBLE_PRECISION: SodaDataTypeName.DOUBLE,  # float8
            "float8": SodaDataTypeName.DOUBLE,
            "float": SodaDataTypeName.DOUBLE,  # synonym for float8
            # Date/time types
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp without time zone": SodaDataTypeName.TIMESTAMP,
            "timestamptz": SodaDataTypeName.TIMESTAMP_TZ,
            "timestamp with time zone": SodaDataTypeName.TIMESTAMP_TZ,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "time without time zone": SodaDataTypeName.TIME,
            # Boolean type
            "boolean": SodaDataTypeName.BOOLEAN,
            "bool": SodaDataTypeName.BOOLEAN,
        }

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["varchar", REDSHIFT_CHARACTER_VARYING],
            ["char", "character"],
            ["smallint", "int2"],
            ["integer", "int", "int4"],
            ["bigint", "int8"],
            ["real", "float4", "float"],
            [REDSHIFT_DOUBLE_PRECISION, "float8"],
            ["timestamp", "timestamp without time zone"],
            ["time", "time without time zone"],
        ]

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"{expression} ~ '{matches.regex_pattern}'"

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        if tuple.check_context(VALUES):
            return f"{','.join(self.build_expression_sql(e) for e in tuple.expressions)}"
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        """
        Redshift does not support DISTINCT on tuples and has nothing like BigQuery's TO_JSON_STRING(STRUCT).

        Instead we approximate TO_JSON_STRING(STRUCT) by concatting all the columns.
        """

        def format_element_expression(e: str) -> str:
            """Use CHR(31) (unit seperator) as delimiter because it is highly unlikely to be appear in the data.
            If it does appear, replace with string '#US#'.  Also replace NULL with a string value to prevent
            cascading NULLS in the concat operation."""
            return f"REPLACE(COALESCE(CAST({e} AS VARCHAR), '__UNDEF__'), CHR(31), '#US#')"

        concat_delim = " || CHR(31) || \n"  # use ASCII unit separator as delimieter
        elements: str = concat_delim.join(
            format_element_expression(self.build_expression_sql(e)) for e in tuple.expressions
        )
        # Use FNV_HASH to convert the string rep into a hash value with a fixed length, will be more performant in COUNT DISTINCT
        return f"FNV_HASH({elements})"

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return "\nUNION ALL\n".join(["SELECT " + self.build_expression_sql(value) for value in values.values])

    def data_type_has_parameter_character_maximum_length(self, data_type_name) -> bool:
        return data_type_name.lower() in ["varchar", "char", REDSHIFT_CHARACTER_VARYING, "character"]

    def data_type_has_parameter_numeric_precision(self, data_type_name) -> bool:
        return data_type_name.lower() in ["numeric", "number", "decimal"]

    def data_type_has_parameter_numeric_scale(self, data_type_name) -> bool:
        return data_type_name.lower() in ["numeric", "number", "decimal"]

    def data_type_has_parameter_datetime_precision(self, data_type_name) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def get_synonyms_for_soda_data_type(self) -> list[list[SodaDataTypeName]]:
        return [
            [SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR],
            [SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL],
        ]
