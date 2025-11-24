from datetime import datetime
from logging import Logger
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.datetime_conversions import (
    convert_datetime_to_str,
    convert_str_to_datetime,
)
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName
from soda_core.common.sql_ast import CAST, REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_dremio.common.data_sources.dremio_data_source_connection import (
    DremioDataSource as DremioDataSourceModel,
)
from soda_dremio.common.data_sources.dremio_data_source_connection import (
    DremioDataSourceConnection,
)

logger: Logger = soda_logger

CHARACTER_VARYING="character varying"  # constant used to avoid triggering Sonar duplicate check

class DremioDataSourceImpl(DataSourceImpl, model_class=DremioDataSourceModel):
    def __init__(self, data_source_model: DremioDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        return DremioSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return DremioDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class DremioSqlDialect(SqlDialect):
    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.DOUBLE, SodaDataTypeName.FLOAT),
        (SodaDataTypeName.CHAR, SodaDataTypeName.VARCHAR, SodaDataTypeName.TEXT),
        (SodaDataTypeName.SMALLINT, SodaDataTypeName.INTEGER),
        (SodaDataTypeName.TIMESTAMP, SodaDataTypeName.TIMESTAMP_TZ),
    )

    def __init__(self):
        super().__init__()    

    def get_database_prefix_index(self) -> int | None:
        return None

    def get_schema_prefix_index(self) -> int | None:
        return 0

    def default_varchar_length(self) -> Optional[int]:
        return 65535

    def supports_data_type_character_maximum_length(self) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def supports_datetime_microseconds(self) -> bool:
        return False

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        assert len(prefixes) == 1, f"Expected 1 prefix, got {len(prefixes)}"
        schema_name: str = prefixes[0]
        quoted_schema_name: str = ".".join([self.quote_default(s) for s in schema_name.split(".")])
        return f"CREATE FOLDER IF NOT EXISTS {quoted_schema_name}" + (";" if add_semicolon else "")

    def qualify_dataset_name(self, dataset_prefix: list[str], dataset_name: str) -> str:
        """
        Creates a fully qualified table name, optionally quoting the table name
        """
        assert len(dataset_prefix) == 1, f"Expected 1 prefix, got {len(dataset_prefix)}"
        schema: str = dataset_prefix[0]
        parts: list[str] = schema.split(".") + [dataset_name]
        parts = [self.quote_default(p) for p in parts if p]
        return ".".join(parts)

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_LIKE({expression}, '{matches.regex_pattern}')"

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[SodaDataTypeName, str]:
        return {
            SodaDataTypeName.CHAR: CHARACTER_VARYING,   # constant used to avoid triggering Sonar duplicate check
            SodaDataTypeName.VARCHAR: CHARACTER_VARYING,
            SodaDataTypeName.TEXT: CHARACTER_VARYING,
            SodaDataTypeName.INTEGER: "integer",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.SMALLINT: "integer",
            SodaDataTypeName.NUMERIC: "decimal",
            SodaDataTypeName.FLOAT: "float",
            SodaDataTypeName.DOUBLE: "double",
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            CHARACTER_VARYING: SodaDataTypeName.VARCHAR,
            "integer": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            "float": SodaDataTypeName.FLOAT,
            "double": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "boolean": SodaDataTypeName.BOOLEAN,
        }

    def _build_cast_sql(self, cast: CAST) -> str:
        to_type_text: str = (
            self.get_data_source_data_type_name_for_soda_data_type_name(cast.to_type)
            if isinstance(cast.to_type, SodaDataTypeName)
            else cast.to_type
        )
        return f"CAST({self.build_expression_sql(cast.expression)} AS {to_type_text})"

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["varchar", CHARACTER_VARYING],
            ["int", "integer"],
            ["decimal", "numeric"],
        ]

    def literal_datetime(self, datetime: datetime):
        # Dremio doesn't like datetime.isoformat()
        return f"'{datetime.strftime('%Y-%m-%d %H:%M:%S')}'"

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        # Dremio doesn't support ISO8601 format with 'T' separator
        # Convert from '2025-01-21T12:34:56+00:00' to '2025-01-21 12:34:56+00:00'
        # This also preserves variable placeholders like '${soda.NOW}' unchanged
        datetime_str = datetime_in_iso8601.replace("T", " ")
        return f"timestamp '{datetime_str}'"

    def literal_datetime_with_tz(self, datetime: datetime):
        # Dremio doesn't support timezones
        return self.literal_datetime(datetime)

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"TIMESTAMPADD(DAY, 1, {timestamp_literal})"

    def convert_str_to_datetime(self, datetime_str: str) -> datetime:
        # format will be '2025-01-21 12:34:56+00:00'
        # convert to iso format '2025-01-21T12:34:56+00:00'
        datetime_iso_str = datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S%z").isoformat()
        return convert_str_to_datetime(datetime_iso_str)

    def convert_datetime_to_str(self, datetime: datetime) -> str:
        datetime_iso_str: str = convert_datetime_to_str(datetime)
        # convert to dremio format '2025-01-21 12:34:56 (no T, no timezone)'
        # remove final 6 characters to strip timezone offset
        return datetime_iso_str.replace("T", " ")[:-6]

    def _get_add_column_sql_expr(self) -> str:
        """Dremio uses ADD COLUMNS (plural) instead of ADD COLUMN"""
        return "ADD COLUMNS"

    def _build_alter_table_add_column_sql(self, alter_table, add_semicolon: bool = True, add_paranthesis: bool = False) -> str:
        """Override to use parentheses required by Dremio's ADD COLUMNS syntax"""
        return super()._build_alter_table_add_column_sql(alter_table, add_semicolon, add_paranthesis=True)
