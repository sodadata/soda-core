from logging import Logger
from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import SodaDataTypeName, SqlDataType
from soda_core.common.sql_ast import CAST, REGEX_LIKE
from soda_core.common.sql_dialect import SqlDialect
from soda_dremio.common.data_sources.dremio_data_source_connection import (
    DremioDataSource as DremioDataSourceModel,
)
from soda_dremio.common.data_sources.dremio_data_source_connection import (
    DremioDataSourceConnection,
)

logger: Logger = soda_logger


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
    )

    

    def __init__(self):
        super().__init__()

    # def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
    #     # Dremio supports CREATE SCHEMA IF NOT EXISTS
    #     # Format: CREATE SCHEMA IF NOT EXISTS source.schema_name
    #     schema_name = ".".join(self.quote_default(prefix) for prefix in prefixes if prefix is not None)
    #     return f"CREATE SCHEMA IF NOT EXISTS {schema_name}" + (";" if add_semicolon else "")

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
            SodaDataTypeName.CHAR: "CHARACTER VARYING",
            SodaDataTypeName.VARCHAR: "CHARACTER VARYING",
            SodaDataTypeName.TEXT: "CHARACTER VARYING",
            SodaDataTypeName.INTEGER: "INT",
            SodaDataTypeName.DECIMAL: "DECIMAL",
            SodaDataTypeName.BIGINT: "BIGINT",
            SodaDataTypeName.NUMERIC: "DECIMAL",
            SodaDataTypeName.FLOAT: "FLOAT",
            SodaDataTypeName.DOUBLE: "DOUBLE",
            SodaDataTypeName.TIMESTAMP: "TIMESTAMP",
            SodaDataTypeName.TIMESTAMP_TZ: "TIMESTAMP",
            SodaDataTypeName.DATE: "DATE",
            SodaDataTypeName.TIME: "TIME",
            SodaDataTypeName.BOOLEAN: "BOOLEAN",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "CHARACTER VARYING": SodaDataTypeName.VARCHAR,
            "INT": SodaDataTypeName.INTEGER,
            "BIGINT": SodaDataTypeName.BIGINT,
            "DECIMAL": SodaDataTypeName.DECIMAL,
            "NUMERIC": SodaDataTypeName.NUMERIC,
            "FLOAT": SodaDataTypeName.FLOAT,
            "DOUBLE": SodaDataTypeName.DOUBLE,
            "TIMESTAMP": SodaDataTypeName.TIMESTAMP,
            "DATE": SodaDataTypeName.DATE,
            "TIME": SodaDataTypeName.TIME,
            "BOOLEAN": SodaDataTypeName.BOOLEAN,
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
            ["varchar", "character varying"],
            ["char", "character"],
            ["int", "integer"],
            ["decimal", "numeric"],
        ]
