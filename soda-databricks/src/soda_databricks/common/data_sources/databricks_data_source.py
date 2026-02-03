from logging import Logger
from typing import Any, Optional, Tuple

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import (
    ColumnMetadata,
    DataSourceNamespace,
    SodaDataTypeName,
)
from soda_core.common.sql_ast import (
    ALTER_TABLE_ADD_COLUMN,
    ALTER_TABLE_DROP_COLUMN,
    CREATE_TABLE_COLUMN,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.statements.table_types import TableType
from soda_databricks.common.data_sources.databricks_data_source_connection import (
    DatabricksDataSourceConnection,
)
from soda_databricks.common.statements.hive_metadata_tables_query import (
    HiveMetadataTablesQuery,
)
from soda_databricks.model.data_source.databricks_data_source import (
    DatabricksDataSource as DatabricksDataSourceModel,
)

logger: Logger = soda_logger


class DatabricksDataSourceImpl(DataSourceImpl, model_class=DatabricksDataSourceModel):
    def __init__(self, data_source_model: DatabricksDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)

    def _create_sql_dialect(self) -> SqlDialect:
        if self.__is_hive_catalog():
            return DatabricksHiveSqlDialect(data_source_impl=self)
        return DatabricksSqlDialect(data_source_impl=self)

    def _create_data_source_connection(self) -> DataSourceConnection:
        return DatabricksDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        if self.__is_hive_catalog():
            return HiveMetadataTablesQuery(
                sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection
            )
        else:
            return super().create_metadata_tables_query()

    def __is_hive_catalog(self):
        # Check the connection "catalog"
        catalog: Optional[str] = self.data_source_model.connection_properties.catalog
        if catalog and catalog.lower() == "hive_metastore":
            return True
        # All other catalogs should be treated as "unity catalogs"
        return False

    def get_columns_metadata(self, dataset_prefixes: list[str], dataset_name: str) -> list[ColumnMetadata]:
        try:
            return super().get_columns_metadata(dataset_prefixes, dataset_name)
        except Exception as e:
            logger.warning(f"Error getting columns metadata for {dataset_name}: {e}\n\nReturning empty list.")
            return []

    def test_schema_exists(self, prefixes: list[str]) -> bool:
        if not self.__is_hive_catalog():
            return super().test_schema_exists(prefixes)

        schema_name: str = prefixes[1]

        result = self.execute_query(
            f"SHOW SCHEMAS LIKE '{schema_name}'"
        ).rows  # We only need to check the schema name, as the catalog name is always the same
        for row in result:
            if row[0] and row[0].lower() == schema_name.lower():
                return True
        return False


class DatabricksSqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.TIMESTAMP_TZ, SodaDataTypeName.TIMESTAMP),
    )

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["int", "integer"],
        ]

    def column_data_type(self) -> str:
        return self.default_casify("data_type")

    def supports_data_type_character_maximum_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return True

    def supports_data_type_numeric_scale(self) -> bool:
        return True

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict:
        return {
            SodaDataTypeName.CHAR: "string",
            SodaDataTypeName.VARCHAR: "string",
            SodaDataTypeName.TEXT: "string",
            SodaDataTypeName.SMALLINT: "smallint",
            SodaDataTypeName.INTEGER: "int",
            SodaDataTypeName.BIGINT: "bigint",
            SodaDataTypeName.NUMERIC: "decimal",
            SodaDataTypeName.DECIMAL: "decimal",
            SodaDataTypeName.FLOAT: "float",
            SodaDataTypeName.DOUBLE: "double",
            SodaDataTypeName.TIMESTAMP: "timestamp",
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "string",  # no native TIME type in Databricks
            SodaDataTypeName.BOOLEAN: "boolean",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "string": SodaDataTypeName.TEXT,
            "varchar": SodaDataTypeName.VARCHAR,
            "char": SodaDataTypeName.CHAR,
            "tinyint": SodaDataTypeName.SMALLINT,
            "short": SodaDataTypeName.SMALLINT,
            "smallint": SodaDataTypeName.SMALLINT,
            "int": SodaDataTypeName.INTEGER,
            "integer": SodaDataTypeName.INTEGER,
            "bigint": SodaDataTypeName.BIGINT,
            "long": SodaDataTypeName.BIGINT,
            "decimal": SodaDataTypeName.DECIMAL,
            "numeric": SodaDataTypeName.NUMERIC,
            "float": SodaDataTypeName.FLOAT,
            "real": SodaDataTypeName.FLOAT,
            "float4": SodaDataTypeName.FLOAT,
            "double": SodaDataTypeName.DOUBLE,
            "double precision": SodaDataTypeName.DOUBLE,
            "float8": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "timestamp without time zone": SodaDataTypeName.TIMESTAMP,
            "timestamp_ntz": SodaDataTypeName.TIMESTAMP,  # If there is explicitly stated that the timestamp is without time zone, we consider it to be the same as TIMESTAMP
            "timestamptz": SodaDataTypeName.TIMESTAMP_TZ,
            "timestamp with time zone": SodaDataTypeName.TIMESTAMP_TZ,
            "date": SodaDataTypeName.DATE,
            "boolean": SodaDataTypeName.BOOLEAN,
            # Not supported -> will be converted to varchar
            # "binary"
            # "interval",
            # "array",
            # "map",
            # "struct"
        }

    def escape_string(self, value: str):
        raw_string = rf"{value}"
        string_literal: str = raw_string.replace(r"'", r"\'")
        return string_literal

    def encode_string_for_sql(self, string: str) -> str:
        """This escapes values that contain newlines correctly."""
        # For databricks, we don't need to encode the string, it's able to handle the newlines correctly.
        # In fact, when we encode the string, we run into issues with the escape characters for the quotes
        return string

    def column_data_type_numeric_scale(self) -> str:
        return self.default_casify("numeric_scale")

    def column_data_type_datetime_precision(self) -> str:
        return self.default_casify("datetime_precision")

    def _build_create_table_column_type(self, create_table_column: CREATE_TABLE_COLUMN) -> str:
        # Databricks will complain if string lengths or datetime precisions are passed in, so strip if they are provided
        if create_table_column.type.name == "string":
            create_table_column.type.character_maximum_length = None
        if create_table_column.type.name in ["timestamp_ntz", "timestamp"]:
            create_table_column.type.datetime_precision = None
        # Double does not support parameters, so we need to strip them.
        if create_table_column.type.name == "double":
            create_table_column.type.numeric_precision = None
            create_table_column.type.numeric_scale = None
        # Timestamp does not support parameters, so we need to strip them.
        if create_table_column.type.name == "timestamp":
            # We should clear all precisions (including datetime_precision, character_maximum_length, numeric_precision, numeric_scale).
            # If we don't, this could lead to a scenario when mapping types from another data source to Databricks whereby we still create `timestamp(3)` instead of `timestamp`.
            create_table_column.type.datetime_precision = None
            create_table_column.type.character_maximum_length = None
            create_table_column.type.numeric_precision = None
            create_table_column.type.numeric_scale = None
        return super()._build_create_table_column_type(create_table_column=create_table_column)

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["varchar", "char", "string"],
            ["smallint", "int2"],
            ["integer", "int", "int4"],
            ["bigint", "int8"],
            ["real", "float4", "float"],
            ["double precision", "float8", "double"],
            ["timestamp", "timestamp without time zone"],
            ["timestamptz", "timestamp with time zone"],
            ["time", "time without time zone"],
        ]

    # Explicitly leaving this here for reference. See comment below why we moved to DESCRIBE TABLE.
    # def build_columns_metadata_query_str(self, table_namespace: DataSourceNamespace, table_name: str) -> str:
    # Unity catalog only stores things in lower case,
    # even though create table may have been quoted and with mixed case
    # table_name_lower: str = table_name.lower()
    # return super().build_columns_metadata_query_str(table_namespace, table_name_lower)

    # We move to DESCRIBE TABLE, that is a more up-to-date way to get the columns metadata. (information_schema is lagging behind sometimes, and does not always return the correct columns)
    def build_columns_metadata_query_str(self, table_namespace: DataSourceNamespace, table_name: str) -> str:
        database_name: str | None = table_namespace.get_database_for_metadata_query()
        schema_name: str = table_namespace.get_schema_for_metadata_query()
        return f"DESCRIBE {database_name}.{schema_name}.{table_name}"

    def build_column_metadatas_from_query_result(self, query_result: QueryResult) -> list[ColumnMetadata]:
        # Filter out dataset description rows (first such line starts with #, ignore the rest) or empty
        filtered_rows = []
        for row in query_result.rows:
            if row[0].startswith("#"):  # ignore all description rows
                break
            if not row[0] and not row[1]:  # empty row
                continue

            filtered_rows.append(row)

        return super().build_column_metadatas_from_query_result(
            QueryResult(rows=filtered_rows, columns=query_result.columns)
        )

    def extract_data_type_name(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> str:
        data_type_name: str = row[1]
        # Some data types have parameters, like decimal(10,0). We need to strip the parameters.
        if "(" in data_type_name:
            data_type_name = data_type_name[: data_type_name.index("(")].strip()
        return data_type_name

    def extract_numeric_precision(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        # We just need the precision, and it's formatted like: decimal(10,0) -> 10
        data_type_name: str = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_precision(data_type_name):
            return None
        return int(row[1].split("(")[1].split(",")[0])

    def extract_numeric_scale(self, row: Tuple[Any, ...], columns: list[Tuple[Any, ...]]) -> Optional[int]:
        # We just need the scale, and it's formatted like: decimal(10,0) -> 0
        data_type_name: str = self.extract_data_type_name(row, columns)
        if not self.data_type_has_parameter_numeric_scale(data_type_name):
            return None
        return int(row[1].split(",")[1].strip(")"))

    def post_schema_create_sql(self, prefixes: list[str]) -> Optional[list[str]]:
        assert len(prefixes) == 2, f"Expected 2 prefixes, got {len(prefixes)}"
        catalog_name: str = self.quote_default(prefixes[0])
        schema_name: str = self.quote_default(prefixes[1])
        return [f"GRANT SELECT, USAGE, CREATE, MANAGE ON SCHEMA {catalog_name}.{schema_name} TO `account users`;"]
        #      f"GRANT SELECT ON FUTURE TABLES IN SCHEMA {catalog_name}.{schema_name} TO `account users`;"]

    @classmethod
    def is_same_soda_data_type_with_synonyms(cls, expected: SodaDataTypeName, actual: SodaDataTypeName) -> bool:
        # Special case of a 1-way synonym: TEXT is allowed where TIME is expected
        if expected == SodaDataTypeName.TIME and actual == SodaDataTypeName.TEXT:
            logger.debug(
                f"In is_same_soda_data_type_with_synonyms, Expected {expected} and actual {actual} are the same"
            )
            return True
        return super().is_same_soda_data_type_with_synonyms(expected, actual)

    def _build_alter_table_add_column_sql(
        self, alter_table: ALTER_TABLE_ADD_COLUMN, add_semicolon: bool = True, add_parenthesis: bool = False
    ) -> str:
        return super()._build_alter_table_add_column_sql(alter_table, add_semicolon=add_semicolon, add_parenthesis=True)

    def _get_add_column_sql_expr(self) -> str:
        return "ADD COLUMNS"

    def _build_alter_table_drop_column_sql(
        self, alter_table: ALTER_TABLE_DROP_COLUMN, add_semicolon: bool = True
    ) -> str:
        column_name_quoted: str = self._quote_column_for_create_table(alter_table.column_name)
        return f"ALTER TABLE {alter_table.fully_qualified_table_name} DROP COLUMNS ({column_name_quoted})" + (
            ";" if add_semicolon else ""
        )

    def drop_column_supported(self) -> bool:
        return False  # Note, this is technically supported. But we need to change the delta table mapping mode name for this (out of scope at the time of writing)

    def convert_table_type_to_enum(self, table_type: str) -> TableType:
        if table_type == "MANAGED":
            return TableType.TABLE
        elif table_type == "VIEW":
            return TableType.VIEW
        elif table_type == "MATERIALIZED_VIEW":
            return TableType.VIEW  # For now, a materialized view is treated as a view.
        else:
            # Default to TABLE if the table type is not recognized (so we're backwards compatible with existing code)
            logger.warning(f"Invalid table type: {table_type}, defaulting to TABLE")
            return TableType.TABLE

    def metadata_casify(self, identifier: str) -> str:
        return identifier.lower()


class DatabricksHiveSqlDialect(DatabricksSqlDialect):
    def post_schema_create_sql(self, prefixes: list[str]) -> Optional[list[str]]:
        assert len(prefixes) == 2, f"Expected 2 prefixes, got {len(prefixes)}"
        catalog_name: str = self.quote_default(prefixes[0])
        schema_name: str = self.quote_default(prefixes[1])

        return [f"GRANT SELECT, USAGE, CREATE ON SCHEMA {catalog_name}.{schema_name} TO `users`;"]
