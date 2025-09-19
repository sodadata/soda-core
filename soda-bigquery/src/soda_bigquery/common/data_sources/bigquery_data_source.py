import logging
from dataclasses import dataclass
from typing import Optional

from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSource as BigQueryDataSourceModel,
)
from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSourceConnection,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import DataSourceNamespace, SodaDataTypeName
from soda_core.common.sql_ast import (
    COLUMN,
    COUNT,
    DISTINCT,
    LITERAL,
    REGEX_LIKE,
    TUPLE,
    VALUES,
    WITH,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery

logger: logging.Logger = soda_logger


@dataclass
class BigQueryDataSourceNamespace(DataSourceNamespace):
    project_id: str
    dataset: str
    location: Optional[str] = None

    def get_namespace_elements(self) -> list[str]:
        if self.location:
            return [self.location, self.project_id]
        else:
            return [self.project_id, self.dataset]

    def get_database_for_metadata_query(self) -> str | None:
        return self.project_id

    def get_schema_for_metadata_query(self) -> str:
        return self.dataset


class BigQueryDataSourceImpl(DataSourceImpl, model_class=BigQueryDataSourceModel):
    def __init__(self, data_source_model: BigQueryDataSourceModel):
        super().__init__(data_source_model=data_source_model)
        self.cached_location = None

    def _create_sql_dialect(self) -> SqlDialect:
        return BigQuerySqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return BigQueryDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    def get_location(self) -> str:
        if self.cached_location is not None:
            location = self.cached_location
        elif self.data_source_model.connection_properties.location is not None:
            location = self.data_source_model.connection_properties.location
        else:
            result = self.execute_query("SELECT @@location")
            location = result.rows[0][0]
            logger.info(f"Detected BigQuery location: {location}")
            self.cached_location = location
        return location

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        super_metadata_tables_query = MetadataTablesQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            prefixes=[f"region-{self.get_location()}"],
        )
        return super_metadata_tables_query

    def build_columns_metadata_query_str(self, dataset_prefixes: list[str], dataset_name: str) -> str:
        table_namespace: DataSourceNamespace = BigQueryDataSourceNamespace(
            project_id=dataset_prefixes[0], dataset=dataset_prefixes[1]
        )

        # BigQuery must be able to override to get the location
        return self.sql_dialect.build_columns_metadata_query_str(
            table_namespace=table_namespace, table_name=dataset_name
        )


class BigQuerySqlDialect(SqlDialect):
    DEFAULT_QUOTE_CHAR = "`"

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        return {
            SodaDataTypeName.CHAR: "string",  # BigQuery only has STRING
            SodaDataTypeName.VARCHAR: "string",
            SodaDataTypeName.TEXT: "string",  # alias for varchar
            SodaDataTypeName.SMALLINT: "int64",  # all integers → INT64
            SodaDataTypeName.INTEGER: "int64",
            SodaDataTypeName.BIGINT: "int64",
            SodaDataTypeName.DECIMAL: "numeric",  # NUMERIC (fixed precision: 38 digits, 9 decimals)
            SodaDataTypeName.NUMERIC: "numeric",
            SodaDataTypeName.FLOAT: "float64",  # FLOAT & DOUBLE → FLOAT64
            SodaDataTypeName.DOUBLE: "float64",
            SodaDataTypeName.TIMESTAMP: "timestamp",  # UTC-based
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",  # still just TIMESTAMP in BigQuery
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "bool",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "string": SodaDataTypeName.VARCHAR,
            "smallint": SodaDataTypeName.SMALLINT,
            "int64": SodaDataTypeName.BIGINT,
            "numeric": SodaDataTypeName.NUMERIC,
            "float64": SodaDataTypeName.DOUBLE,
            "double precision": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "bool": SodaDataTypeName.BOOLEAN,
        }

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["int64", "integer"],
            ["bool", "boolean"],
            ["numeric", "decimal"],
            ["bignumeric", "bigdecimal"],
        ]

    def information_schema_namespace_elements(self, data_source_namespace: BigQueryDataSourceNamespace) -> list[str]:
        return [data_source_namespace.project_id, data_source_namespace.dataset, "INFORMATION_SCHEMA"]

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"TO_JSON_STRING(STRUCT({super()._build_tuple_sql(tuple)}))"

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"REGEXP_CONTAINS({expression}, r'{matches.regex_pattern}')"

    def supports_data_type_character_maximum_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return False

    def supports_data_type_numeric_scale(self) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp('{datetime_in_iso8601}')"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc(timestamp({timestamp_literal}), day)"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval 1 day"

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        # The first select row should have column aliases
        # Remaining rows don't need aliases
        def build_literal_with_alias(literal, alias: COLUMN | None) -> str:
            return f"{self.literal(literal)} AS {self.quote_column(alias.name)}"

        literal_rows: list[str] = []
        for tuple in values.values:
            if alias_columns:
                literal_sqls: list[str] = []
                for i in range(len(tuple.expressions)):
                    literal: LITERAL = tuple.expressions[i]
                    alias: COLUMN = alias_columns[i]
                    literal_sqls.append(build_literal_with_alias(literal=literal, alias=alias))
                literal_rows.append(", ".join(literal_sql for literal_sql in literal_sqls))
                alias_columns = None
            else:
                literal_rows.append(", ".join(self.build_expression_sql(e) for e in tuple.expressions))

        select_rows: list[str] = [f"SELECT {literal_row}" for literal_row in literal_rows]

        return "\nUNION ALL ".join([select_row for select_row in select_rows])

    def _build_cte_with_sql_line(self, with_element: WITH) -> str:
        return f"WITH {self.quote_default(with_element.alias)} AS ("

    def is_same_soda_data_type(self, expected: SodaDataTypeName, actual: SodaDataTypeName) -> bool:
        list_of_text_synonyms = [SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR]
        list_of_integer_synonyms = [SodaDataTypeName.INTEGER, SodaDataTypeName.BIGINT, SodaDataTypeName.SMALLINT]
        list_of_numeric_synonyms = [SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL]
        list_of_timestamp_synonyms = [
            SodaDataTypeName.TIMESTAMP,
            SodaDataTypeName.TIMESTAMP_TZ,
        ]  # Bigquery does not support timezones
        list_of_float_synonyms = [SodaDataTypeName.FLOAT, SodaDataTypeName.DOUBLE]

        found_synonym = False
        synonym_correct = False

        for list_of_synonyms in [
            list_of_text_synonyms,
            list_of_integer_synonyms,
            list_of_numeric_synonyms,
            list_of_timestamp_synonyms,
            list_of_float_synonyms,
        ]:
            if expected in list_of_synonyms or actual in list_of_synonyms:
                (found_synonym, synonym_correct) = (
                    True,
                    actual in list_of_synonyms and expected in list_of_synonyms,
                )
                break

        if found_synonym and synonym_correct:
            if expected != actual:
                logger.debug(f"In is_same_soda_data_type, Expected {expected} and actual {actual} are the same")
            return True
        else:
            return super().is_same_soda_data_type(expected, actual)
