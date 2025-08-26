from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.metadata_types import SodaDataTypeNames
from soda_core.common.sql_ast import COLUMN, COUNT, DISTINCT, REGEX_LIKE, TUPLE, VALUES
from soda_core.common.sql_dialect import SqlDialect
from soda_redshift.common.data_sources.redshift_data_source_connection import (
    RedshiftDataSource as RedshiftDataSourceModel,
)
from soda_redshift.common.data_sources.redshift_data_source_connection import (
    RedshiftDataSourceConnection,
)


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

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"{expression} ~ '{matches.regex_pattern}'"

    def default_varchar_length(self) -> Optional[int]:
        return 255

    def get_sql_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        base_dict = super().get_sql_data_type_name_by_soda_data_type_names()
        base_dict[SodaDataTypeNames.TEXT] = f"character varying({self.default_varchar_length()})"
        return base_dict

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
