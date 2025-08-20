from typing import Optional

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import COUNT, DISTINCT, TUPLE, VALUES
from soda_core.common.sql_dialect import DBDataType, SqlDialect
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSource as SnowflakeDataSourceModel,
)
from soda_snowflake.common.data_sources.snowflake_data_source_connection import (
    SnowflakeDataSourceConnection,
)


class SnowflakeDataSourceImpl(DataSourceImpl, model_class=SnowflakeDataSourceModel):
    def __init__(self, data_source_model: SnowflakeDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return SnowflakeSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return SnowflakeDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class SnowflakeSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        return " SELECT * FROM VALUES\n" + ",\n".join([self.build_expression_sql(value) for value in values.values])

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"ARRAY_CONSTRUCT{super()._build_tuple_sql(tuple)}"

    def default_varchar_length(self) -> Optional[int]:
        return 16777216

    def get_contract_type_dict(self) -> dict[str, str]:
        return {
            DBDataType.TEXT: "TEXT",
            DBDataType.INTEGER: "NUMBER",
            DBDataType.DECIMAL: "FLOAT",
            DBDataType.DATE: "DATE",
            DBDataType.TIME: "TIME",
            DBDataType.TIMESTAMP: "TIMESTAMP_NTZ",
            DBDataType.TIMESTAMP_TZ: "TIMESTAMP_TZ",
            DBDataType.BOOLEAN: "BOOLEAN",
        }
