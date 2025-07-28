from typing import Optional
from datetime import datetime

from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.sql_ast import REGEX_LIKE
from soda_core.common.sql_dialect import DBDataType, SqlDialect
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSource as PostgresDataSourceModel,
)
from soda_postgres.common.data_sources.postgres_data_source_connection import (
    PostgresDataSourceConnection,
)


class PostgresDataSourceImpl(DataSourceImpl, model_class=PostgresDataSourceModel):
    def __init__(self, data_source_model: PostgresDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return PostgresSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return PostgresDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )


class PostgresSqlDialect(SqlDialect):
    def __init__(self):
        super().__init__()

    def _build_regex_like_sql(self, matches: REGEX_LIKE) -> str:
        expression: str = self.build_expression_sql(matches.expression)
        return f"{expression} ~ '{matches.regex_pattern}'"

    def create_schema_if_not_exists_sql(self, schema_name: str) -> str:
        quoted_schema_name: str = self.quote_default(schema_name)
        return f"CREATE SCHEMA IF NOT EXISTS {quoted_schema_name} AUTHORIZATION CURRENT_USER;"

    def default_varchar_length(self) -> Optional[int]:
        return 255

    def get_sql_type_dict(self) -> dict[str, str]:
        base_dict = super().get_sql_type_dict()
        base_dict[DBDataType.TEXT] = f"character varying({self.default_varchar_length()})"
        return base_dict

    def literal_datetime(self, datetime_value: datetime) -> str:
        """PostgreSQL-specific timestamp literal format with timezone support"""
        if datetime_value.tzinfo:
            # Convert to UTC and format with timezone info
            utc_datetime = datetime_value.astimezone()
            datetime_str = utc_datetime.strftime("%Y-%m-%d %H:%M:%S %z")
            datetime_str_formatted = datetime_str[:-2] + ":" + datetime_str[-2:]
            return f"'{datetime_str_formatted}'"
        else:
            # No timezone info, treat as local time
            datetime_str_formatted = datetime_value.strftime("%Y-%m-%d %H:%M:%S")
            return f"'{datetime_str_formatted}'"
