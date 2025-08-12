from typing import Optional

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

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: bool = True) -> str:
        return (
            f"{super().create_schema_if_not_exists_sql(prefixes, add_semicolon=False)} AUTHORIZATION CURRENT_USER"
            + (";" if add_semicolon else "")
        )

    def default_varchar_length(self) -> Optional[int]:
        return 255

    def get_sql_type_dict(self) -> dict[str, str]:
        base_dict: dict = super().get_sql_type_dict()
        base_dict[DBDataType.TEXT] = f"character varying({self.default_varchar_length()})"
        return base_dict
