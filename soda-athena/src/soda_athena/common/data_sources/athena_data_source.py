import logging
from datetime import datetime
from typing import Optional

from soda_athena.common.data_sources.athena_data_source_connection import (
    AthenaDataSource as AthenaDataSourceModel,
)
from soda_athena.common.data_sources.athena_data_source_connection import (
    AthenaDataSourceConnection,
)
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_datatypes import DBDataType
from soda_core.common.sql_dialect import SqlDialect

logger: logging.Logger = soda_logger


class AthenaDataSourceImpl(DataSourceImpl, model_class=AthenaDataSourceModel):
    def __init__(self, data_source_model: AthenaDataSourceModel):
        super().__init__(data_source_model=data_source_model)

    def _create_sql_dialect(self) -> SqlDialect:
        return AthenaSqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return AthenaDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    # def execute_query(self, sql: str) -> QueryResult:
    #     # Athena does not play well with freezegun.
    #     # The datasources requires a timestamp in the query (done automatically), this timestamp must be in sync with AWS servers
    #     # This is not the case when using freezegun.
    #     # We need to disable freezegun for this method.

    #     # TODO: Refactor this so it's in a proper place. It shouldn't live here.
    #     # now = datetime.now()
    #     # if now < datetime(year=2025, month=8, day=1, hour=0, minute=0, second=0):
    #     #     with freezegun.freeze_time(freezegun.api.real_datetime.now(tz=timezone.utc)):
    #     #         result = self.connection.execute_query(sql=sql)
    #     #     return result
    #     return self.connection.execute_query(sql=sql)


class AthenaSqlDialect(SqlDialect):
    def get_contract_type_dict(self) -> dict[str, str]:
        base_contract_type_dict = super().get_contract_type_dict()
        base_contract_type_dict[DBDataType.TEXT] = "string"
        base_contract_type_dict[DBDataType.BOOLEAN] = "boolean"
        base_contract_type_dict[DBDataType.INTEGER] = "int"
        base_contract_type_dict[DBDataType.DECIMAL] = "decimal"
        base_contract_type_dict[DBDataType.DATE] = "date"
        base_contract_type_dict[DBDataType.TIME] = "date"
        base_contract_type_dict[DBDataType.TIMESTAMP] = "timestamp"
        base_contract_type_dict[DBDataType.TIMESTAMP_TZ] = "timestamp"
        return base_contract_type_dict

    def get_sql_type_dict(self) -> dict[str, str]:
        base_sql_type_dict = super().get_sql_type_dict()
        # base_sql_type_dict[DBDataType.TIMESTAMP] = "timestamp(3)"
        # base_sql_type_dict[DBDataType.TIMESTAMP_TZ] = "timestamp(3)"
        return base_sql_type_dict

    def quote_default(self, identifier: Optional[str]) -> Optional[str]:
        return identifier

    def literal_datetime(self, datetime: datetime):
        return f"From_iso8601_timestamp('{datetime.isoformat()}')"

    def literal_datetime_with_tz(self, datetime: datetime):
        # Can be overloaded if the subclass does not support timezones (may have to do conversion yourself)
        # We assume that all timestamps are stored in UTC.
        # See Fabric for an example
        return self.literal_datetime(datetime)
