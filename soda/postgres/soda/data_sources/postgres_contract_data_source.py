from __future__ import annotations

import logging

from soda.contracts.impl.contract_data_source import ClContractDataSource
from soda.contracts.impl.sodacl_log_converter import SodaClLogConverter
from soda.contracts.impl.yaml_helper import YamlFile
from soda.data_sources.postgres_data_source import PostgresDataSource
from soda.execution.data_source import DataSource as SodaCLDataSource

logger = logging.getLogger(__name__)


class PostgresContractDataSource(ClContractDataSource):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file)

    def _create_connection(self, connection_yaml_dict: dict) -> object:
        import psycopg2

        if connection_yaml_dict["password"] == "":
            connection_yaml_dict["password"] = None

        self._log_connection_properties_excl_pwd("postgres", connection_yaml_dict)

        return psycopg2.connect(**connection_yaml_dict)

    def _create_sodacl_data_source(self,
                                   database_name: str | None,
                                   schema_name: str | None,
                                   sodacl_data_source_name: str,
                                   sodacl_logs: SodaClLogConverter
                                   ) -> SodaCLDataSource:
        sodacl_data_source = PostgresDataSource(
            logs=sodacl_logs,
            data_source_name=sodacl_data_source_name,
            data_source_properties={}
        )
        sodacl_data_source.database = database_name
        sodacl_data_source.schema = schema_name
        sodacl_data_source.quote_tables = True
        sodacl_data_source.connection = self.connection
        return sodacl_data_source
