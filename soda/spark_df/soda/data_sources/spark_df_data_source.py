from soda.common.logs import Logs
from soda.data_sources.spark_data_source import DataSourceImpl
from soda.data_sources.spark_df_connection import SparkDfConnection


class DataSourceImpl(DataSourceImpl):
    TYPE = "spark-df"

    def __init__(
        self,
        logs: Logs,
        data_source_name: str,
        data_source_properties: dict,
        connection_properties: dict
    ):
        super().__init__(logs, data_source_name, data_source_properties, connection_properties)
        self.method = 'spark-df'
        self.host = None
        self.port = None
        self.database = None
        self.server_side_parameters = None

    def connect(self, connection_properties: dict):
        spark_session = connection_properties.get('spark_session')
        self.connection = SparkDfConnection(spark_session)
