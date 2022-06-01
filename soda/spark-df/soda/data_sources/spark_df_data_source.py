from soda.data_sources.spark_df_connection import SparkDfConnection
from soda.execution.data_source import DataSource


class DataSourceImpl(DataSource):
    TYPE = "spark-df"

    def connect(self, connection_properties: dict):
        spark_session = connection_properties.get('spark_session')
        self.connection = SparkDfConnection(spark_session)
