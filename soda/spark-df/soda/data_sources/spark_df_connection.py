from pyspark.sql.session import SparkSession

from soda.data_sources.spark_df_cursor import SparkDfCursor


class SparkDfConnection:

    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def sql(self, sql: str) -> SparkDfCursor:
        df = self.spark_session.sql(sqlQuery=sql)
        return SparkDfCursor(df)
