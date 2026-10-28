from __future__ import annotations

from typing import TYPE_CHECKING

from soda.data_sources.spark_df_cursor import SparkDfCursor

if TYPE_CHECKING:
    from pyspark.sql.session import SparkSession


class SparkDfConnection:
    def __init__(self, spark_session: SparkSession):
        if spark_session is None:
            raise ValueError(
                "Data source type spark_df needs a Spark session. Pass one with scan.add_spark_session(spark_session)."
            )
        # pyspark is an optional extra, so check here what the install-time pin used to guarantee.
        if not hasattr(spark_session.range(0), "offset"):
            raise RuntimeError(
                "Soda needs DataFrame.offset(), which needs pyspark 3.5 or later, or databricks-connect. "
                'Install with pip install "soda-core-spark-df[pyspark]", or upgrade databricks-connect.'
            )
        self.spark_session = spark_session

    def cursor(self) -> SparkDfCursor:
        return SparkDfCursor(self.spark_session)

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass
