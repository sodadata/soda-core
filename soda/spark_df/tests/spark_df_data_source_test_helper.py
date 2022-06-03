from pyspark.sql import SparkSession


class SparkDfDataSourceTestHelper:
    @staticmethod
    def initialize_local_spark_session(scan: "Scan"):
        spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()
        scan.add_spark_session(spark_session=spark_session)
