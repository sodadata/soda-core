import logging

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from tests.helpers.scanner import Scanner


def test_spark_df(scanner: Scanner):
    """Add plugin specific tests here. Present so that CI is simpler and to avoid false plugin-specific tests passing."""

    id = "a76824f0-50c0-11eb-8be8-88e9fe6293fd"
    data = [
        {
            "id": id,
            "name": "Paula Landry",
            "size": 3006
        },
        {
            "id": id,
            "name": "Kevin Crawford",
            "size": 7243
        }
    ]

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("size", IntegerType(), True)
        ]
    )

    spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()
    df = spark_session.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView('MYTABLE')

    spark_session.sql(sqlQuery='SELECT id, name as NME FROM MYTABLE').createOrReplaceTempView('OTHERTABLE')

    rows = spark_session.sql("SHOW TABLES").collect()
    for row in rows:
        table_name = row[1]
        logging.debug(f"TABLE: {table_name}")
        table_df = spark_session.table(table_name)
        table_df.printSchema()
