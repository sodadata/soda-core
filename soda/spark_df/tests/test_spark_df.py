import logging
from textwrap import dedent

from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_spark_df(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - row_count = 10.0
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_profiling_queries(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)
    scanner.data_source.test(dedent(
        f"""
            WITH
                value_frequencies AS (
                    SELECT cat as value_name, count(*) as frequency
                    FROM {table_name}
                    GROUP BY cat
                ),
                mins as (
                    SELECT cast('mins' as string) as metric, row_number() over(ORDER BY value_name asc) as idx, value_name, frequency
                    FROM value_frequencies
                    WHERE value_name is not null
                    ORDER BY value_name asc
                    LIMIT 5
                ),
                maxs as (
                    SELECT cast('maxs' as string) as metric, row_number() over(ORDER BY value_name desc) as idx, value_name, frequency
                    FROM value_frequencies
                    WHERE value_name is not null
                    ORDER BY value_name desc
                    LIMIT 5
                ),
                frequent_values as (
                    SELECT cast('frequent_values' as string) as metric, row_number() over(ORDER BY frequency desc) as idx, value_name, frequency
                    FROM value_frequencies
                    WHERE value_name is not null
                    ORDER BY frequency desc
                    LIMIT 5
                ),
                result as (
                    SELECT * FROM mins
                    UNION
                    SELECT * FROM maxs
                    UNION
                    SELECT * FROM frequent_values
                )
                SELECT *
                FROM result
                ORDER BY metric asc, idx asc
        """
    ))


def test_spark_df_basics():
    id = "a76824f0-50c0-11eb-8be8-88e9fe6293fd"
    data = [{"id": id, "name": "Paula Landry", "size": 3006}, {"id": id, "name": "Kevin Crawford", "size": 7243}]

    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("size", IntegerType(), True),
        ]
    )

    spark_session = SparkSession.builder.master("local").appName("test").getOrCreate()
    df = spark_session.createDataFrame(data=data, schema=schema)
    df.createOrReplaceTempView("MYTABLE")

    spark_session.sql(sqlQuery="SELECT id, name as NME FROM MYTABLE").createOrReplaceTempView("OTHERTABLE")

    show_tables_df = spark_session.sql("SHOW TABLES FROM ''")
    show_tables_df.printSchema()
    show_tables_df.show()

    rows = show_tables_df.collect()
    for i in range(0, len(rows)):
        row = rows[i]
        for j in range(0, len(row)):
            cell_value = row[j]
            logging.debug(f"value ({i},{j}): {cell_value}")
            logging.debug(f"type ({i},{j}): {type(cell_value).__name__}")
