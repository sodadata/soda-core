from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from pyspark.sql import SparkSession
from soda_sparkdf.common.data_sources.sparkdf_data_source import (
    SparkDataFrameDataSourceConnection,
    SparkDataFrameDataSourceImpl,
)

# Teset table spec is just used to build the contract, the actual data is in the df/csv/etc
test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("row_count_sparkdf")
    .column_varchar("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)

contract_str = f"""
    columns:
        - name: id
          data_type: text
          checks:
            - missing:
    checks:
        - row_count:
            threshold:
                must_be: 3
"""


def test_basic_setup(data_source_test_helper: DataSourceTestHelper):
    connection: SparkDataFrameDataSourceConnection = data_source_test_helper.data_source_impl.connection

    # connection.session.createDataFrame([(1,), (2,), (3,)], ["id"]).createOrReplaceTempView("my_test_table")

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_str,
    )


def test_basic_setup_with_existing_session(data_source_test_helper: DataSourceTestHelper):
    my_spark_session = (
        SparkSession.builder.master("local")
        .appName("test_sparkdf")
        .config("spark.sql.warehouse.dir", data_source_test_helper.test_dir)
        .getOrCreate()
    )

    another_data_source_impl = SparkDataFrameDataSourceImpl.from_existing_session(
        session=my_spark_session, name=data_source_test_helper.name
    )

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_str,
    )
