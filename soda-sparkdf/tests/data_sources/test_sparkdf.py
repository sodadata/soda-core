from logging import Logger
from textwrap import dedent

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from pyspark.sql import SparkSession
from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_sparkdf.common.data_sources.sparkdf_data_source import (
    SparkDataFrameDataSource,
)

logger: Logger = soda_logger


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

contract_str = """
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

    # Overwrite the data source impl with the new one, so the contract verification uses the new data source
    data_source_test_helper.data_source_impl = another_data_source_impl

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=contract_str,
    )


def test_more_complex_setup_with_existing_session(data_source_test_helper: DataSourceTestHelper):
    my_spark_session = (
        SparkSession.builder.master("local")
        .appName("test_sparkdf")
        .config("spark.sql.warehouse.dir", data_source_test_helper.test_dir)
        .getOrCreate()
    )

    my_data_source_impl = SparkDataFrameDataSource.from_existing_session(
        session=my_spark_session, name=data_source_test_helper.name
    )

    # Create a dataframe with some ids
    my_spark_session.createDataFrame([(1,), (2,), (3,)], ["id"]).createOrReplaceTempView("my_test_table")

    # Mock the soda cloud, because we need that for the contract verification
    mock_cloud = data_source_test_helper.enable_soda_cloud_mock([])

    contract_str = """
    dataset: primary_datasource/my_test_table
    columns:
        - name: id
          data_type: integer
          checks:
            - missing:
    checks:
        - row_count:
            threshold:
                must_be: 3
    """
    checks_contract_yaml_str = dedent(contract_str).strip()

    logger.debug(f"Contract:\n{checks_contract_yaml_str}")

    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(checks_contract_yaml_str)],
        variables=None,
        data_source_impls=[my_data_source_impl],
        soda_cloud_impl=mock_cloud,
        soda_cloud_use_agent=False,
        soda_cloud_publish_results=False,
        dwh_data_source_file_path=None,
    )

    assert result.is_ok, f"Expected contract to be ok, but got {result.get_errors_str()}"
