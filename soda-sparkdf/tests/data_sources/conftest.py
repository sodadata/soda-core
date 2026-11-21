import pytest
from pyspark.sql import SparkSession
from soda_sparkdf.common.data_sources.sparkdf_data_source import SparkDataFrameDataSource


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return SparkSession.builder.master("local").appName("soda_sparkdf").getOrCreate()


@pytest.fixture
def data_source(spark):
    """A Spark DataFrame data source named like the contracts' data source, databricks_prod."""
    return SparkDataFrameDataSource.from_existing_session(session=spark, name="databricks_prod", use_catalog=True)
