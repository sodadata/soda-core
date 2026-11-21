"""execution_dataset on Spark, the one data source that reads another dataset: a contract's dataset is read from a
DataFrame's temporary view, and every result stays the contract's dataset's."""

import glob
import os
import subprocess
import sys
import uuid
from textwrap import dedent
from typing import Optional

import pyspark
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from pyspark.sql import SparkSession
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api.verify_api import verify_contract_locally
from soda_core.contracts.contract_verification import ContractVerificationSession, ContractVerificationSessionResult

DATA_SOURCE_NAME = "databricks_prod"


@pytest.fixture
def name(spark) -> str:
    name: str = f"orders_{uuid.uuid4().hex[:8]}"
    yield name
    spark.catalog.dropTempView(name)
    for table in (name, f"{name}_batch"):
        spark.sql(f"DROP TABLE IF EXISTS default.{table}")


@pytest.fixture
def write_contract(tmp_path):
    def write(name: str, contract: Optional[str] = None) -> str:
        path = os.path.join(tmp_path, f"{uuid.uuid4().hex[:8]}.yaml")
        with open(path, "w") as f:
            f.write((contract or _contract(name)) + "\n")
        return path

    return write


def _create_table(spark: SparkSession, name: str) -> None:
    # Other data than the DataFrame: 3 rows, one with a negative amount.
    spark.sql(f"CREATE TABLE default.{name} (id bigint, amount double) USING parquet")
    spark.sql(f"INSERT INTO default.{name} VALUES (1, 10.0), (2, -5.0), (3, 7.0)")


def _reading_line(result: ContractVerificationSessionResult) -> Optional[str]:
    return next((line for line in result.get_logs() if line.startswith("Reading ")), None)


def _register_view(spark: SparkSession, name: str) -> None:
    spark.createDataFrame([(i, float(i)) for i in range(1, 6)], "id bigint, amount double").createOrReplaceTempView(
        name
    )


def _contract(name: str, extra_checks: str = "") -> str:
    # Passes on the DataFrame's 5 rows, fails on the table's 3.
    return (
        dedent(
            f"""
        dataset: {DATA_SOURCE_NAME}/spark_catalog/default/{name}
        columns:
          - name: id
          - name: amount
            checks:
              - missing:
        checks:
          - schema:
          - row_count:
              threshold:
                must_be: 5
          - failed_rows:
              expression: amount < 0
        """
        ).strip()
        + extra_checks
    )


def _publish(soda_cloud: MockSodaCloud, data_source, contract: str, execution_dataset: Optional[str]):
    return ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract)],
        data_source_impls=[data_source],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
        execution_dataset=execution_dataset,
        publish_schema_metadata=True,
    )


def _publishing_cloud() -> MockSodaCloud:
    return MockSodaCloud(
        responses=[
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
            MockResponse(status_code=200, json_object={"scanId": "scan-1", "checks": []}),
        ]
    )


def _results_upload(soda_cloud: MockSodaCloud) -> dict:
    return next(r.json for r in soda_cloud.requests if isinstance(r.json, dict) and "defaultDataSource" in r.json)


def test_override_reads_the_view_and_ends_with_the_run(spark, data_source, name, write_contract):
    _create_table(spark, name)
    _register_view(spark, name)
    contract_file_path = write_contract(name)

    result = verify_contract_locally(
        data_sources=[data_source],
        contract_file_path=contract_file_path,
        execution_dataset=name,
        publish_schema_metadata=False,
    )
    assert result.is_passed, result.get_errors_str()
    assert (
        _reading_line(result)
        == f"Reading {DATA_SOURCE_NAME}/spark_catalog/default/{name} from {DATA_SOURCE_NAME}/{name}"
    )
    assert "run their own SQL" not in result.get_logs_str()

    def verify(execution_dataset: Optional[str]) -> ContractVerificationSessionResult:
        return verify_contract_locally(
            data_sources=[data_source], contract_file_path=contract_file_path, execution_dataset=execution_dataset
        )

    # A table works as well as a view.
    spark.sql(f"CREATE TABLE default.{name}_batch USING parquet AS SELECT * FROM {name}")
    assert verify(f"spark_catalog/default/{name}_batch").is_passed
    # Without execution_dataset, or naming the contract's own table, the run reads the table.
    for plain in (verify(None), verify(f"spark_catalog/default/{name}")):
        assert plain.is_failed and _reading_line(plain) is None


def test_a_view_whose_name_needs_quoting_is_read(spark, data_source, name, write_contract):
    # The schema check describes the view with the dialect's quoting.
    view: str = f"{name}-tmp"
    _register_view(spark, f"`{view}`")
    try:
        result = verify_contract_locally(
            data_sources=[data_source],
            contract_file_path=write_contract(name),
            execution_dataset=view,
            publish_schema_metadata=False,
        )
    finally:
        spark.catalog.dropTempView(view)
    assert result.is_passed, result.get_errors_str()


def test_results_publish_to_the_contracts_dataset(spark, data_source, name):
    _create_table(spark, name)
    _register_view(spark, name)
    dataset = f"{DATA_SOURCE_NAME}/spark_catalog/default/{name}"

    on_the_table: ContractVerificationSessionResult = _publish(_publishing_cloud(), data_source, _contract(name), None)
    soda_cloud = _publishing_cloud()
    soda_cloud.set_data_source_type(dataset, "databricks")
    on_the_view: ContractVerificationSessionResult = _publish(soda_cloud, data_source, _contract(name), name)

    assert on_the_view.is_passed, on_the_view.get_errors_str()
    assert [r.check.identity for r in on_the_view.contract_verification_results[0].check_results] == [
        r.check.identity for r in on_the_table.contract_verification_results[0].check_results
    ]
    upload: dict = _results_upload(soda_cloud)
    assert upload["definitionName"] == dataset
    assert {(c["dataSource"], tuple(c["datasetPrefix"]), c["table"]) for c in upload["checks"]} == {
        (DATA_SOURCE_NAME, ("spark_catalog", "default"), name)
    }
    assert [m["datasetQualifiedName"] for m in upload["metadata"]] == [dataset]


def test_a_reference_to_the_contracts_own_dataset_reads_the_view(spark, data_source, name, write_contract):
    # The view has ids 1 to 5 and the table 1 to 3, so a reference read from the table finds ids 4 and 5 invalid.
    _create_table(spark, name)
    _register_view(spark, name)
    dataset: str = f"{DATA_SOURCE_NAME}/spark_catalog/default/{name}"
    contract: str = dedent(
        f"""
        dataset: {dataset}
        columns:
          - name: id
            checks:
              - invalid:
                  valid_reference_data:
                    dataset: {dataset}
                    column: id
          - name: amount
        """
    ).strip()
    result = verify_contract_locally(
        data_sources=[data_source],
        contract_file_path=write_contract(name, contract),
        execution_dataset=name,
        publish_schema_metadata=False,
    )
    assert result.is_passed, result.get_errors_str()


def test_custom_sql_that_names_the_table_itself_reads_the_table(spark, data_source, name, write_contract):
    _create_table(spark, name)
    _register_view(spark, name)
    literal_sql_checks = f"""
  - metric:
      name: literal_table_count
      query: SELECT COUNT(*) FROM spark_catalog.default.{name}
      threshold:
        must_be: 3
  - failed_rows:
      qualifier: keys
      keys_query: SELECT id FROM spark_catalog.default.{name} WHERE amount < 0
      threshold:
        must_be: 1"""
    contract_file_path: str = write_contract(name, _contract(name, literal_sql_checks))

    def verify(**kwargs) -> ContractVerificationSessionResult:
        return verify_contract_locally(
            data_sources=[data_source], contract_file_path=contract_file_path, execution_dataset=name, **kwargs
        )

    result = verify()
    assert result.is_passed, result.get_errors_str()
    assert _named_in_the_warning(result) == {"checks.metric", "checks.failed_rows.keys"}
    # A check the run leaves out isn't named.
    assert _named_in_the_warning(verify(check_paths=["checks.row_count"])) == set()


def _named_in_the_warning(result: ContractVerificationSessionResult) -> set[str]:
    """The checks a run's warnings name, by check path."""
    verification = result.contract_verification_results[0]
    warnings: list[str] = [r.getMessage() for r in verification.log_records if r.levelname == "WARNING"]
    paths: set[str] = {check_result.check.check_path for check_result in verification.check_results}
    return {path for path in paths if any(repr(path) in warning for warning in warnings)}


@pytest.mark.parametrize(
    "with_table, execution_dataset, advice",
    [
        (True, "{name}", None),
        (False, "{name}", None),
        (True, "{ds}/default/{name}", None),
        # A dotted bare name gets the slash form to pass instead.
        (True, "default.{name}", "execution_dataset='default/"),
    ],
    ids=["same-named table", "no view", "the full DQN", "dotted name"],
)
def test_execution_dataset_must_name_a_view_or_an_existing_table(
    spark, data_source, name, with_table, execution_dataset, advice, write_contract
):
    if with_table:
        _create_table(spark, name)
    contract_file_path: str = write_contract(name)
    given: str = execution_dataset.format(name=name, ds=DATA_SOURCE_NAME)
    with pytest.raises(InvalidArgumentException, match=advice):
        verify_contract_locally(
            data_sources=[data_source],
            contract_file_path=contract_file_path,
            execution_dataset=given,
            publish_schema_metadata=False,
        )


@pytest.mark.parametrize(
    "cloud_data_source_type, refusal",
    [("spark_df", None), ("snowflake", "of type snowflake"), (None, "exists in Soda Cloud")],
)
def test_results_publish_only_onto_a_databricks_or_spark_dataframe_dataset(
    spark, data_source, name, cloud_data_source_type, refusal
):
    _create_table(spark, name)
    _register_view(spark, name)
    soda_cloud = _publishing_cloud()
    soda_cloud.set_data_source_type(f"{DATA_SOURCE_NAME}/spark_catalog/default/{name}", cloud_data_source_type)

    contract: str = _contract(name)
    if refusal is None:
        assert _publish(soda_cloud, data_source, contract, name).is_passed
        return
    with pytest.raises(InvalidArgumentException, match=refusal):
        _publish(soda_cloud, data_source, contract, name)
    # A run that doesn't publish reads the view whatever the type.
    assert ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(_contract(name))],
        data_source_impls=[data_source],
        soda_cloud_impl=soda_cloud,
        execution_dataset=name,
    ).is_passed


# Spark Connect can't share a process with the classic session above, so this runs in its own.
CONNECT_SCRIPT = """
import sys, tempfile, uuid
from pyspark.sql import SparkSession
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.contracts.api.verify_api import verify_contract_locally
from soda_sparkdf.common.data_sources.sparkdf_data_source import SparkDataFrameDataSource

spark = (
    SparkSession.builder.remote("local[1]")
    .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
name = f"orders_{uuid.uuid4().hex[:8]}"
spark.sql(f"CREATE TABLE default.{name} (id bigint, amount double) USING parquet")
spark.sql(f"INSERT INTO default.{name} VALUES (1, 10.0), (2, -5.0), (3, 7.0)")
data_source = SparkDataFrameDataSource.from_existing_session(session=spark, name="databricks_prod", use_catalog=True)
contract_file_path = tempfile.mkstemp(suffix=".yaml")[1]
with open(sys.argv[1]) as template, open(contract_file_path, "w") as contract:
    contract.write(template.read().replace("NAME", name))

def verify():
    return verify_contract_locally(
        data_sources=[data_source],
        contract_file_path=contract_file_path,
        execution_dataset=name,
        publish_schema_metadata=False,
    )

try:
    verify()
    sys.exit("a same-named table passed the guard")
except InvalidArgumentException:
    pass

df = spark.createDataFrame([(i, float(i)) for i in range(1, 6)], "id bigint, amount double")
df.createOrReplaceTempView(name)
result = verify()
assert result.is_passed, result.get_errors_str()
"""


@pytest.mark.skipif(
    not glob.glob(os.path.join(os.path.dirname(pyspark.__file__), "jars", "spark-connect_*.jar")),
    reason="this pyspark doesn't bundle Spark Connect",
)
def test_connect_guard_and_override_read_the_view(tmp_path, write_contract):
    template_path = write_contract("NAME")
    script_path = os.path.join(tmp_path, "connect_check.py")
    with open(script_path, "w") as f:
        f.write(CONNECT_SCRIPT)

    # SPARK_TESTING keeps the local Connect server off the fixed port 15002, so parallel runs don't collide.
    completed = subprocess.run(
        [sys.executable, script_path, template_path],
        capture_output=True,
        text=True,
        timeout=600,
        env={**os.environ, "SPARK_TESTING": "1"},
    )
    assert completed.returncode == 0, completed.stdout[-3000:] + completed.stderr[-3000:]
