import re
import uuid
from pathlib import Path
from textwrap import dedent

import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from pyspark.sql import DataFrame, SparkSession
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.soda_cloud import SodaCloud
from soda_core.contracts.contract_verification import ContractVerificationSessionResult
from soda_sparkdf import SparkDataFrameDataSource, verify_contract_on_dataframe

DATA_SOURCE_NAME = "databricks_prod"
DATASET_PREFIX = f"{DATA_SOURCE_NAME}/spark_catalog/default"


@pytest.fixture
def df(spark) -> DataFrame:
    return spark.createDataFrame([(i, float(i)) for i in range(1, 6)], "id bigint, amount double")


@pytest.fixture
def name() -> str:
    return f"orders_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def contract_file_path(tmp_path, name) -> str:
    return _contract_file(tmp_path, name)


def _contract(table: str) -> str:
    # No table backs the dataset, so the checks pass only on the DataFrame's 5 rows.
    return dedent(
        f"""
        dataset: {DATASET_PREFIX}/{table}
        columns:
          - name: id
            data_type: bigint
          - name: amount
            data_type: double
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


def _contract_file(tmp_path, table: str) -> str:
    path = tmp_path / f"{uuid.uuid4().hex[:8]}.yml"
    path.write_text(_contract(table))
    return str(path)


def _verify(df, data_sources, contract_file_path: str, **kwargs) -> ContractVerificationSessionResult:
    return verify_contract_on_dataframe(
        df, data_sources=data_sources, contract_file_path=contract_file_path, publish_schema_metadata=False, **kwargs
    )


def _assert_passes(df, data_sources, contract_file_path: str) -> None:
    result: ContractVerificationSessionResult = _verify(df, data_sources, contract_file_path)
    assert result.is_passed, result.get_errors_str()


def _view_of(result: ContractVerificationSessionResult) -> str:
    """The view the run read, from its line "Reading <dataset> from <data source>/<view>"."""
    return next(line for line in result.get_logs() if line.startswith("Reading ")).rsplit("/", 1)[1]


def _assert_named_after(view: str, name: str) -> None:
    # The DQN as a slug, then 8 hex characters of its hash.
    assert re.fullmatch(f"soda_df_databricks_prod_spark_catalog_default_{name}_[0-9a-f]{{8}}", view), view


def _soda_df_views(spark: SparkSession) -> list[str]:
    return [t.name for t in spark.catalog.listTables() if t.name.startswith("soda_df_")]


def test_helper_publishes_the_dataframe_to_the_contracts_dataset(
    spark, df, data_source, name, contract_file_path, monkeypatch
):
    soda_cloud = MockSodaCloud(
        responses=[
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
            MockResponse(status_code=200, json_object={"scanId": "scan-1", "checks": []}),
        ]
    )
    soda_cloud.set_data_source_type(f"{DATASET_PREFIX}/{name}", "databricks")
    monkeypatch.setattr(SodaCloud, "from_config", staticmethod(lambda *args, **kwargs: soda_cloud))

    result = verify_contract_on_dataframe(
        df,
        data_sources=[data_source],
        # A path object works as its str does.
        contract_file_path=Path(contract_file_path),
        soda_cloud_file_path="soda-cloud.yml",
        publish=True,
        publish_schema_metadata=True,
    )

    assert result.is_passed, result.get_errors_str()
    _assert_named_after(_view_of(result), name)
    assert _soda_df_views(spark) == []
    [upload] = [r.json for r in soda_cloud.requests if isinstance(r.json, dict) and "defaultDataSource" in r.json]
    assert {(c["dataSource"], tuple(c["datasetPrefix"]), c["table"]) for c in upload["checks"]} == {
        (DATA_SOURCE_NAME, ("spark_catalog", "default"), name)
    }
    # publish_schema_metadata=True sends the DataFrame's columns as the dataset's.
    assert [(c["columnName"], c["sourceDataType"]) for c in upload["metadata"][0]["schema"]] == [
        ("id", "bigint"),
        ("amount", "double"),
    ]


def test_helper_drops_its_view_when_the_verification_raises(spark, df, data_source, contract_file_path):
    # Raised by verify_contract_locally, after the helper registered the view.
    with pytest.raises(InvalidArgumentException, match="Soda Cloud configuration file is required"):
        _verify(df, data_source, contract_file_path, publish=True)
    assert _soda_df_views(spark) == []


def test_helper_names_the_view_after_the_dataset_identifier(df, data_source, name, monkeypatch):
    soda_cloud = MockSodaCloud(responses=[MockResponse(json_object={"contents": _contract(name)})])
    monkeypatch.setattr(SodaCloud, "from_config", staticmethod(lambda *args, **kwargs: soda_cloud))
    result = verify_contract_on_dataframe(
        df,
        data_sources=data_source,
        dataset_identifier=f"{DATASET_PREFIX}/{name}",
        # An empty path names no file.
        contract_file_path="",
        soda_cloud_file_path="soda-cloud.yml",
        publish_schema_metadata=False,
    )
    assert result.is_passed, result.get_errors_str()
    _assert_named_after(_view_of(result), name)


def test_helper_refuses_a_view_it_did_not_register_and_leaves_it(spark, df, data_source, name, tmp_path):
    # Two datasets whose names have one ASCII slug.
    contract_file_path, sibling = (_contract_file(tmp_path, f"{prefix}_{name}") for prefix in ("销售", "库存"))
    view: str = _view_of(_verify(df, data_source, contract_file_path))
    spark.range(1).createTempView(view)
    try:
        _assert_passes(df, data_source, sibling)
        with pytest.raises(InvalidArgumentException, match=f"{view} already exists") as raised:
            _verify(df, data_source, contract_file_path)
        assert f'spark.sql("DROP VIEW IF EXISTS `{view}`")' in str(raised.value)
        assert spark.table(view).count() == 1, "the helper replaced or dropped a view it did not register"
    finally:
        spark.sql(f"DROP VIEW IF EXISTS `{view}`")


def test_helper_keeps_a_cache_the_caller_set(df, data_source, contract_file_path):
    df.cache()
    df.count()
    try:
        _assert_passes(df, data_source, contract_file_path)
        assert df.storageLevel.useMemory, "the helper dropped the cache the caller set on the DataFrame"
    finally:
        df.unpersist()


def test_helper_refuses_an_execution_dataset(df, data_source, name, contract_file_path):
    with pytest.raises(InvalidArgumentException, match="sets execution_dataset"):
        _verify(df, data_source, contract_file_path, execution_dataset=name)


def test_helper_needs_a_data_source_on_the_dataframes_session(spark, df, data_source, contract_file_path):
    _assert_passes(df, data_source, contract_file_path)
    other_session = SparkDataFrameDataSource.from_existing_session(
        session=spark.newSession(), name=DATA_SOURCE_NAME, use_catalog=True
    )
    with pytest.raises(InvalidArgumentException, match="from_existing_session"):
        _verify(df, other_session, contract_file_path)
    # verify_contract_locally refuses a tuple, before the helper looks for the session.
    with pytest.raises(InvalidArgumentException, match="tuple"):
        _verify(df, (data_source,), contract_file_path)
