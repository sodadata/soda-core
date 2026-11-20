"""execution_dataset: the mistakes it refuses before another dataset is read. What a run reads with it is tested
on Spark, the one data source that reads another dataset, in soda-sparkdf."""

from __future__ import annotations

import os

import duckdb
import pytest
from helpers.mock_soda_cloud import MockSodaCloud
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.api import verify_api
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_core.contracts.impl.contract_verification_impl import CheckCollectionImplExtension, ContractImpl
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

CONTRACT = """
dataset: ds/main/orders
columns:
  - name: id
checks:
  - row_count:
  - schema:
"""
EXECUTION_DATASET = "main/orders_batch"


def _duckdb() -> DuckDBDataSourceImpl:
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE main.orders (id INTEGER)")
    connection.execute("INSERT INTO main.orders VALUES (1)")
    connection.execute("CREATE TABLE main.orders_batch (id INTEGER)")
    return DuckDBDataSourceImpl.from_existing_cursor(connection, name="ds")


def _verify(contract: str = CONTRACT, **kwargs):
    return ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract)], data_source_impls=[_duckdb()], **kwargs
    )


def test_a_data_source_that_reads_no_relation_refuses_execution_dataset():
    assert _verify().is_passed
    # Naming the contract's own table reads no other dataset, so it is a plain run.
    assert _verify(execution_dataset="main/orders").is_passed
    with pytest.raises(InvalidArgumentException):
        _verify(execution_dataset=EXECUTION_DATASET)


def test_execution_dataset_needs_a_verification():
    with pytest.raises(InvalidArgumentException, match="verify, don't validate"):
        _verify(execution_dataset=EXECUTION_DATASET, only_validate_without_execute=True)


def test_execution_dataset_needs_a_data_source_named_like_the_contracts():
    contract: str = CONTRACT.replace("ds/main/orders", "prod/main/orders")
    assert "Data source 'prod' not found" in _verify(contract).get_errors_str()
    with pytest.raises(InvalidArgumentException):
        _verify(contract, execution_dataset=EXECUTION_DATASET)


def test_execution_dataset_reads_one_contract_at_a_time():
    def verify(**kwargs):
        return ContractVerificationSession.execute(
            contract_yaml_sources=[ContractYamlSource.from_str(CONTRACT) for _ in "ab"],
            data_source_impls=[_duckdb()],
            **kwargs,
        )

    assert verify().is_passed
    with pytest.raises(InvalidArgumentException, match="single contract"):
        verify(execution_dataset=EXECUTION_DATASET)


@pytest.mark.parametrize(
    "local_only",
    [{"execution_dataset": EXECUTION_DATASET}, {"publish_schema_metadata": False}],
    ids=lambda k: next(iter(k)),
)
def test_a_runner_refuses_what_only_a_local_verification_does(local_only: dict):
    contract_yaml_sources = [ContractYamlSource.from_str(CONTRACT)]
    soda_cloud = MockSodaCloud()
    with pytest.raises(InvalidArgumentException, match="not on a runner"):
        ContractVerificationSession.execute(
            contract_yaml_sources=contract_yaml_sources,
            soda_cloud_impl=soda_cloud,
            soda_cloud_use_runner=True,
            **local_only,
        )


class _OtherKindImpl(ContractImpl):
    """A kind other than contract that uploads per file, as monitors do."""

    kind = "other-kind-for-test"


def test_execution_dataset_reads_a_contract_and_no_other_kind():
    other_kind: str = f"kind: {_OtherKindImpl.kind}\n{CONTRACT}"
    assert _verify(other_kind).is_passed
    with pytest.raises(InvalidArgumentException, match=f"kind '{_OtherKindImpl.kind}'"):
        _verify(other_kind, execution_dataset=EXECUTION_DATASET)


class _RefusingExtension(CheckCollectionImplExtension):
    """Refuses what the caller passed while its checks are parsed, as the reconciliation extension does under an
    override."""

    def parse_checks(self, contract_impl):
        raise InvalidArgumentException("refused by the extension")


def test_an_extension_that_refuses_stops_the_run():
    assert _verify().is_passed
    ContractImpl.impl_extensions["refusing_for_test"] = _RefusingExtension
    try:
        with pytest.raises(InvalidArgumentException, match="refused by the extension"):
            _verify()
    finally:
        ContractImpl.impl_extensions.pop("refusing_for_test", None)


def _verify_locally(tmp_path, **kwargs):
    contract_file_path = os.path.join(tmp_path, "contract.yml")
    with open(contract_file_path, "w") as f:
        f.write(CONTRACT)
    return verify_api.verify_contract_locally(contract_file_path=contract_file_path, data_sources=[_duckdb()], **kwargs)


@pytest.mark.parametrize(
    "mistake, message",
    [
        ({"execution_dataset": "orders/", "publish_schema_metadata": False}, "DQN without the data source"),
        ({"execution_dataset": object(), "publish_schema_metadata": False}, "DQN without the data source"),
        ({"execution_dataset": "main/orders_batch", "publish": True}, "must be True or False"),
    ],
    ids=["malformed", "not a string", "no publish_schema_metadata"],
)
def test_verify_contract_locally_refuses_a_mistake(tmp_path, mistake: dict, message: str):
    assert _verify_locally(tmp_path).is_passed
    with pytest.raises(InvalidArgumentException, match=message):
        _verify_locally(tmp_path, **mistake)


def test_verify_contract_locally_leaves_out_the_columns_on_request(tmp_path):
    def columns(**kwargs):
        return _verify_locally(tmp_path, **kwargs).contract_verification_results[0].dataset_columns

    assert columns()
    assert columns(publish_schema_metadata=False) is None
