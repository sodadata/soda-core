"""How a contract's dataset becomes the table its queries read."""

import duckdb
import pytest
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl


def _verify(dataset: str):
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE main.orders (id INTEGER)")
    connection.execute("INSERT INTO main.orders VALUES (1)")
    return ContractVerificationSession.execute(
        contract_yaml_sources=[
            ContractYamlSource.from_str(f"dataset: {dataset}\ncolumns: []\nchecks:\n  - row_count:\n")
        ],
        data_source_impls=[DuckDBDataSourceImpl.from_existing_cursor(connection, name="ds")],
    )


def test_a_dataset_reads_its_table():
    assert _verify("ds/memory/main/orders").is_passed


def test_an_empty_segment_in_the_dataset_raises():
    with pytest.raises(TypeError):
        _verify("ds/memory//orders")
