"""How a contract's dataset becomes the table its queries read."""

import duckdb
import pytest
from soda_athena.common.data_sources.athena_data_source import AthenaSqlDialect
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.sql_dialect import FROM, JOIN, SELECT, STAR
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl, DuckDBSqlDialect


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


def test_an_empty_part_raises_wherever_a_dataset_is_named():
    with pytest.raises(InvalidArgumentException, match="empty part"):
        _verify("ds/memory//orders")
    dialect = DuckDBDataSourceImpl.from_existing_cursor(duckdb.connect(":memory:"), name="ds").sql_dialect
    assert dialect.qualify_dataset_name([None, "main"], "orders") == '"main"."orders"'
    with pytest.raises(InvalidArgumentException, match="empty part"):
        dialect.qualify_dataset_name(["soda_diag", ""], "orders")
    assert 'FROM "main"."orders"' in dialect.build_select_sql(
        [SELECT([STAR()]), FROM("orders", table_prefix=[None, "main"])]
    )
    for statement in (
        [SELECT([STAR()]), FROM("orders", table_prefix=["", "main"])],
        [SELECT([STAR()]), FROM("orders", table_prefix=["main"]), JOIN("customers", table_prefix=["main", ""])],
    ):
        with pytest.raises(InvalidArgumentException, match="empty part"):
            dialect.build_select_sql(statement)


@pytest.mark.parametrize("prefix", [["a", "", "c"], ["", "b", "c"]])
def test_an_empty_part_raises_on_a_dialect_that_joins_prefix_parts(prefix: list[str]):
    # Athena joins the catalog's parts with '/', so a check after its hook would see no empty part.
    dialect = AthenaSqlDialect()
    assert dialect.qualify_dataset_name(["a", "b", "c"], "orders") == '"a/b"."c"."orders"'
    with pytest.raises(InvalidArgumentException, match="empty part"):
        dialect.qualify_dataset_name(prefix, "orders")
    with pytest.raises(InvalidArgumentException, match="empty part"):
        dialect.build_select_sql([SELECT([STAR()]), FROM("orders", table_prefix=prefix)])


def test_an_empty_part_raises_on_a_dialect_that_splits_prefix_parts():
    # A hook may turn one given part into several, as the Dremio dialect in soda-extensions does with a dotted space
    # and folder.
    class SplittingDialect(DuckDBSqlDialect, sqlglot_dialect="duckdb"):
        def _normalize_dataset_prefix(self, dataset_prefix):
            return [part for given in dataset_prefix or [] for part in given.split(".")]

    dialect = SplittingDialect()
    assert dialect.qualify_dataset_name(["space.folder"], "orders") == '"space"."folder"."orders"'
    with pytest.raises(InvalidArgumentException, match="empty part"):
        dialect.qualify_dataset_name(["space..folder"], "orders")
