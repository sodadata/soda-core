"""${soda.DATASET}: the checked table in custom SQL, written in by the data source when it runs the SQL."""

from __future__ import annotations

import duckdb
import pytest
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.check_collections.session import execute_check_collections
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import ContractVerificationSession
from soda_duckdb.common.data_sources.duckdb_data_source import DuckDBDataSourceImpl

# Every check passes on Orders-Tmp, ids 1 and 2.
CONTRACT = """
dataset: ds/main/Orders-Tmp
columns:
  - name: id
checks:
  - metric:
      query: SELECT COUNT(*) FROM ${soda.DATASET}
      threshold:
        must_be: 2
  - failed_rows:
      expression: id NOT IN (SELECT id FROM ${soda.DATASET})
  - row_count:
      filter: id IN (SELECT MIN(id) FROM ${soda.DATASET})
      threshold:
        must_be: 1
"""


@pytest.fixture
def duckdb_data_source():
    connection = duckdb.connect(":memory:")
    connection.execute('CREATE TABLE main."Orders-Tmp" (id INTEGER)')
    connection.execute('INSERT INTO main."Orders-Tmp" VALUES (1), (2)')
    connection.execute("CREATE TABLE main.customers (id INTEGER)")
    connection.execute("INSERT INTO main.customers VALUES (1), (2), (3)")
    return DuckDBDataSourceImpl.from_existing_cursor(connection, name="ds")


def test_soda_dataset_names_the_quoted_table_in_queries_expressions_and_filters(duckdb_data_source):
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(CONTRACT)], data_source_impls=[duckdb_data_source]
    )
    assert result.is_passed, result.get_errors_str()


def test_soda_dataset_reads_the_data_source_the_run_is_given_whatever_its_name(duckdb_data_source):
    contract = CONTRACT.replace("dataset: ds/", "dataset: other_name/")
    result = execute_check_collections(
        yaml_sources=[ContractYamlSource.from_str(contract)], data_source_impl=duckdb_data_source
    )
    assert not result.has_errors, result.get_errors_str()
    assert result.results and all(r.is_passed for r in result.results)


def test_soda_dataset_names_each_contracts_own_table(duckdb_data_source):
    customers = CONTRACT.replace("Orders-Tmp", "customers").replace("must_be: 2", "must_be: 3")
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(CONTRACT), ContractYamlSource.from_str(customers)],
        data_source_impls=[duckdb_data_source],
    )
    assert result.is_passed, result.get_errors_str()


def test_soda_dataset_works_through_a_variable_in_the_contract_filter(duckdb_data_source):
    contract = "variables:\n  checked:\n    default: ${soda.DATASET}\n" + CONTRACT.replace(
        "columns:", "filter: id IN (SELECT id FROM ${var.checked})\ncolumns:", 1
    )
    result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(contract)], data_source_impls=[duckdb_data_source]
    )
    assert result.is_passed, result.get_errors_str()


def test_soda_cloud_shows_the_checks_sql_as_written(duckdb_data_source):
    soda_cloud = MockSodaCloud(
        responses=[
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
            MockResponse(status_code=200, json_object={"scanId": "scan-1", "checks": []}),
        ]
    )
    ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(CONTRACT)],
        data_source_impls=[duckdb_data_source],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
    )
    upload = next(r.json for r in soda_cloud.requests if isinstance(r.json, dict) and "defaultDataSource" in r.json)
    metric_check = next(c for c in upload["checks"] if c["checkType"] == "metric")
    assert "SELECT COUNT(*) FROM ${soda.DATASET}" in metric_check["definition"]


def test_soda_dataset_is_refused_in_sql_run_outside_a_check(duckdb_data_source):
    duckdb_data_source.open_connection()
    assert duckdb_data_source.execute_query('SELECT COUNT(*) FROM main."Orders-Tmp"').rows == [(2,)]
    with pytest.raises(InvalidArgumentException):
        duckdb_data_source.execute_query("SELECT COUNT(*) FROM ${soda.DATASET}")
