from typing import NamedTuple

import pytest
from soda_core.common.data_source_results import QueryResult
from soda_synapse.common.data_sources.synapse_data_source import SynapseDataSourceImpl
from soda_synapse.test_helpers.synapse_data_source_test_helper import SynapseDataSourceTestHelper

_COLUMNS_METADATA = QueryResult(rows=[("id", "int"), ("name", "bigint")], columns=(("column_name",), ("data_type",)))


class ImplUnderTest(NamedTuple):
    impl: SynapseDataSourceImpl
    executed_queries: list[str]


@pytest.fixture
def under_test(monkeypatch) -> ImplUnderTest:
    # A real impl from the helper's real config; only the warehouse round-trip is stubbed.
    impl = SynapseDataSourceTestHelper("unit_test").data_source_impl
    executed_queries: list[str] = []

    def execute_query(sql: str, log_query: bool = True) -> QueryResult:
        executed_queries.append(sql)
        return _COLUMNS_METADATA

    monkeypatch.setattr(impl, "execute_query", execute_query)
    return ImplUnderTest(impl, executed_queries)


def test_pagination_resolves_a_tables_columns_once(under_test: ImplUnderTest):
    first = under_test.impl._get_column_names_for_pagination(["db", "schema"], "table")
    second = under_test.impl._get_column_names_for_pagination(["db", "schema"], "table")
    under_test.impl._get_column_names_for_pagination(["db", "schema"], "other_table")
    assert first == second == ["id", "name"]
    assert len(under_test.executed_queries) == 2
    assert "[information_schema].[columns]" in under_test.executed_queries[0]


def test_clear_column_names_cache_makes_the_next_lookup_query_again(under_test: ImplUnderTest):
    under_test.impl._get_column_names_for_pagination(["db", "schema"], "table")
    under_test.impl.clear_column_names_cache()
    under_test.impl._get_column_names_for_pagination(["db", "schema"], "table")
    assert len(under_test.executed_queries) == 2
