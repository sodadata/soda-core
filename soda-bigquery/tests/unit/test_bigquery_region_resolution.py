from types import SimpleNamespace
from unittest.mock import MagicMock

from soda_bigquery.common.data_sources.bigquery_data_source import (
    BigQueryDataSourceImpl,
    BigQueryMetadataTablesQuery,
    BigQuerySqlDialect,
)
from soda_core.common.data_source_results import QueryResult

PROJECT = "test-project"


def _make_impl(location: str | None = None) -> BigQueryDataSourceImpl:
    impl = BigQueryDataSourceImpl.__new__(BigQueryDataSourceImpl)
    impl.data_source_model = MagicMock()
    impl.data_source_model.connection_properties.location = location
    impl.data_source_connection = MagicMock()
    impl.data_source_connection.project_id = PROJECT
    impl._dataset_region_cache = {}
    return impl


def _mock_datasets(impl: BigQueryDataSourceImpl, locations_by_dataset_id: dict[str, str]) -> None:
    impl.data_source_connection.client.list_datasets.return_value = [
        SimpleNamespace(dataset_id=dataset_id) for dataset_id in locations_by_dataset_id
    ]
    impl.data_source_connection.client.get_dataset.side_effect = lambda reference: SimpleNamespace(
        location=locations_by_dataset_id[reference.split(".")[-1]]
    )


class TestRegionsInScope:
    def test_configured_location_wins_without_metadata_lookups(self):
        impl = _make_impl(location="europe-west4")

        assert impl.regions_in_scope(project_id=PROJECT, dataset_id="whatever") == ["europe-west4"]
        impl.data_source_connection.client.list_datasets.assert_not_called()
        impl.data_source_connection.client.get_dataset.assert_not_called()

    def test_known_dataset_resolves_its_own_region(self):
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="asia-northeast1")

        assert impl.regions_in_scope(project_id=PROJECT, dataset_id="dataset_tokyo") == ["asia-northeast1"]
        impl.data_source_connection.client.get_dataset.assert_called_once_with(f"{PROJECT}.dataset_tokyo")
        impl.data_source_connection.client.list_datasets.assert_not_called()

    def test_without_dataset_lists_project_datasets_and_dedupes_regions(self):
        impl = _make_impl()
        _mock_datasets(impl, {"dataset_eu_1": "EU", "dataset_us": "US", "dataset_eu_2": "EU"})

        assert impl.regions_in_scope(project_id=PROJECT) == ["EU", "US"]
        impl.data_source_connection.client.list_datasets.assert_called_once_with(project=PROJECT)

    def test_dataset_region_lookups_are_cached(self):
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="EU")

        impl.regions_in_scope(project_id=PROJECT, dataset_id="dataset_a")
        impl.regions_in_scope(project_id=PROJECT, dataset_id="dataset_a")

        impl.data_source_connection.client.get_dataset.assert_called_once()

    def test_falls_back_to_connection_project(self):
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="EU")

        assert impl.regions_in_scope(dataset_id="dataset_a") == ["EU"]
        impl.data_source_connection.client.get_dataset.assert_called_once_with(f"{PROJECT}.dataset_a")


class TestMetadataTablesQuery:
    def _make_query(self, impl: BigQueryDataSourceImpl, rows_for_sql) -> BigQueryMetadataTablesQuery:
        data_source_connection = impl.data_source_connection

        def execute_query(sql: str, **_kwargs) -> QueryResult:
            return QueryResult(rows=rows_for_sql(sql), columns=None)

        data_source_connection.execute_query.side_effect = execute_query
        return BigQueryMetadataTablesQuery(
            sql_dialect=BigQuerySqlDialect(),
            data_source_connection=data_source_connection,
            data_source_impl=impl,
        )

    def test_queries_each_region_and_concatenates_results(self):
        impl = _make_impl()
        _mock_datasets(impl, {"dataset_eu": "EU", "dataset_us": "US"})

        def rows_for_sql(sql: str):
            if "region-EU" in sql:
                return [(PROJECT, "dataset_eu", "orders", "BASE TABLE")]
            if "region-US" in sql:
                return [(PROJECT, "dataset_us", "shipments", "BASE TABLE")]
            raise AssertionError(f"unexpected SQL: {sql}")

        query = self._make_query(impl, rows_for_sql)
        results = query.execute(database_name=PROJECT)

        assert sorted(fully_qualified.get_object_name() for fully_qualified in results) == ["orders", "shipments"]
        executed_sqls = [call.args[0] for call in impl.data_source_connection.execute_query.call_args_list]
        assert len(executed_sqls) == 2
        assert all("INFORMATION_SCHEMA" in sql for sql in executed_sqls)
        assert all("@@location" not in sql for sql in executed_sqls)

    def test_known_dataset_queries_only_its_region(self):
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="EU")

        query = self._make_query(impl, lambda sql: [(PROJECT, "dataset_eu", "orders", "BASE TABLE")])
        results = query.execute(database_name=PROJECT, schema_name="dataset_eu")

        assert [fully_qualified.get_object_name() for fully_qualified in results] == ["orders"]
        executed_sqls = [call.args[0] for call in impl.data_source_connection.execute_query.call_args_list]
        assert len(executed_sqls) == 1
        assert "region-EU" in executed_sqls[0]
        impl.data_source_connection.client.list_datasets.assert_not_called()


class TestCreateMetadataTablesQuery:
    def test_returns_region_aware_query_without_running_probe_queries(self):
        impl = _make_impl()
        impl.sql_dialect = BigQuerySqlDialect()

        query = impl.create_metadata_tables_query()

        assert isinstance(query, BigQueryMetadataTablesQuery)
        impl.data_source_connection.execute_query.assert_not_called()


class TestGetLocationRemoved:
    def test_get_location_is_gone(self):
        impl = _make_impl()

        assert not hasattr(impl, "get_location")
