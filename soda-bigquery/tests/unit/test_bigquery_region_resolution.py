from types import SimpleNamespace
from unittest.mock import MagicMock

from google.api_core.exceptions import NotFound
from google.cloud.bigquery.dataset import DatasetListItem
from soda_bigquery.common.data_sources.bigquery_data_source import (
    BigQueryDataSourceImpl,
    BigQueryMetadataTablesQuery,
    BigQuerySqlDialect,
)
from soda_bigquery.common.data_sources.bigquery_data_source_connection import BigQueryJSONStringAuth
from soda_core.common.data_source_results import QueryResult

PROJECT = "test-project"
STORAGE_PROJECT = "storage-project"


def _make_impl(location: str | None = None, storage_project_id: str | None = PROJECT) -> BigQueryDataSourceImpl:
    impl = BigQueryDataSourceImpl.__new__(BigQueryDataSourceImpl)
    impl.data_source_model = MagicMock()
    # The real connection properties, not a mock: the model is frozen, so anything
    # that reaches for `location` as a writable pin has to fail here rather than in CI.
    impl.data_source_model.connection_properties = BigQueryJSONStringAuth(account_info_json="{}", location=location)
    impl.data_source_connection = MagicMock()
    impl.data_source_connection.project_id = PROJECT
    impl.data_source_connection.storage_project_id = storage_project_id
    impl._dataset_region_cache = {}
    return impl


def _dataset_list_item(dataset_id: str, location: str | None = None) -> DatasetListItem:
    resource = {"datasetReference": {"projectId": PROJECT, "datasetId": dataset_id}}
    if location:
        resource["location"] = location
    return DatasetListItem(resource)


def _mock_datasets(impl: BigQueryDataSourceImpl, locations_by_dataset_id: dict[str, str]) -> None:
    impl.data_source_connection.client.list_datasets.return_value = [
        _dataset_list_item(dataset_id) for dataset_id in locations_by_dataset_id
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

    def test_listed_dataset_locations_spare_the_per_dataset_lookup(self):
        impl = _make_impl()
        impl.data_source_connection.client.list_datasets.return_value = [
            _dataset_list_item("dataset_eu", location="EU"),
            _dataset_list_item("dataset_us", location="US"),
        ]

        assert impl.regions_in_scope(project_id=PROJECT) == ["EU", "US"]
        assert impl.region_for_dataset(PROJECT, "dataset_eu") == "EU"
        impl.data_source_connection.client.get_dataset.assert_not_called()

    def test_listed_dataset_without_location_is_looked_up(self):
        impl = _make_impl()
        impl.data_source_connection.client.list_datasets.return_value = [
            _dataset_list_item("dataset_eu", location="EU"),
            _dataset_list_item("dataset_unknown"),
        ]
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="US")

        assert impl.regions_in_scope(project_id=PROJECT) == ["EU", "US"]
        impl.data_source_connection.client.get_dataset.assert_called_once_with(f"{PROJECT}.dataset_unknown")

    def test_dataset_region_lookups_are_cached(self):
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="EU")

        impl.regions_in_scope(project_id=PROJECT, dataset_id="dataset_a")
        impl.regions_in_scope(project_id=PROJECT, dataset_id="dataset_a")

        impl.data_source_connection.client.get_dataset.assert_called_once()

    def test_missing_dataset_has_no_regions_in_scope(self):
        """A dataset that does not exist holds no tables, so there is nothing to query and nothing to raise."""
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.side_effect = NotFound("no such dataset")

        assert impl.regions_in_scope(project_id=PROJECT, dataset_id="never_created") == []

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

    def test_missing_dataset_returns_no_tables_without_querying(self):
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.side_effect = NotFound("no such dataset")

        query = self._make_query(impl, lambda sql: [])
        results = query.execute(database_name=PROJECT, schema_name="never_created")

        assert results == []
        impl.data_source_connection.execute_query.assert_not_called()

    def test_without_project_in_scope_lists_and_reads_the_storage_project(self):
        impl = _make_impl(storage_project_id=STORAGE_PROJECT)
        _mock_datasets(impl, {"dataset_eu": "EU"})

        def rows_for_sql(sql: str):
            if f"`{STORAGE_PROJECT}`.`region-EU`" in sql:
                return [(STORAGE_PROJECT, "dataset_eu", "orders", "BASE TABLE")]
            raise AssertionError(f"unexpected SQL: {sql}")

        query = self._make_query(impl, rows_for_sql)
        results = query.execute()

        assert [(fully_qualified.database_name, fully_qualified.get_object_name()) for fully_qualified in results] == [
            (STORAGE_PROJECT, "orders")
        ]
        impl.data_source_connection.client.list_datasets.assert_called_once_with(project=STORAGE_PROJECT)
        impl.data_source_connection.client.get_dataset.assert_called_once_with(f"{STORAGE_PROJECT}.dataset_eu")

    def test_project_in_scope_wins_over_the_storage_project(self):
        impl = _make_impl(storage_project_id=STORAGE_PROJECT)
        _mock_datasets(impl, {"dataset_eu": "EU"})

        query = self._make_query(impl, lambda sql: [(PROJECT, "dataset_eu", "orders", "BASE TABLE")])
        query.execute(database_name=PROJECT)

        impl.data_source_connection.client.list_datasets.assert_called_once_with(project=PROJECT)
        executed_sql = impl.data_source_connection.execute_query.call_args.args[0]
        assert f"`{PROJECT}`.`region-EU`" in executed_sql
        assert STORAGE_PROJECT not in executed_sql

    def test_known_dataset_in_another_project_is_read_from_that_project(self):
        # Jobs run in PROJECT while the scope names a dataset in STORAGE_PROJECT, as the diagnostics
        # warehouse does when it writes to the storage project.
        impl = _make_impl(storage_project_id=STORAGE_PROJECT)
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="EU")

        def rows_for_sql(sql: str):
            if f"`{STORAGE_PROJECT}`.`region-EU`" in sql:
                return [(STORAGE_PROJECT, "diagnostics", "scans", "BASE TABLE")]
            # The region view of the project that runs the job holds none of the storage project's tables.
            return []

        query = self._make_query(impl, rows_for_sql)
        results = query.execute(database_name=STORAGE_PROJECT, schema_name="diagnostics")

        assert [(fully_qualified.database_name, fully_qualified.get_object_name()) for fully_qualified in results] == [
            (STORAGE_PROJECT, "scans")
        ]
        impl.data_source_connection.client.get_dataset.assert_called_once_with(f"{STORAGE_PROJECT}.diagnostics")

    def test_without_any_project_the_region_view_stays_unqualified(self):
        impl = _make_impl(storage_project_id=None)
        impl.data_source_connection.project_id = None
        _mock_datasets(impl, {"dataset_eu": "EU"})

        query = self._make_query(impl, lambda sql: [])
        query.execute()

        executed_sql = impl.data_source_connection.execute_query.call_args.args[0]
        assert "FROM `region-EU`.`INFORMATION_SCHEMA`" in executed_sql
        impl.data_source_connection.client.list_datasets.assert_called_once_with(project=None)


class TestCreateMetadataTablesQuery:
    def test_returns_region_aware_query_without_running_probe_queries(self):
        impl = _make_impl()
        impl.sql_dialect = BigQuerySqlDialect()

        query = impl.create_metadata_tables_query()

        assert isinstance(query, BigQueryMetadataTablesQuery)
        impl.data_source_connection.execute_query.assert_not_called()


class TestSnapshotRegionPinning:
    def test_pinned_region_resolution_touches_no_bigquery_client(self, monkeypatch):
        from soda_bigquery.test_helpers.bigquery_data_source_test_helper import BigQueryDataSourceTestHelper

        monkeypatch.setenv("BIGQUERY_LOCATION", "europe-west1")
        impl = _make_impl()
        helper = BigQueryDataSourceTestHelper.__new__(BigQueryDataSourceTestHelper)

        helper._snapshot_pin_metadata_lookups(impl)

        assert impl.regions_in_scope(project_id=PROJECT) == ["europe-west1"]
        assert impl.region_for_dataset(PROJECT, "dataset_a") == "europe-west1"
        impl.data_source_connection.client.list_datasets.assert_not_called()
        impl.data_source_connection.client.get_dataset.assert_not_called()

    def test_pinned_unscoped_query_reads_nothing_but_sql_from_the_connection(self, monkeypatch):
        from soda_bigquery.test_helpers.bigquery_data_source_test_helper import BigQueryDataSourceTestHelper

        monkeypatch.setenv("BIGQUERY_LOCATION", "EU")
        monkeypatch.setenv("BIGQUERY_ACCOUNT_INFO_JSON", '{"project_id": "account-project"}')
        impl = _make_impl()
        # In replay, reading any other attribute of the connection opens a real one. This connection
        # can only run SQL, so such a read fails the test instead.
        impl.data_source_connection = MagicMock(spec=["execute_query"])
        impl.data_source_connection.execute_query.return_value = QueryResult(rows=[], columns=None)
        helper = BigQueryDataSourceTestHelper.__new__(BigQueryDataSourceTestHelper)
        helper._snapshot_pin_metadata_lookups(impl)
        query = BigQueryMetadataTablesQuery(
            sql_dialect=BigQuerySqlDialect(),
            data_source_connection=impl.data_source_connection,
            data_source_impl=impl,
        )

        query.execute()

        executed_sql = impl.data_source_connection.execute_query.call_args.args[0]
        assert "`account-project`.`region-EU`" in executed_sql


class TestGetLocationRemoved:
    def test_get_location_is_gone(self):
        impl = _make_impl()

        assert not hasattr(impl, "get_location")
