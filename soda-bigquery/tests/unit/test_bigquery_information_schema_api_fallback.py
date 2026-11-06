"""The BigQuery API fallback for tables metadata when a region INFORMATION_SCHEMA view denies access.

A region view needs table and routine permissions on every dataset in the region; an account that is only
granted some datasets gets a 403 for the whole view. Discovery then lists the region's tables through
``datasets.list`` and ``tables.list``, which return exactly the datasets the account has access to.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from google.cloud.bigquery.dataset import DatasetListItem
from google.cloud.bigquery.dbapi import exceptions as dbapi_exceptions
from google.cloud.bigquery.table import TableListItem
from soda_bigquery.common.data_sources.bigquery_data_source import (
    BigQueryDataSourceImpl,
    BigQueryMetadataTablesQuery,
    BigQuerySqlDialect,
    find_information_schema_access_denied,
    sql_like_to_regex,
    table_name_matches_like_filters,
)
from soda_bigquery.common.data_sources.bigquery_data_source_connection import BigQueryJSONStringAuth
from soda_core.common.data_source_results import QueryResult
from soda_core.common.statements.table_types import (
    FullyQualifiedMaterializedViewName,
    FullyQualifiedTableName,
    FullyQualifiedViewName,
    TableType,
)

PROJECT = "test-project"

# The error text BigQuery returns for a region view, as seen in production scan logs.
INFORMATION_SCHEMA_DENIED = Forbidden(
    f"POST https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries?prettyPrint=false: "
    f"Access Denied: Table {PROJECT}:region-europe-west4.INFORMATION_SCHEMA.TABLES: User does not have the "
    "required permissions ('bigquery.routines.list', 'bigquery.tables.list' permission(s) at the dataset level; "
    "and 'bigquery.routines.get' permission(s) at the routine level; and 'bigquery.tables.get' permission(s) "
    f"at the table level) to query system entity {PROJECT}:region-europe-west4.INFORMATION_SCHEMA.TABLES."
)
JOBS_CREATE_DENIED = Forbidden(
    f"POST https://bigquery.googleapis.com/bigquery/v2/projects/{PROJECT}/queries?prettyPrint=false: "
    f"Access Denied: Project {PROJECT}: User does not have bigquery.jobs.create permission in project {PROJECT}."
)


def _as_raised_by_dbapi(api_error: Exception) -> dbapi_exceptions.DatabaseError:
    """Wrap an API error the way the BigQuery DB-API cursor does: DatabaseError(api_error), raised in its except."""
    try:
        raise api_error
    except Exception as exc:
        try:
            raise dbapi_exceptions.DatabaseError(exc)
        except dbapi_exceptions.DatabaseError as wrapped:
            return wrapped


def _make_impl(location: str | None = None) -> BigQueryDataSourceImpl:
    impl = BigQueryDataSourceImpl.__new__(BigQueryDataSourceImpl)
    impl.data_source_model = MagicMock()
    impl.data_source_model.connection_properties = BigQueryJSONStringAuth(account_info_json="{}", location=location)
    impl.data_source_connection = MagicMock()
    impl.data_source_connection.project_id = PROJECT
    impl.data_source_connection.storage_project_id = PROJECT
    impl._dataset_region_cache = {}
    return impl


def _mock_datasets(impl: BigQueryDataSourceImpl, locations_by_dataset_id: dict[str, str]) -> None:
    impl.data_source_connection.client.list_datasets.return_value = [
        DatasetListItem({"datasetReference": {"projectId": PROJECT, "datasetId": dataset_id}, "location": location})
        for dataset_id, location in locations_by_dataset_id.items()
    ]


def _table_item(dataset_id: str, table_id: str, table_type: str = "TABLE") -> TableListItem:
    return TableListItem(
        {
            "tableReference": {"projectId": PROJECT, "datasetId": dataset_id, "tableId": table_id},
            "type": table_type,
        }
    )


def _mock_tables(impl: BigQueryDataSourceImpl, tables_by_dataset_id: dict[str, object]) -> None:
    """Each value is a list of table items, or an exception that list_tables raises for that dataset."""

    def list_tables(dataset_reference: str):
        tables_or_error = tables_by_dataset_id[dataset_reference.split(".")[-1]]
        if isinstance(tables_or_error, Exception):
            raise tables_or_error
        return iter(tables_or_error)

    impl.data_source_connection.client.list_tables.side_effect = list_tables


def _make_query(impl: BigQueryDataSourceImpl, rows_or_error_for_sql) -> BigQueryMetadataTablesQuery:
    def execute_query(sql: str, **_kwargs) -> QueryResult:
        rows_or_error = rows_or_error_for_sql(sql)
        if isinstance(rows_or_error, Exception):
            raise rows_or_error
        return QueryResult(rows=rows_or_error, columns=None)

    impl.data_source_connection.execute_query.side_effect = execute_query
    return BigQueryMetadataTablesQuery(
        sql_dialect=BigQuerySqlDialect(),
        data_source_connection=impl.data_source_connection,
        data_source_impl=impl,
    )


def _names(results) -> list[tuple[str, str, str]]:
    return sorted((result.database_name, result.schema_name, result.get_object_name()) for result in results)


ALL_TYPES = [TableType.TABLE, TableType.VIEW, TableType.MATERIALIZED_VIEW]


class TestFindInformationSchemaAccessDenied:
    def test_finds_the_api_error_inside_the_dbapi_wrapper(self):
        assert find_information_schema_access_denied(_as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)) is (
            INFORMATION_SCHEMA_DENIED
        )

    def test_finds_an_unwrapped_api_error(self):
        assert find_information_schema_access_denied(INFORMATION_SCHEMA_DENIED) is INFORMATION_SCHEMA_DENIED

    def test_ignores_a_403_that_is_not_about_information_schema(self):
        assert find_information_schema_access_denied(_as_raised_by_dbapi(JOBS_CREATE_DENIED)) is None

    def test_ignores_other_errors_that_mention_information_schema(self):
        bad_request = BadRequest("Syntax error in INFORMATION_SCHEMA query")
        assert find_information_schema_access_denied(_as_raised_by_dbapi(bad_request)) is None


class TestSqlLikeToRegex:
    @pytest.mark.parametrize(
        "pattern, name, matches",
        [
            ("orders", "orders", True),
            ("orders", "ORDERS", True),
            ("orders", "orders_v2", False),
            ("ord%", "orders_v2", True),
            ("%_v2", "orders_v2", True),
            ("%_v2", "v2", False),
            ("o_ders", "orders", True),
            ("o_ders", "oders", False),
            ("a.b", "a.b", True),
            ("a.b", "axb", False),
            ("100\\%", "100%", True),
            ("100\\%", "1000", False),
            ("tmp\\_%", "tmp_orders", True),
            ("tmp\\_%", "tmporders", False),
        ],
    )
    def test_matches_like_bigquery(self, pattern: str, name: str, matches: bool):
        assert bool(sql_like_to_regex(pattern).fullmatch(name)) is matches


class TestApiFallback:
    def test_denied_region_view_lists_the_regions_tables_with_the_api(self):
        impl = _make_impl()
        _mock_datasets(impl, {"sales": "europe-west4", "finance": "europe-west4", "us_only": "US"})
        _mock_tables(
            impl,
            {
                "sales": [_table_item("sales", "orders"), _table_item("sales", "orders_view", "VIEW")],
                "finance": [_table_item("finance", "ledger_mv", "MATERIALIZED_VIEW")],
                "us_only": [_table_item("us_only", "shipments")],
            },
        )

        def rows_or_error_for_sql(sql: str):
            if "region-europe-west4" in sql:
                return _as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)
            return [(PROJECT, "us_only", "shipments", "BASE TABLE")]

        results = _make_query(impl, rows_or_error_for_sql).execute(types_to_return=ALL_TYPES)

        assert _names(results) == [
            (PROJECT, "finance", "ledger_mv"),
            (PROJECT, "sales", "orders"),
            (PROJECT, "sales", "orders_view"),
            (PROJECT, "us_only", "shipments"),
        ]
        by_name = {result.get_object_name(): result for result in results}
        assert isinstance(by_name["orders"], FullyQualifiedTableName)
        assert isinstance(by_name["orders_view"], FullyQualifiedViewName)
        assert isinstance(by_name["ledger_mv"], FullyQualifiedMaterializedViewName)
        # The US region's view answered, so its dataset is not listed through the API.
        listed = sorted(call.args[0] for call in impl.data_source_connection.client.list_tables.call_args_list)
        assert listed == [f"{PROJECT}.finance", f"{PROJECT}.sales"]

    def test_types_default_to_tables_only_like_the_sql_path(self):
        impl = _make_impl()
        _mock_datasets(impl, {"sales": "EU"})
        _mock_tables(impl, {"sales": [_table_item("sales", "orders"), _table_item("sales", "orders_view", "VIEW")]})

        results = _make_query(impl, lambda sql: _as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)).execute()

        assert _names(results) == [(PROJECT, "sales", "orders")]

    def test_table_name_filters_apply_to_the_api_listing(self):
        impl = _make_impl()
        _mock_datasets(impl, {"sales": "EU"})
        _mock_tables(
            impl,
            {
                "sales": [
                    _table_item("sales", "orders"),
                    _table_item("sales", "orders_archive"),
                    _table_item("sales", "Customers"),
                    _table_item("sales", "invoices"),
                ]
            },
        )

        results = _make_query(impl, lambda sql: _as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)).execute(
            include_table_name_like_filters=["order%", "customers"],
        )

        assert _names(results) == [
            (PROJECT, "sales", "Customers"),
            (PROJECT, "sales", "orders"),
            (PROJECT, "sales", "orders_archive"),
        ]

    @pytest.mark.parametrize(
        "table_name, includes, excludes, matches",
        [
            ("orders", None, None, True),
            ("orders", ["order%", "customers"], None, True),
            ("invoices", ["order%", "customers"], None, False),
            ("orders_archive", ["order%"], ["%_archive"], False),
            ("orders", ["order%"], ["%_archive"], True),
            ("orders", None, ["ORD%"], False),
        ],
    )
    def test_table_name_filters_match_the_metadata_query(self, table_name, includes, excludes, matches):
        # Exclude filters are checked here rather than through execute(): building the metadata SQL with an
        # exclude filter fails before any query runs (NOT_LIKE has no operator rendering in SqlDialect).
        assert table_name_matches_like_filters(table_name, includes, excludes) is matches

    def test_dataset_in_scope_lists_only_that_dataset(self):
        impl = _make_impl()
        impl.data_source_connection.client.get_dataset.return_value = SimpleNamespace(location="EU")
        _mock_tables(impl, {"sales": [_table_item("sales", "orders")]})

        results = _make_query(impl, lambda sql: _as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)).execute(
            database_name=PROJECT, schema_name="sales"
        )

        assert _names(results) == [(PROJECT, "sales", "orders")]
        impl.data_source_connection.client.list_datasets.assert_not_called()
        impl.data_source_connection.client.list_tables.assert_called_once_with(f"{PROJECT}.sales")

    def test_configured_location_lists_every_visible_dataset(self):
        # A configured location pins the one region and skips the region lookups, so the fallback takes every
        # dataset the account can see as part of it.
        impl = _make_impl(location="europe-west4")
        _mock_datasets(impl, {"sales": "europe-west4", "finance": "europe-west4"})
        _mock_tables(impl, {"sales": [_table_item("sales", "orders")], "finance": [_table_item("finance", "ledger")]})

        results = _make_query(impl, lambda sql: _as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)).execute()

        assert _names(results) == [(PROJECT, "finance", "ledger"), (PROJECT, "sales", "orders")]

    def test_dataset_whose_tables_cannot_be_listed_is_skipped(self):
        impl = _make_impl()
        _mock_datasets(impl, {"sales": "EU", "restricted": "EU", "deleted": "EU"})
        _mock_tables(
            impl,
            {
                "sales": [_table_item("sales", "orders")],
                "restricted": Forbidden("Access Denied: Dataset test-project:restricted"),
                "deleted": NotFound("Not found: Dataset test-project:deleted"),
            },
        )

        results = _make_query(impl, lambda sql: _as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)).execute()

        assert _names(results) == [(PROJECT, "sales", "orders")]

    def test_fallback_logs_a_warning_with_the_reason(self, caplog):
        impl = _make_impl()
        _mock_datasets(impl, {"sales": "EU"})
        _mock_tables(impl, {"sales": []})

        with caplog.at_level("WARNING"):
            _make_query(impl, lambda sql: _as_raised_by_dbapi(INFORMATION_SCHEMA_DENIED)).execute()

        assert any(
            "listed with the BigQuery API instead" in record.getMessage()
            and "INFORMATION_SCHEMA.TABLES: User does not have the required permissions" in record.getMessage()
            for record in caplog.records
        )

    def test_missing_jobs_create_is_not_hidden_by_the_fallback(self):
        impl = _make_impl()
        _mock_datasets(impl, {"sales": "EU"})

        query = _make_query(impl, lambda sql: _as_raised_by_dbapi(JOBS_CREATE_DENIED))

        with pytest.raises(dbapi_exceptions.DatabaseError):
            query.execute()
        impl.data_source_connection.client.list_tables.assert_not_called()

    def test_other_query_errors_propagate(self):
        impl = _make_impl()
        _mock_datasets(impl, {"sales": "EU"})

        query = _make_query(impl, lambda sql: _as_raised_by_dbapi(BadRequest("Syntax error")))

        with pytest.raises(dbapi_exceptions.DatabaseError):
            query.execute()
        impl.data_source_connection.client.list_tables.assert_not_called()
