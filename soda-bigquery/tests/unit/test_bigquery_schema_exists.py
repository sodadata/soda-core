"""Dataset existence on BigQuery is looked up with datasets.get, not with INFORMATION_SCHEMA.SCHEMATA.

The project-level SCHEMATA view only lists datasets in the query job's location, so a dataset in another
region looked missing and the diagnostics warehouse tried to create it, which needs bigquery.datasets.create.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import Forbidden, NotFound
from soda_bigquery.common.data_sources.bigquery_data_source import BigQueryDataSourceImpl
from soda_bigquery.common.data_sources.bigquery_data_source_connection import BigQueryJSONStringAuth

PROJECT = "test-project"
STORAGE_PROJECT = "storage-project"


def _make_impl(location: str | None = None) -> BigQueryDataSourceImpl:
    impl = BigQueryDataSourceImpl.__new__(BigQueryDataSourceImpl)
    impl.data_source_model = MagicMock()
    impl.data_source_model.connection_properties = BigQueryJSONStringAuth(account_info_json="{}", location=location)
    impl.data_source_connection = MagicMock()
    impl.data_source_connection.project_id = PROJECT
    impl.data_source_connection.storage_project_id = PROJECT
    impl._dataset_region_cache = {}
    return impl


def _mock_datasets(impl: BigQueryDataSourceImpl, locations_by_reference: dict[str, str]) -> None:
    def get_dataset(reference: str):
        if reference not in locations_by_reference:
            raise NotFound(f"Not found: Dataset {reference}")
        return SimpleNamespace(location=locations_by_reference[reference])

    impl.data_source_connection.client.get_dataset.side_effect = get_dataset


class TestSchemaExists:
    def test_existing_dataset_outside_the_us_exists(self):
        impl = _make_impl()
        _mock_datasets(impl, {f"{PROJECT}.soda_diagnostics": "europe-west4"})

        assert impl.test_schema_exists([PROJECT, "soda_diagnostics"]) is True
        impl.data_source_connection.client.get_dataset.assert_called_once_with(f"{PROJECT}.soda_diagnostics")

    def test_runs_no_query_job(self):
        impl = _make_impl()
        _mock_datasets(impl, {f"{PROJECT}.soda_diagnostics": "EU"})

        impl.test_schema_exists([PROJECT, "soda_diagnostics"])

        impl.data_source_connection.execute_query.assert_not_called()

    def test_missing_dataset_does_not_exist(self):
        impl = _make_impl()
        _mock_datasets(impl, {})

        assert impl.test_schema_exists([PROJECT, "soda_diagnostics"]) is False

    def test_configured_location_does_not_make_a_missing_dataset_exist(self):
        # region_for_dataset returns a configured location without a lookup; existence must still look.
        impl = _make_impl(location="europe-west4")
        _mock_datasets(impl, {})

        assert impl.test_schema_exists([PROJECT, "soda_diagnostics"]) is False
        impl.data_source_connection.client.get_dataset.assert_called_once_with(f"{PROJECT}.soda_diagnostics")

    def test_looks_in_the_project_of_the_prefixes(self):
        impl = _make_impl()
        _mock_datasets(impl, {f"{STORAGE_PROJECT}.soda_diagnostics": "US"})

        assert impl.test_schema_exists([STORAGE_PROJECT, "soda_diagnostics"]) is True
        assert impl.test_schema_exists([PROJECT, "soda_diagnostics"]) is False

    def test_a_dataset_created_after_a_miss_is_found(self):
        impl = _make_impl()
        locations_by_reference: dict[str, str] = {}
        _mock_datasets(impl, locations_by_reference)
        assert impl.test_schema_exists([PROJECT, "soda_diagnostics"]) is False

        locations_by_reference[f"{PROJECT}.soda_diagnostics"] = "US"

        assert impl.test_schema_exists([PROJECT, "soda_diagnostics"]) is True

    def test_access_denied_propagates(self):
        impl = _make_impl()
        denied = Forbidden(
            f"Access Denied: Dataset {PROJECT}:soda_diagnostics: Permission bigquery.datasets.get denied",
            errors=[{"reason": "accessDenied", "domain": "global"}],
        )
        impl.data_source_connection.client.get_dataset.side_effect = denied

        with pytest.raises(Forbidden):
            impl.test_schema_exists([PROJECT, "soda_diagnostics"])

    @pytest.mark.parametrize("prefixes", [["soda_diagnostics"], [PROJECT, "a", "b"]])
    def test_rejects_prefixes_that_are_not_project_and_dataset(self, prefixes: list[str]):
        impl = _make_impl()

        with pytest.raises(ValueError):
            impl.test_schema_exists(prefixes)
