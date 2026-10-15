from unittest.mock import Mock

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_fixtures import test_datasource
from helpers.test_table import TestTableSpecification

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("bigquery_region_resolution")
    .column_varchar("id")
    .rows(rows=[("1",)])
    .build()
)


@pytest.mark.no_snapshot  # Snapshots pin region resolution, so only a live run reaches the BigQuery REST API.
@pytest.mark.skipif(
    test_datasource != "bigquery",
    reason="Checks what the BigQuery datasets.list response carries.",
)
def test_dataset_listing_carries_the_region_of_each_dataset(data_source_test_helper: DataSourceTestHelper, monkeypatch):
    data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_impl = data_source_test_helper.data_source_impl
    project_id = data_source_test_helper.extract_database_from_prefix()
    dataset_id = data_source_test_helper.extract_schema_from_prefix()
    client = data_source_impl.data_source_connection.client
    dataset_location = client.get_dataset(f"{project_id}.{dataset_id}").location

    monkeypatch.setattr(data_source_impl, "_dataset_region_cache", {})
    get_dataset = Mock(wraps=client.get_dataset)
    monkeypatch.setattr(client, "get_dataset", get_dataset)

    regions = data_source_impl.regions_in_scope(project_id=project_id)

    assert dataset_location in regions
    assert data_source_impl._dataset_region_cache[(project_id, dataset_id)] == dataset_location
    get_dataset.assert_not_called()
