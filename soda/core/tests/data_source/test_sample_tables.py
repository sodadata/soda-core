from __future__ import annotations

from tests.helpers.common_test_tables import (
    customers_dist_check_test_table,
    customers_profiling,
    customers_test_table,
    orders_test_table,
)
from tests.helpers.scanner import Scanner


def test_sample_tables(scanner: Scanner):
    table_name = scanner.ensure_test_table(orders_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          sample datasets:
            tables:
              - include {table_name}
        """
    )
    scan.execute()
    # remove the data source name because it's a pain to test
    discover_tables_result = mock_soda_cloud.pop_scan_result()

    assert discover_tables_result is not None
    profilings = discover_tables_result["profiling"]
    profiling = profilings[0]
    sample_file = profiling["sampleFile"]
    columns = sample_file["columns"]
    casify = scanner.data_source.default_casify_column_name
    assert [c["name"] for c in columns] == [
        casify(c)
        for c in [
            "id",
            "customer_id_nok",
            "customer_id_ok",
            "customer_country",
            "customer_zip",
            "text",
        ]
    ]
    for c in columns:
        type = c["type"]
        assert isinstance(type, str)
        assert len(type) > 0
    assert sample_file["totalRowCount"] == 7
    assert sample_file["storedRowCount"] == 7

    reference = sample_file["reference"]
    type = reference["type"]
    assert type == "sodaCloudStorage"
    file_id = reference["fileId"]
    assert isinstance(file_id, str)
    assert len(file_id) > 0


def test_discover_tables_customer_wildcard_incl_only(scanner: Scanner):
    scanner.ensure_test_table(customers_test_table)
    orders_test_table_name = scanner.ensure_test_table(orders_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
        sample datasets:
          tables:
            - include %{orders_test_table_name[:-2]}%
        """
    )
    scan.execute()
    discover_tables_result = mock_soda_cloud.pop_scan_result()
    assert discover_tables_result is not None
    profilings = discover_tables_result["profiling"]
    dataset_names = [profiling["table"].lower() for profiling in profilings]
    assert dataset_names == [orders_test_table_name.lower()]


def test_discover_tables_customer_wildcard_incl_excl(scanner: Scanner):
    scanner.ensure_test_table(customers_test_table)
    scanner.ensure_test_table(orders_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
        sample datasets:
          tables:
            - include %sodatest_%
            - exclude %orders%
            - exclude %profiling%
        """
    )
    scan.execute()
    discover_tables_result = mock_soda_cloud.pop_scan_result()
    profilings = discover_tables_result["profiling"]
    dataset_names = [profiling["table"] for profiling in profilings]
    for dataset_name in dataset_names:
        assert "order" not in dataset_name.lower()
        assert "profiling" not in dataset_name.lower()
