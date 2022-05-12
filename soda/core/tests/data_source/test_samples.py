from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_missing_sample(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    mock_sampler = scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(id) = 1
        """
    )
    scan.execute()

    mock_sampler.samples[0]
