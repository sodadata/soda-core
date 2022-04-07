from helpers.mock_soda_cloud import MockSodaCloud
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_profile_columns(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.activate_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          profile columns:
            columns: ['%{table_name}%.%']
        """
    )
    scan.execute()

    distance_profiling_result = scan._profile_columns_result_tables[0].result_columns[0]
    assert 'distance' == distance_profiling_result.column_name
    assert [-999, 0, 5, 10, 999, None] == distance_profiling_result.mins

    profiling_result = mock_soda_cloud.scan_result_dicts[0]['profiling']
    customer_column_profiles = profiling_result[0]['columnProfiles']
    distance_profile = customer_column_profiles[0]

    assert distance_profile['columnName'] == 'distance'
    assert distance_profile['profile']['mins'] == [-999, 0, 5, 10, 999, None]
