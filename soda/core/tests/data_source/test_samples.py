import logging

from soda.common.yaml_helper import to_yaml_str
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


def test_missing_count_sample(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(id) = 1
        """
    )
    scan.execute()

    scan_result = mock_soda_cloud.pop_scan_result()
    logging.debug(to_yaml_str(scan_result))
    soda_cloud_api_check_result_dict = scan_result['checks'][0]
    assert 'missing_count(id) = 1' in soda_cloud_api_check_result_dict['definition']
    diagnostics = soda_cloud_api_check_result_dict['diagnostics']
    assert diagnostics['value'] == 1

    assert_missing_sample(diagnostics)


def test_missing_percent_sample(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_percent(id) < 20 %
        """
    )
    scan.execute()

    scan_result = mock_soda_cloud.pop_scan_result()
    logging.debug(to_yaml_str(scan_result))
    soda_cloud_api_check_result_dict = scan_result['checks'][0]
    assert 'missing_percent(id) < 20 %' in soda_cloud_api_check_result_dict['definition']
    diagnostics = soda_cloud_api_check_result_dict['diagnostics']
    assert diagnostics['value'] == 10.0

    assert_missing_sample(diagnostics)


def assert_missing_sample(diagnostics):
    failed_rows_file = diagnostics['failedRowsFile']
    columns = failed_rows_file['columns']
    assert columns[0]['name'] == 'id'
    assert columns[1]['name'] == 'size'
    assert failed_rows_file['totalRowCount'] == 1
    assert failed_rows_file['storedRowCount'] == 1
    reference = failed_rows_file['reference']
    assert reference['type'] == 'sodaCloudStorage'
    assert isinstance(reference['fileId'], str)
    assert len(reference['fileId']) > 0
