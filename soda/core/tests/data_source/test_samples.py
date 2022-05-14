import logging
from typing import Set, Iterable

from soda.common.yaml_helper import to_yaml_str
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.mock_soda_cloud import MockSodaCloud
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
    # logging.debug(to_yaml_str(scan_result))
    soda_cloud_api_check_result_dict = scan_result['checks'][0]
    assert 'missing_count(id) = 1' in soda_cloud_api_check_result_dict['definition']
    diagnostics = soda_cloud_api_check_result_dict['diagnostics']
    assert diagnostics['value'] == 1

    assert_missing_sample(diagnostics, mock_soda_cloud)


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
    # logging.debug(to_yaml_str(scan_result))
    soda_cloud_api_check_result_dict = scan_result['checks'][0]
    assert 'missing_percent(id) < 20 %' in soda_cloud_api_check_result_dict['definition']
    diagnostics = soda_cloud_api_check_result_dict['diagnostics']
    assert diagnostics['value'] == 10.0

    assert_missing_sample(diagnostics, mock_soda_cloud)


def assert_missing_sample(diagnostics, mock_soda_cloud):
    failed_rows_file = diagnostics['failedRowsFile']
    columns = failed_rows_file['columns']
    assert columns[0]['name'] == 'id'
    assert columns[1]['name'] == 'size'
    assert failed_rows_file['totalRowCount'] == 1
    assert failed_rows_file['storedRowCount'] == 1
    reference = failed_rows_file['reference']
    assert reference['type'] == 'sodaCloudStorage'
    file_id = reference['fileId']
    assert isinstance(file_id, str)
    assert len(file_id) > 0

    file_content = mock_soda_cloud.find_file_content_by_file_id(file_id)
    assert file_content.startswith('[null, ')


def test_various_valid_invalid_sample_combinations(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(cat) > 0
            - missing_percent(cat) > 0
            - invalid_count(cat) > 0
            - invalid_percent(cat) > 0
            - missing_count(cat) > 0:
                missing values: ['HIGH']
            - missing_percent(cat) > 0:
                missing values: ['HIGH']
            - invalid_count(cat) > 0:
                valid values: ['HIGH']
            - invalid_percent(cat) > 0:
                valid values: ['HIGH']
        """
    )
    scan.execute_unchecked()

    assert failed_rows_line_count(mock_soda_cloud, 0) == 5
    assert failed_rows_line_count(mock_soda_cloud, 1) == 5
    assert 'failedRowsFile' not in get_diagnostics_keys(mock_soda_cloud, 2)
    assert 'failedRowsFile' not in get_diagnostics_keys(mock_soda_cloud, 3)
    assert failed_rows_line_count(mock_soda_cloud, 4) == 8
    assert failed_rows_line_count(mock_soda_cloud, 5) == 8
    assert failed_rows_line_count(mock_soda_cloud, 6) == 2
    assert failed_rows_line_count(mock_soda_cloud, 7) == 2


def get_diagnostics_keys(mock_soda_cloud: MockSodaCloud, check_index: int) -> Iterable[str]:
    check_result = mock_soda_cloud.find_check_result(check_index)
    diagnostics = check_result['diagnostics']
    return diagnostics.keys()


def failed_rows_line_count(mock_soda_cloud: MockSodaCloud, check_index: int) -> int:
    check_result = mock_soda_cloud.find_check_result(check_index)
    diagnostics = check_result['diagnostics']
    assert 'failedRowsFile' in diagnostics
    failed_rows_file = diagnostics['failedRowsFile']
    reference = failed_rows_file['reference']
    file_id = reference['fileId']
    file_contents = mock_soda_cloud.find_file_content_by_file_id(file_id)
    return file_contents.count('\n')


def test_duplicate_samples(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - duplicate_count(cat) > 0
            - duplicate_count(cat, country) > 0
        """
    )
    scan.execute_unchecked()

    assert failed_rows_line_count(mock_soda_cloud, 0) == 1
    assert failed_rows_line_count(mock_soda_cloud, 1) == 1
