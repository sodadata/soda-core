from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from soda.sampler.default_sampler import DefaultSampler


def test_missing_count_sample(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(id) = 1
        """
    )
    scan.execute()

    diagnostics = mock_soda_cloud.find_check_diagnostics(0)
    assert diagnostics["value"] == 1
    assert_missing_sample(mock_soda_cloud, 0)


def test_missing_count_sample_disabled(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    mock_soda_cloud.disable_collecting_warehouse_data = True
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(id) = 1
        """
    )
    scan.execute()

    diagnostics = mock_soda_cloud.find_check_diagnostics(0)
    assert diagnostics["value"] == 1
    assert len(mock_soda_cloud.files) == 0
    assert isinstance(scan._configuration.sampler, DefaultSampler)


def test_missing_percent_sample(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_percent(id) < 20 %
        """
    )
    scan.execute()

    assert_missing_sample(mock_soda_cloud, 0)


def assert_missing_sample(mock_soda_cloud, check_index):
    diagnostics = mock_soda_cloud.find_check_diagnostics(check_index)
    failed_rows_file = diagnostics["failedRowsFile"]
    columns = failed_rows_file["columns"]
    assert columns[0]["name"].lower() == "id"
    assert columns[1]["name"].lower() == "size"
    assert failed_rows_file["totalRowCount"] == 1
    assert failed_rows_file["storedRowCount"] == 1
    reference = failed_rows_file["reference"]
    assert reference["type"] == "sodaCloudStorage"
    file_id = reference["fileId"]
    assert isinstance(file_id, str)
    assert len(file_id) > 0

    file_content = mock_soda_cloud.find_file_content_by_file_id(file_id)
    assert file_content.startswith("[null, ")


def test_various_valid_invalid_sample_combinations(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
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

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 5
    assert mock_soda_cloud.find_failed_rows_line_count(1) == 5
    assert "failedRowsFile" not in mock_soda_cloud.find_check_diagnostics(2).keys()
    assert "failedRowsFile" not in mock_soda_cloud.find_check_diagnostics(3).keys()
    assert mock_soda_cloud.find_failed_rows_line_count(4) == 8
    assert mock_soda_cloud.find_failed_rows_line_count(5) == 8
    assert mock_soda_cloud.find_failed_rows_line_count(6) == 2
    assert mock_soda_cloud.find_failed_rows_line_count(7) == 2


def test_duplicate_samples(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
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

    assert mock_soda_cloud.find_failed_rows_line_count(0) == 1
    assert mock_soda_cloud.find_failed_rows_line_count(1) == 1


def test_duplicate_without_rows_samples(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.enable_mock_sampler()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - duplicate_count(id) = 0
        """
    )
    scan.execute_unchecked()

    assert len(mock_soda_cloud.files) == 0
