from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture


def test_default_missing(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(id) = 1
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_column_configured_missing_values(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - missing_count(id) = 2:
            missing values: [N/A, ID5]
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_check_configured_missing_values(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - missing_count(id) = 2:
            missing values:
             - ID5
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_missing_format(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - missing_count(country) = 0:
            missing format: integer
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
