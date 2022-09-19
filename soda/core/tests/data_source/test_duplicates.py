from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture


def test_duplicates_single_column(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - duplicate_count(cat) = 1
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_duplicates_multiple_columns(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - duplicate_count(cat, country) = 1
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
