from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture


def test_default_invalid(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - invalid_count(id) = 0
        - valid_count(id) = 9
    """
    )
    scan.execute_unchecked()

    scan.assert_log_warning("Counting invalid without valid specification does not make sense")
    scan.assert_all_checks_pass()


def test_column_configured_invalid_values(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - invalid_count(id) = 6
        - valid_count(id) = 3
      configurations for {table_name}:
        valid values for id:
         - ID1
         - ID2
         - ID3
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_valid_min_max(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - invalid_count(size) = 3:
            valid min: 0
        - invalid_count(size) = 4:
            valid max: 0
    """
    )
    scan.execute()
    scan.assert_all_checks_pass()


def test_valid_format_email(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - invalid_count(email) = 1:
                valid format: email
            - missing_count(email) = 5
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_column_configured_invalid_and_missing_values(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - missing_count(pct) = 3
            - invalid_count(pct) = 1
            - valid_count(pct) = 6
          configurations for {table_name}:
            missing values for pct: ['N/A', 'No value']
            valid format for pct: percentage
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_valid_length(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - invalid_count(cat) = 2
            - valid_count(cat) = 3
          configurations for {table_name}:
            valid min length for cat: 4
            valid max length for cat: 4
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - invalid_count(cat) = 2
            - valid_count(cat) = 3
          configurations for {table_name}:
            valid length for cat: 4
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_check_and_column_configured_invalid_values(data_source_fixture: DataSourceFixture):
    """
    In case both column *and* check configurations are specified, they both are applied.
    """
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    digit_regex = data_source_fixture.data_source.escape_regex(r"ID\d")

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - valid_count(id) = 9
            - valid_count(id) = 2:
                valid values:
                 - ID1
                 - ID2
            - invalid_count(id) = 0
            - invalid_count(id) = 7:
                valid values:
                 - ID1
                 - ID2
          configurations for {table_name}:
            valid regex for id: {digit_regex}
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
