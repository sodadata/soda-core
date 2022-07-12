from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture


def test_numeric_metric_checks_on_text_column(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - min(sizeTxt) = -3
            - max(sizeTxt) = 6
            - avg(sizeTxt) between 1.12 and 1.13
            - sum(sizeTxt) = 7.9
            - min(pct) = -28.42
            - max(pct) = 22.75
          configurations for {table_name}:
            valid format for sizeTxt: decimal
            valid format for pct: percentage
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_numeric_metric_checks_on_text_column_local_format(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - min(sizeTxt) = -3:
                valid format: decimal
            - max(sizeTxt) = 6:
                valid format: decimal
            - min(pct) = -28.42:
                valid format: percentage
            - max(pct) = 22.75:
                valid format: percentage
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()
