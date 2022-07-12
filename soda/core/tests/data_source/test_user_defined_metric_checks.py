from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from soda.execution.check_outcome import CheckOutcome


def test_user_defined_table_expression_metric_check(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
      checks for {table_name}:
        - avg_surface between 1068 and 1069:
            avg_surface expression: AVG(size * distance)
        - ones(sizeTxt):
            name: There must be 3 occurences of 1 in sizeText
            ones expression: SUM(LENGTH(sizeTxt) - LENGTH(REPLACE(sizeTxt, '1', '')))
            warn: when < 3
            fail: when < 2
    """
    )
    scan.execute()

    avg_surface_check = scan._checks[0]
    avg_surface = avg_surface_check.check_value
    assert isinstance(avg_surface, float)
    assert 1068 < avg_surface < 1069
    assert avg_surface_check.outcome == CheckOutcome.PASS

    ones_check = scan._checks[1]
    assert isinstance(ones_check.check_value, int)
    assert ones_check.check_value == 2
    assert ones_check.check_cfg.name == "There must be 3 occurences of 1 in sizeText"
    assert ones_check.outcome == CheckOutcome.WARN


def test_user_defined_data_source_query_metric_check(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
          checks:
            - avg_surface between 1068 and 1069:
                avg_surface query: |
                  SELECT AVG(size * distance) as avg_surface
                  FROM {qualified_table_name}
        """
    )
    scan.execute()

    scan.assert_all_checks_pass()

    avg_surface = scan._checks[0].check_value
    assert isinstance(avg_surface, float)
    assert 1068 < avg_surface < 1069
