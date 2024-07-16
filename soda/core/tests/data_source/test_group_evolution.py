from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture


def test_group_evolution(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)
    casify = data_source_fixture.data_source.default_casify_column_name

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
            checks for {table_name}:
              - group evolution:
                  query: |
                    SELECT distinct({casify('country')})
                    FROM {qualified_table_name}
                  fail:
                    when required group missing: ["BE"]
                    when forbidden group present: ["US"]
    """
    )
    scan.execute()

    scan.assert_all_checks_pass()


def test_group_evolution_query_multiline(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)
    qualified_table_name = data_source_fixture.data_source.qualified_table_name(table_name)
    casify = data_source_fixture.data_source.default_casify_column_name

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
            checks for {table_name}:
              - group evolution:
                  query: |
                    SELECT distinct({casify('country')})
                    FROM {qualified_table_name}
                  fail:
                    when required group missing: ["BE"]
                    when forbidden group present: ["US"]
    """
    )
    scan.execute()

    # No empty line at the end of the string
    assert scan._queries[0].sql == f"""SELECT distinct({casify('country')})\nFROM {qualified_table_name}"""
