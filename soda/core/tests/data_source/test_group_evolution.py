import pytest
from helpers.common_test_tables import customers_test_table
from helpers.data_source_fixture import DataSourceFixture
from helpers.fixtures import test_data_source


@pytest.mark.skipif(
    test_data_source not in ["postgres", "bigquery", "spark_df"],
    reason="Need to make tests work with lower and upper case values for column names",
)
def test_group_evolution(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan = data_source_fixture.create_test_scan()
    scan.add_sodacl_yaml_str(
        f"""
            checks for {table_name}:
              - group evolution:
                  query: |
                    SELECT country
                    FROM {table_name}
                    GROUP BY country
                  warn:
                    when groups change: any
                  fail:
                    when required group missing: ["UK"]
                    when forbidden group present: ["US"]
                    when groups change:
                        - group add
                        - group delete

    """
    )
    scan.execute()

    scan.assert_all_checks_pass()
