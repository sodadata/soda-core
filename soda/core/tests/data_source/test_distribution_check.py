from textwrap import dedent

import pytest
from tests.helpers.common_test_tables import customers_dist_check_test_table
from tests.helpers.data_source_fixture import DataSourceFixture
from tests.helpers.fixtures import test_data_source


# @pytest.mark.skipif(
#     test_data_source == "athena",
#     reason="TODO: fix for athena.",
# )
def test_distribution_check(data_source_fixture: DataSourceFixture, mock_file_system):
    table_name = data_source_fixture.ensure_test_table(customers_dist_check_test_table)
    table_name = data_source_fixture.data_source.default_casify_table_name(table_name)

    scan = data_source_fixture.create_test_scan()

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/customers_size_distribution_reference.yml": dedent(
            f"""
            dataset: {table_name}
            column: size
            distribution_type: continuous
            distribution_reference:
                bins: [1, 2, 3]
                weights: [0.5, 0.2, 0.3]
        """
        ).strip(),
    }

    scan.add_sodacl_yaml_str(
        f"""
        checks for {table_name}:
            - distribution_difference(size, my_happy_ml_model_distribution) >= 0.05:
                distribution reference file: {user_home_dir}/customers_size_distribution_reference.yml
                method: ks
    """
    )

    scan.enable_mock_soda_cloud()
    scan.execute()


@pytest.mark.parametrize(
    "table, expectation",
    [
        pytest.param(
            customers_dist_check_test_table, "SELECT \n  size \nFROM {schema_name}{table_name}\n LIMIT 1000000"
        ),
    ],
)
# @pytest.mark.skipif(
#     test_data_source == "athena",
#     reason="TODO: fix for athena.",
# )
def test_distribution_sql(data_source_fixture: DataSourceFixture, mock_file_system, table, expectation):
    table_name = data_source_fixture.ensure_test_table(table)

    user_home_dir = os.path.expanduser("~")

    mock_file_system.files = {
        f"{user_home_dir}/the_distribution_check_file.yml": dedent(
            f"""
                checks for {table_name}:
                  - distribution_difference(size, my_happy_ml_model_distribution) >= 0.05:
                      distribution reference file: ./customers_size_distribution_reference.yml
                      method: ks
            """
        ).strip(),
    }
    # TODO: do this logic at mock file system
    ref_file = f"{user_home_dir}/customers_size_distribution_reference.yml"
    with open(ref_file, "w") as f:
        reference_table = f"""
            table: {table_name}
            column: size
            distribution_type: continuous
            distribution reference:
                bins: [1, 2, 3]
                weights: [0.5, 0.2, 0.3]
        """
        f.write(reference_table)

    scan = data_source_fixture.create_test_scan()
    scan._configuration.file_system = mock_file_system
    scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_file(f"{user_home_dir}/the_distribution_check_file.yml")
    scan.execute()

    os.remove(ref_file)
    # TODO: We might want to put this into a helper at some point if we're to use this
    # in more than one test down the line.
    if test_data_source != "spark_df":
        assert scan._checks[0].query.sql == expectation.format(
            table_name=table_name, schema_name=f"{data_source_fixture.schema_name}."
        )
    else:
        assert scan._checks[0].query.sql == expectation.format(table_name=table_name, schema_name="")
