from textwrap import dedent

import pytest
from tests.helpers.common_test_tables import customers_dist_check_test_table
from tests.helpers.data_source_fixture import DataSourceFixture
from tests.helpers.fixtures import test_data_source


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
            - distribution_difference(size) >= 0.05:
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
def test_distribution_sql(data_source_fixture: DataSourceFixture, mock_file_system, table, expectation):
    table_name = data_source_fixture.ensure_test_table(table)
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
                - distribution_difference(size) >= 0.05:
                    distribution reference file:  {user_home_dir}/customers_size_distribution_reference.yml
                    method: ks
        """
    )

    scan.enable_mock_soda_cloud()
    scan.execute()
    if test_data_source != "spark_df":
        assert scan._checks[0].query.sql == expectation.format(
            table_name=table_name, schema_name=f"{data_source_fixture.schema_name}."
        )
    else:
        assert scan._checks[0].query.sql == expectation.format(table_name=table_name, schema_name="")


def test_distribution_missing_bins_weights(data_source_fixture: DataSourceFixture, mock_file_system):
    from soda.scientific.distribution.comparison import MissingBinsWeightsException

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
        """
        ).strip(),
    }

    scan.add_sodacl_yaml_str(
        f"""
        checks for {table_name}:
            - distribution_difference(size) >= 0.05:
                distribution reference file: {user_home_dir}/customers_size_distribution_reference.yml
                method: ks
    """
    )

    scan.execute(allow_error_warning=True)

    log_message = (
        'The DRO in your "/Users/johndoe/customers_size_distribution_reference.yml" distribution reference file does'
        ' not contain a "distribution_reference" key with weights and bins. Make sure that before running "soda scan" you'
        ' create a DRO by running "soda update-dro". For more information visit the docs:\nhttps://docs.soda.io/soda-cl/distribution.html#generate-a-distribution-reference-object-dro.'
    )

    log = next(log for log in scan._logs.logs if isinstance(log.message, MissingBinsWeightsException))
    assert log.message.args[0] == log_message


def test_distribution_check_with_dro_name(data_source_fixture: DataSourceFixture, mock_file_system):
    table_name = data_source_fixture.ensure_test_table(customers_dist_check_test_table)
    table_name = data_source_fixture.data_source.default_casify_table_name(table_name)

    scan = data_source_fixture.create_test_scan()

    user_home_dir = mock_file_system.user_home_dir()

    mock_file_system.files = {
        f"{user_home_dir}/customers_size_distribution_reference.yml": dedent(
            f"""
            customers_dro1:
                dataset: {table_name}
                column: size
                distribution_type: continuous
                distribution_reference:
                    bins: [1, 2, 3]
                    weights: [0.5, 0.2, 0.3]

            customers_dro2:
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
            - distribution_difference(size, customers_dro1) >= 0.05:
                distribution reference file: {user_home_dir}/customers_size_distribution_reference.yml
                method: ks
    """
    )

    scan.enable_mock_soda_cloud()
    scan.execute()
