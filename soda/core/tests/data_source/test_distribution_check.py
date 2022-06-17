import os
from textwrap import dedent

import pytest
from tests.helpers.common_test_tables import customers_dist_check_test_table
from tests.helpers.mock_file_system import MockFileSystem
from tests.helpers.fixtures import test_data_source
from tests.helpers.scanner import Scanner


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
def test_distribution_check(scanner: Scanner, mock_file_system: MockFileSystem):
    table_name = scanner.ensure_test_table(customers_dist_check_test_table)
    table_name = scanner.data_source.default_casify_table_name(table_name)
    
    scan = scanner.create_test_scan()
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
        pytest.param(customers_dist_check_test_table, "SELECT \n  size \nFROM {table_name}\n LIMIT 1000000"),
    ],
)
@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
def test_distribution_sql(scanner: Scanner, mock_file_system, table, expectation):
    table_name = scanner.ensure_test_table(table)
    table_name = scanner.data_source.default_casify_table_name(table_name)
    scan = scanner.create_test_scan()
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
                    distribution reference file:  {user_home_dir}/customers_size_distribution_reference.yml
                    method: ks
        """
    )

    scan.enable_mock_soda_cloud()
    scan.execute()
    assert scan._checks[0].query.sql == expectation.format(table_name=table_name)
