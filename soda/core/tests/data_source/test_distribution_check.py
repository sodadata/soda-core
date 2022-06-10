import os
from textwrap import dedent

import pytest
from tests.helpers.common_test_tables import customers_dist_check_test_table
from tests.helpers.fixtures import test_data_source
from tests.helpers.scanner import Scanner


@pytest.mark.skipif(
    test_data_source == "athena",
    reason="TODO: fix for athena.",
)
def test_distribution_check(scanner: Scanner, mock_file_system):
    table_name = scanner.ensure_test_table(customers_dist_check_test_table)

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

    scan = scanner.create_test_scan()
    scan._configuration.file_system = mock_file_system
    scan.add_sodacl_yaml_file(f"{user_home_dir}/the_distribution_check_file.yml")
    scan.execute()
    os.remove(ref_file)


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

    scan = scanner.create_test_scan()
    scan._configuration.file_system = mock_file_system
    scan.add_sodacl_yaml_file(f"{user_home_dir}/the_distribution_check_file.yml")
    scan.execute()

    os.remove(ref_file)
    assert scan._checks[0].query.sql == expectation.format(table_name=table_name)
