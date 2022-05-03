import os
from textwrap import dedent

import pytest

from tests.helpers.common_test_tables import customers_dist_check_test_table
from tests.helpers.mock_file_system import MockFileSystem
from tests.helpers.scanner import Scanner


@pytest.mark.skip("Unskip after scientific package is re-enabled and/or move to someplace else.")
def test_distribution_check(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_dist_check_test_table)

    # The following files are made available on the mocked file system:
    mock_file_system = MockFileSystem()

    # user_home_dir = mock_file_system.user_home_dir()
    user_home_dir = os.path.expanduser("~")

    mock_file_system.files = {
        f"{user_home_dir}/the_distribution_check_file.yml": dedent(
            f"""
                checks for {table_name}:
                  - distribution_difference(size, my_happy_ml_model_distribution) >= 0.05:
                      distribution reference file: ./customers_size_distribution_reference.yml
            """
        ).strip(),
    }

    # TODO: do this logic at mock file system
    ref_file = f"{user_home_dir}/customers_size_distribution_reference.yml"
    with open(ref_file, "w") as f:
        reference_table = f"""
            table: {table_name}
            column: size
            method: continuous
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
