import logging
from typing import Dict, List

from helpers.common_test_tables import (
    customers_dist_check_test_table,
    customers_test_table,
)
from helpers.data_source_fixture import DataSourceFixture
from soda.common.yaml_helper import to_yaml_str


def assert_no_duplicate_check_identities(scan_result: dict):
    cloud_checks_by_identity = get_cloud_checks_by_identity(scan_result)

    duplicate_identity_messages = []
    for identity, duplicate_identity_cloud_checks in cloud_checks_by_identity.items():
        if len(duplicate_identity_cloud_checks) > 1:
            duplicate_identity_messages.append(
                f"Duplicate check identities: {len(duplicate_identity_cloud_checks)} checks have identity {identity}:"
            )
            for duplicate_identity_cloud_check in duplicate_identity_cloud_checks:
                location = duplicate_identity_cloud_check["location"]
                file_path = location["filePath"]
                line = location["line"]
                definition = duplicate_identity_cloud_check["definition"]
                duplicate_identity_messages.append(f"{file_path}:{line} {definition}")
            duplicate_identity_messages.append("")

    if duplicate_identity_messages:
        raise AssertionError("\n".join(duplicate_identity_messages))


def get_cloud_checks_by_identity(scan_result) -> Dict[str, List[dict]]:
    logging.debug(to_yaml_str(scan_result))
    cloud_checks_by_identity: Dict[str, List[dict]] = {}
    for cloud_check in scan_result["checks"]:
        identity = cloud_check["identity"]
        cloud_checks_by_identity.setdefault(identity, []).append(cloud_check)
    return cloud_checks_by_identity


def execute_scan_and_get_scan_result(data_source_fixture: DataSourceFixture, sodacl_yaml_str: str) -> dict:
    scan = data_source_fixture.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(sodacl_yaml_str)
    scan.execute()
    return mock_soda_cloud.pop_scan_result()


def test_check_identity_ignore_name(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - row_count > 0
        """,
    )

    row_count_identity = scan_result["checks"][0]["identity"]

    assert isinstance(row_count_identity, str)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - row_count > 0:
                name: Naming this check should not change the identity!
        """,
    )

    # check that the identity remains the same
    assert scan_result["checks"][0]["identity"] == row_count_identity

    logs = scan_result["logs"]
    assert len(logs) > 0
    first_log = logs[0]
    assert first_log.get("level") == "info"
    assert first_log.get("message").startswith("Soda Core 3.")
    first_log_index = first_log.get("index")
    assert isinstance(first_log_index, int) and first_log_index >= 0
    first_log_timestamp = first_log.get("timestamp")
    assert isinstance(first_log_timestamp, str) and len(first_log_timestamp) > 0


def test_check_identity_line_number_change(data_source_fixture: DataSourceFixture):
    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - missing_count(id) < 10
        """,
    )

    missing_identity = scan_result["checks"][0]["identity"]

    assert isinstance(missing_identity, str)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - row_count > 0
            - missing_count(id) < 10
        """,
    )

    assert scan_result["checks"][1]["identity"] == missing_identity


def test_explicitely_specified_check_identity(data_source_fixture: DataSourceFixture):
    # 1. First a Soda Cloud user creates a new check
    # 2. Then the soda cloud user asks the Soda Cloud editor to fill in the identity in the check source so that...
    # 3. The Soda Cloud user can update the check keeping the same identity and hence without loosing the history

    table_name = data_source_fixture.ensure_test_table(customers_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - row_count > 0
        """,
    )

    row_count_identity = scan_result["checks"][0]["identity"]

    assert isinstance(row_count_identity, str)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - row_count > 0:
                identity: {row_count_identity}
        """,
    )

    # check that the identity remains the same
    assert scan_result["checks"][0]["identity"] == row_count_identity

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          checks for {table_name}:
            - row_count > 1:
                identity: {row_count_identity}
        """,
    )

    # check that the identity remains the same after changing the check (threshold in this case)
    assert scan_result["checks"][0]["identity"] == row_count_identity


def test_for_each_identity(data_source_fixture: DataSourceFixture):
    """Tests that same check generated by "for each" clause and manually will have unique identity and definition."""
    customers_table_name = data_source_fixture.ensure_test_table(customers_test_table)
    customers_dist_table_name = data_source_fixture.ensure_test_table(customers_dist_check_test_table)

    scan_result = execute_scan_and_get_scan_result(
        data_source_fixture,
        f"""
          for each dataset D:
            datasets:
                - {customers_table_name}
                - {customers_dist_table_name}
            checks:
            - row_count > 0
          checks for {customers_table_name}:
            - row_count > 0
        """,
    )
    assert scan_result["checks"][0]["identity"] != scan_result["checks"][1]["identity"]
    assert scan_result["checks"][0]["identity"] != scan_result["checks"][2]["identity"]
    assert scan_result["checks"][1]["identity"] != scan_result["checks"][2]["identity"]
