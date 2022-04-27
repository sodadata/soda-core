import logging
from typing import Dict, List

from soda.common.yaml_helper import to_yaml_str
from tests.helpers.common_test_tables import customers_test_table
from tests.helpers.scanner import Scanner


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


def test_check_identity_row_count(scanner: Scanner):
    table_name = scanner.ensure_test_table(customers_test_table)

    scan = scanner.create_test_scan()
    mock_soda_cloud = scan.enable_mock_soda_cloud()
    scan.add_sodacl_yaml_str(
        f"""
          checks for {table_name}:
            - row_count > 0
            - row_count > 1
            - row_count > 1:
                name: row count must be greater than one
        """
    )
    scan.execute()

    scan_result = mock_soda_cloud.scan_results[0]
    cloud_checks_by_identity = get_cloud_checks_by_identity(scan_result)

    first_check_identity = scan._checks[0].create_identity()
    assert len(cloud_checks_by_identity[first_check_identity]) == 1

    second_check_identity = scan._checks[1].create_identity()
    assert len(cloud_checks_by_identity[second_check_identity]) == 2
