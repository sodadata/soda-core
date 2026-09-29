from typing import Optional

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.test_functions import dedent_and_strip
from helpers.test_table import TestTable, TestTableSpecification
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    CheckOutcome,
    ContractVerificationResult,
    ContractVerificationSession,
    ContractVerificationSessionResult,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("logs_formatting")
    .column_varchar("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)


def test_split_log_lines(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=dedent_and_strip(
            """
    checks:
      - row_count:
          threshold:
            must_be: 3
    """
        ),
    )
    log_lines = contract_verification_result.get_logs()

    expected = [
        # A string-sourced contract (no local file path, no collection id) falls
        # back to the yaml source's description instead of printing 'None'.
        "Verifying contract 📜 Contract YAML string 🤞",
        # some (many) non-deterministic log lines are skipped, we just need to make sure table is correctly split over multiple lines
        "+-----------------+------------------------------------+-------------+-----------+--------------+------------+------------------------+",
        "| Column          | Check                              | Threshold   | Outcome   | Check Type   | Identity   | Diagnostics            |",
        "+=================+====================================+=============+===========+==============+============+========================+",
        # "| [dataset-level] | Row count meets expected threshold | level: fail | ✅ PASSED | row_count    | 2ccd8a76   | check_rows_tested: 3   |",
        "|                 |                                    | must be: 3  |           |              |            | dataset_rows_tested: 3 |",
        "+-----------------+------------------------------------+-------------+-----------+--------------+------------+------------------------+",
        "# Summary:",
        "|----------------|---|----|",
        "| Checks         | 1 |    |",
        "| Passed         | 1 | ✅ |",
        "| Failed         | 0 | ✅ |",
        "| Warned         | 0 | ✅ |",
        "| Not Evaluated  | 0 | ✅ |",
        "| Excluded       | 0 | ✅ |",
        "| Runtime Errors | 0 | ✅ |",
    ]
    for line in expected:
        assert line in log_lines

    # The log_table_extra_columns seam is opt-in: contract verification never
    # supplies extra columns, so no "Window" (or other caller-supplied)
    # column may ever appear in its table.
    assert not any("Window" in line for line in log_lines)
    # The log_table_header_overrides seam is opt-in too: contract verification
    # keeps the "Check" header (pinned verbatim in `expected` above) and must
    # never render the metric-monitoring "Monitor" header.
    assert not any("Monitor" in line for line in log_lines)


_PASSING_CONTRACT_CHECKS = """
    columns:
      - name: id
    checks:
      - row_count:
"""

# The first contract of a two-contract session errors while it is parsed or while its queries run.
_ERRORING_CONTRACT_CHECKS = {
    "parse": """
        columns:
          - name: id
        checks:
          - not_a_check_type:
    """,
    "query": """
        columns:
          - name: not_a_column
            checks:
              - missing:
    """,
}


def _contract_yaml_source(
    data_source_test_helper: DataSourceTestHelper, test_table: TestTable, checks_yaml: str
) -> ContractYamlSource:
    return ContractYamlSource.from_str(
        f"dataset: {data_source_test_helper.build_dqn(test_table)}\n" + dedent_and_strip(checks_yaml)
    )


def _verify_contracts(
    data_source_test_helper: DataSourceTestHelper,
    checks_yamls: list[str],
    logs: Optional[Logs] = None,
) -> tuple[ContractVerificationSessionResult, list[dict]]:
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    soda_cloud = MockSodaCloud(
        [MockResponse(status_code=200, json_object={"scanId": f"scan-{index}"}) for index in range(len(checks_yamls))]
    )
    soda_cloud._upload_contract_yaml_file = lambda *args, **kwargs: "contract-file-id"
    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[
            _contract_yaml_source(data_source_test_helper, test_table, checks_yaml) for checks_yaml in checks_yamls
        ],
        data_source_impls=[data_source_test_helper.data_source_impl],
        soda_cloud_impl=soda_cloud,
        soda_cloud_publish_results=True,
        logs=logs,
    )
    payloads: list[dict] = [
        request.json
        for request in soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]
    return session_result, payloads


def _payload_messages(payload: dict, level: Optional[str] = None) -> list[str]:
    return [log["message"] for log in payload.get("logs") or [] if level is None or log["level"] == level]


@pytest.mark.parametrize("erroring_phase", _ERRORING_CONTRACT_CHECKS.keys())
def test_first_contract_error_leaves_the_second_contract_its_own_status(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, erroring_phase: str
):
    """Without a caller Logs, ``ContractVerificationSession.execute`` builds one and
    hands it to the session, so this is what every two-contract API call does."""
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    session_result, _ = _verify_contracts(
        data_source_test_helper, [_ERRORING_CONTRACT_CHECKS[erroring_phase], _PASSING_CONTRACT_CHECKS]
    )

    first, second = session_result.contract_verification_results
    assert first.status is CheckCollectionStatus.ERROR
    assert second.status is CheckCollectionStatus.PASSED
    assert [check_result.outcome for check_result in second.check_results] == [CheckOutcome.PASSED]
    assert second.get_errors() == []


@pytest.mark.parametrize("erroring_phase", _ERRORING_CONTRACT_CHECKS.keys())
def test_each_contract_of_a_session_uploads_only_its_own_records_and_the_caller_sees_each_once(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, erroring_phase: str
):
    """Each upload carries only its own contract's records, and the caller's Logs,
    the one its failure report reads, gets every record exactly once."""
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    caller_logs = Logs()
    session_result, payloads = _verify_contracts(
        data_source_test_helper,
        [_ERRORING_CONTRACT_CHECKS[erroring_phase], _PASSING_CONTRACT_CHECKS],
        logs=caller_logs,
    )

    first, second = session_result.contract_verification_results
    assert first.status is CheckCollectionStatus.ERROR
    assert second.status is CheckCollectionStatus.PASSED

    first_payload, second_payload = payloads
    for payload in payloads:
        assert len([m for m in _payload_messages(payload) if m.startswith("Verifying contract")]) == 1
    assert _payload_messages(first_payload, level="error")
    assert _payload_messages(second_payload, level="error") == []

    first_record_ids = {id(record) for record in first.log_records}
    second_record_ids = {id(record) for record in second.log_records}
    assert first_record_ids.isdisjoint(second_record_ids)
    caller_record_ids = [id(record) for record in caller_logs.get_log_records()]
    assert len(caller_record_ids) == len(set(caller_record_ids))
    assert first_record_ids | second_record_ids <= set(caller_record_ids)


def test_single_contract_session_keeps_the_callers_logs(data_source_test_helper: DataSourceTestHelper, monkeypatch):
    """A lone contract has no sibling to keep apart from, so it keeps using the
    caller's Logs: its upload still carries what the caller logged before the session."""
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    caller_logs = Logs()
    soda_logger.warning("Logged by the caller before the session")
    session_result, (payload,) = _verify_contracts(
        data_source_test_helper, [_PASSING_CONTRACT_CHECKS], logs=caller_logs
    )

    (result,) = session_result.contract_verification_results
    assert result.status is CheckCollectionStatus.PASSED
    assert result.log_records is caller_logs.get_log_records()
    assert "Logged by the caller before the session" in _payload_messages(payload, level="warning")
