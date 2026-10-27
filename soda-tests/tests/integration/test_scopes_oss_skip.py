"""Without an extension that runs scopes, core runs the checks without a scope and does not evaluate the others.

Each declared scope stays inactive. Its checks build no metrics and send no queries, and go up to Soda Cloud as not
evaluated next to the checks that ran. One error names their scopes, so the contract ends ERROR with exit code 3
instead of looking like a run that passed.

Every test here drops the soda-scopes extension, so the file pins core alone even where soda-scopes is installed.
"""

from __future__ import annotations

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.test_table import TestTableSpecification
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import interpret_contract_verification_result
from soda_core.contracts.contract_verification import CheckCollectionStatus, CheckOutcome

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("scopes_oss_skip")
    .column_integer("id")
    .column_integer("amount")
    .rows(rows=[(1, 10), (2, None), (3, 30)])
    .build()
)

# Only the scoped checks read 'amount', so no query may name it.
SCOPED_CONTRACT: str = """
    scopes:
      high:
        name: High ids
        filter: id > 1
      release-gate:
        name: Release gate
        schedule:
          cron: "0 6 * * *"
    columns:
      - name: id
        checks:
          - missing:
      - name: amount
        checks:
          - missing:
              scope: high
          - invalid:
              scope: release-gate
              valid_min: 0
    checks:
      - row_count:
      - failed_rows:
          scope: high
          expression: amount > 20
      - metric:
          scope: release-gate
          expression: avg(amount)
          threshold:
            must_be_greater_than: 0
"""
QUERY_METHODS: tuple[str, ...] = (
    "execute_query",
    "execute_query_one_by_one",
    "execute_query_one_by_one_prefer_streaming",
    "execute_query_iterate",
)


def _record_queries(monkeypatch, data_source_impl) -> list[str]:
    executed_sql: list[str] = []
    for method_name in QUERY_METHODS:
        original = getattr(data_source_impl, method_name)

        def recording(sql: str, *args, _original=original, **kwargs):
            executed_sql.append(sql)
            return _original(sql, *args, **kwargs)

        monkeypatch.setattr(data_source_impl, method_name, recording)
    return executed_sql


def test_scoped_checks_are_not_evaluated_without_queries(monkeypatch, data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
            MockResponse(status_code=200, json_object={"scanId": "scopes-oss-skip-scan"}),
        ]
    )
    executed_sql: list[str] = _record_queries(monkeypatch, data_source_test_helper.data_source_impl)

    session_result = data_source_test_helper.verify_contract(test_table=test_table, contract_yaml_str=SCOPED_CONTRACT)

    [result] = session_result.contract_verification_results
    assert [(check_result.check.scope, check_result.outcome) for check_result in result.check_results] == [
        (None, CheckOutcome.PASSED),
        ("high", CheckOutcome.NOT_EVALUATED),
        ("release-gate", CheckOutcome.NOT_EVALUATED),
        (None, CheckOutcome.PASSED),
        ("high", CheckOutcome.NOT_EVALUATED),
        ("release-gate", CheckOutcome.NOT_EVALUATED),
    ]
    assert result.number_of_checks_excluded == 0
    assert result.get_errors() == [
        "Not evaluating 4 checks in scope 'high', 'release-gate': running checks in a scope needs a Soda extension "
        "that runs scopes."
    ]
    assert result.status == CheckCollectionStatus.ERROR
    assert interpret_contract_verification_result(session_result) == ExitCode.LOG_ERRORS

    # The unscoped checks ran, and nothing queried a scoped check.
    assert executed_sql
    assert [sql for sql in executed_sql if "amount" in sql.lower()] == []

    [upload] = [
        request.json
        for request in data_source_test_helper.soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]
    uploaded_outcomes = {check["identities"]["vc1"]: check["outcome"] for check in upload["checks"]}
    assert uploaded_outcomes == {
        check_result.check.identity: ("unevaluated" if check_result.check.scope else "pass")
        for check_result in result.check_results
    }
    assert [log["message"] for log in upload["logs"] if log["level"] == "error"] == result.get_errors()


def test_a_check_scope_from_a_variable_fails_without_queries(
    monkeypatch, data_source_test_helper: DataSourceTestHelper
):
    # A scope key is fixed. Read as null, the scope would put the check in the base scope and run it over the whole
    # dataset.
    monkeypatch.delenv("SODA_TEST_SCOPE_KEY", raising=False)
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_test_helper.enable_soda_cloud_mock(
        [
            MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"}),
            MockResponse(status_code=200, json_object={"scanId": "scopes-oss-skip-scan"}),
        ]
    )
    executed_sql: list[str] = _record_queries(monkeypatch, data_source_test_helper.data_source_impl)

    session_result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            scopes:
              high:
                name: High ids
            checks:
              - row_count:
              - failed_rows:
                  scope: ${env.SODA_TEST_SCOPE_KEY}
                  expression: amount > 20
        """,
    )

    [result] = session_result.contract_verification_results
    assert result.get_errors() == [
        "Check 'scope' cannot use a variable, but was '${env.SODA_TEST_SCOPE_KEY}'. Name a declared scope key"
    ]
    assert result.status == CheckCollectionStatus.ERROR
    assert interpret_contract_verification_result(session_result) == ExitCode.LOG_ERRORS
    assert [sql for sql in executed_sql if "amount" in sql.lower()] == []
