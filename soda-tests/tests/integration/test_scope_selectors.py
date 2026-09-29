"""The ``scope`` check filter on a real data source.

``scope=<key>`` selects the checks of a declared scope, ``scope=base`` the checks without a scope, several values OR
together and ``scope!=<key>`` excludes. Core never activates a declared scope, so a selected scoped check still goes
up as EXCLUDED; the selection itself is read from ``CheckImpl.selected``. A key that no collection in the session
declares fails the run with exit code 3 before any query against the dataset.

Every test here drops the soda-scopes extension, so the file pins core alone even where soda-scopes is installed.
"""

from __future__ import annotations

from typing import Optional

import pytest
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from helpers.scope_test_kinds import SCOPE_UNSUPPORTED_KIND
from helpers.scopes_extension_removal import without_scopes_extension  # noqa: F401
from helpers.test_functions import dedent_and_strip
from helpers.test_table import TestTable, TestTableSpecification
from soda_core.check_collections.base import CheckCollectionImpl
from soda_core.check_collections.session import execute_check_collections
from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.contract import interpret_contract_verification_result
from soda_core.cli.handlers.scan import run_scan
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import check_outcome_to_soda_cloud
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import CheckOutcome, ContractVerificationSession
from soda_core.contracts.impl.check_selector import CheckSelector
from soda_core.contracts.impl.contract_verification_impl import CheckCollectionImplExtension, ContractImpl

pytestmark = pytest.mark.usefixtures("without_scopes_extension")

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("scope_selectors")
    .column_integer("id")
    .column_varchar("region")
    .rows(rows=[(1, "eu"), (2, "eu"), (3, "us")])
    .build()
)

# Two declared scopes and base checks. Column checks come first, as in the YAML.
CONTRACT_YAML: str = """
    scopes:
      eu:
        name: EU
        filter: region = 'eu'
      us:
        name: US
        filter: region = 'us'
    columns:
      - name: id
        checks:
          - missing:
          - missing:
              scope: eu
      - name: region
    checks:
      - row_count:
      - row_count:
          scope: eu
      - row_count:
          scope: us
"""
CHECKS: list[tuple[str, str]] = [
    ("missing", "base"),
    ("missing", "eu"),
    ("row_count", "base"),
    ("row_count", "eu"),
    ("row_count", "us"),
]

# A kind without scope support that declares no 'eu', standing in for a data standard. Its 'apac' is never a known key.
UNSUPPORTED_YAML: str = f"""
    kind: {SCOPE_UNSUPPORTED_KIND}
    scopes:
      apac:
        name: APAC
    columns:
      - name: id
        checks:
          - missing:
      - name: region
    checks:
      - row_count:
      - row_count:
          scope: eu
"""
UNSUPPORTED_CHECKS: list[tuple[str, str]] = [("missing", "base"), ("row_count", "base"), ("row_count", "eu")]

_RECORDER_EXTENSION: str = "scope_selectors_recorder"


@pytest.fixture
def built_collections() -> list[CheckCollectionImpl]:
    """Every collection the test builds, of any kind, recorded by a global extension removed at teardown."""
    built: list[CheckCollectionImpl] = []

    class _Recorder(CheckCollectionImplExtension):
        def __init__(self, contract_impl: CheckCollectionImpl):
            built.append(contract_impl)

    CheckCollectionImpl.register_extension(_RECORDER_EXTENSION, _Recorder)
    try:
        yield built
    finally:
        CheckCollectionImpl.impl_extensions.pop(_RECORDER_EXTENSION, None)


def _checks(collection: CheckCollectionImpl, selected_only: bool = False) -> list[tuple[str, str]]:
    return [
        (check_impl.type, check_impl.scope.key)
        for check_impl in collection.all_check_impls
        if check_impl.selected or not selected_only
    ]


def _yaml_source(
    data_source_test_helper: DataSourceTestHelper, test_table: TestTable, yaml_str: str, file_path: str
) -> ContractYamlSource:
    yaml_str = f"dataset: {data_source_test_helper.build_dqn(test_table)}\n{dedent_and_strip(yaml_str)}"
    return ContractYamlSource.from_str(yaml_str=yaml_str, file_path=file_path)


def _capture_sql(data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The SQL of every statement the data source connection runs from here on, until the test ends."""
    captured: list[str] = []
    connection = data_source_test_helper.data_source_impl.data_source_connection
    for method_name in (
        "execute_query",
        "execute_query_one_by_one",
        "execute_query_one_by_one_prefer_streaming",
        "execute_query_iterate",
        "execute_update",
    ):
        original = getattr(connection, method_name)

        def _recording(*args, _original=original, **kwargs):
            captured.append(kwargs["sql"] if "sql" in kwargs else args[0])
            return _original(*args, **kwargs)

        monkeypatch.setattr(connection, method_name, _recording)
    return captured


@pytest.mark.parametrize(
    "check_filters, selected",
    [
        pytest.param([], CHECKS, id="bare"),
        pytest.param(["scope=eu"], [("missing", "eu"), ("row_count", "eu")], id="eu"),
        pytest.param(
            ["scope=eu", "scope=us"], [("missing", "eu"), ("row_count", "eu"), ("row_count", "us")], id="eu-us"
        ),
        pytest.param(["scope=base"], [("missing", "base"), ("row_count", "base")], id="base"),
        pytest.param(["scope!=eu"], [("missing", "base"), ("row_count", "base"), ("row_count", "us")], id="not-eu"),
        pytest.param(["scope=eu", "type=missing"], [("missing", "eu")], id="eu-missing"),
    ],
)
def test_scope_selector_matrix(
    data_source_test_helper: DataSourceTestHelper,
    built_collections: list[CheckCollectionImpl],
    check_filters: list[str],
    selected: list[tuple[str, str]],
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)

    session_result = data_source_test_helper.verify_contract(
        contract_yaml_str=CONTRACT_YAML,
        test_table=test_table,
        check_selectors=CheckSelector.parse_all(check_filters),
    )

    assert not session_result.has_errors
    [contract_impl] = built_collections
    assert _checks(contract_impl) == CHECKS
    assert _checks(contract_impl, selected_only=True) == selected

    # Only the selected base checks run. Every check is in the result.
    [result] = session_result.contract_verification_results
    expected_outcomes = [
        CheckOutcome.PASSED if check in selected and check[1] == "base" else CheckOutcome.EXCLUDED for check in CHECKS
    ]
    assert [(check_result.check.type, check_result.check.scope or "base") for check_result in result.check_results] == (
        CHECKS
    )
    assert [check_result.outcome for check_result in result.check_results] == expected_outcomes
    assert result.number_of_checks_excluded == expected_outcomes.count(CheckOutcome.EXCLUDED)


@pytest.mark.parametrize("check_filter", ["scope=apac", "scope!=apac"])
def test_an_unknown_scope_key_exits_3_before_any_query(
    data_source_test_helper: DataSourceTestHelper, monkeypatch: pytest.MonkeyPatch, check_filter: str
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    executed_sql: list[str] = _capture_sql(data_source_test_helper, monkeypatch)
    run_logs: list[Logs] = []

    def verify(check_filters: list[str]):
        def command(logs: Logs) -> ExitCode:
            run_logs.append(logs)
            session_result = ContractVerificationSession.execute(
                contract_yaml_sources=[
                    _yaml_source(data_source_test_helper, test_table, CONTRACT_YAML, "scope_selectors.yml")
                ],
                data_source_impls=[data_source_test_helper.data_source_impl],
                check_selectors=CheckSelector.parse_all(check_filters),
                logs=logs,
            )
            return interpret_contract_verification_result(session_result)

        return run_scan(soda_cloud=None, command=command)

    def queries_on_the_dataset() -> list[str]:
        table_name: str = test_table.unique_name.lower()
        return [sql for sql in executed_sql if table_name in sql.lower() or "_soda_filtered_" in sql.lower()]

    # A known key queries the dataset, so the capture sees those queries.
    assert verify(["scope=eu"]) == ExitCode.OK
    assert queries_on_the_dataset()
    executed_sql.clear()

    assert verify([check_filter]) == ExitCode.LOG_ERRORS
    assert "'apac'" in run_logs[-1].get_errors_str()
    assert queries_on_the_dataset() == []


class _FileIdSodaCloud(MockSodaCloud):
    """Answers every file upload with a file id, so each file of a session uploads its results."""

    def _http_handle(self, method, url, headers, json, data):
        response = super()._http_handle(method=method, url=url, headers=headers, json=json, data=data)
        if isinstance(json, dict) and json.get("type") == "sodaCoreUploadContractFile":
            return MockResponse(status_code=200, json_object={"fileId": "scope-selectors-file-id"})
        return response


def _execute_mixed_session(
    data_source_test_helper: DataSourceTestHelper, soda_cloud: Optional[MockSodaCloud], check_filters: list[str]
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    data_source_impl = data_source_test_helper.data_source_impl
    return execute_check_collections(
        yaml_sources=[
            _yaml_source(data_source_test_helper, test_table, CONTRACT_YAML, "contract.yml"),
            _yaml_source(data_source_test_helper, test_table, UNSUPPORTED_YAML, "unsupported.yml"),
        ],
        data_source_impl=None,
        soda_cloud_impl=soda_cloud,
        publish_results=soda_cloud is not None,
        all_data_source_impls={data_source_impl.name: data_source_impl},
        primary_data_source_impl=data_source_impl,
        check_selectors=CheckSelector.parse_all(check_filters),
        default_impl_class=ContractImpl,
    )


@pytest.mark.parametrize(
    "check_filters, contract_outcomes, unsupported_outcomes",
    [
        pytest.param(
            [],
            ["pass", "excluded", "pass", "excluded", "excluded"],
            ["pass", "pass", "excluded"],
            id="bare",
        ),
        pytest.param(["scope=eu"], ["excluded"] * 5, ["excluded"] * 3, id="eu"),
    ],
)
def test_mixed_session_uploads_every_check(
    data_source_test_helper: DataSourceTestHelper,
    built_collections: list[CheckCollectionImpl],
    monkeypatch: pytest.MonkeyPatch,
    check_filters: list[str],
    contract_outcomes: list[str],
    unsupported_outcomes: list[str],
):
    monkeypatch.delenv("SODA_SCAN_ID", raising=False)
    soda_cloud = _FileIdSodaCloud()

    session_result = _execute_mixed_session(data_source_test_helper, soda_cloud, check_filters)

    assert not session_result.has_errors
    contract_impl, unsupported_impl = built_collections
    assert _checks(contract_impl) == CHECKS
    assert _checks(unsupported_impl) == UNSUPPORTED_CHECKS
    if check_filters:
        # The placeholder scope of the kind without support answers with its own key.
        assert _checks(contract_impl, selected_only=True) == [("missing", "eu"), ("row_count", "eu")]
        assert _checks(unsupported_impl, selected_only=True) == [("row_count", "eu")]

    uploads = [
        request.json
        for request in soda_cloud.requests
        if isinstance(request.json, dict) and request.json.get("type") == "sodaCoreInsertScanResults"
    ]
    assert len(uploads) == 2
    for result, upload, outcomes in zip(
        session_result.results, uploads, [contract_outcomes, unsupported_outcomes], strict=True
    ):
        assert [check_outcome_to_soda_cloud(check_result.outcome) for check_result in result.check_results] == outcomes
        assert [(check["identities"]["vc1"], check["outcome"]) for check in upload["checks"]] == [
            (check_result.check.identity, outcome) for check_result, outcome in zip(result.check_results, outcomes)
        ]


def test_mixed_session_never_knows_a_key_of_a_kind_without_scope_support(
    data_source_test_helper: DataSourceTestHelper, built_collections: list[CheckCollectionImpl]
):
    with pytest.raises(InvalidArgumentException, match="'apac'"):
        _execute_mixed_session(data_source_test_helper, None, ["scope=apac"])

    assert len(built_collections) == 2
