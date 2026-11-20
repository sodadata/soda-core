"""Run every contract verification of a Spark DataFrame suite through execution_dataset.

Load with ``-p helpers.execution_dataset_plugin`` and ``TEST_DATASOURCE=sparkdf``. Before each
``ContractVerificationSession.execute`` the contract's table is copied into a DataFrame, registered as a temporary
view, and the table is renamed away. The verification then reads the view through ``execution_dataset``, as a
Spark job does, with Spark's own checks of the view in place. Any SQL that names the dataset's table without the
dialect's naming fails instead of reading data. Afterwards the table gets its name back.

A dataset in Soda Cloud has the type of its data source, so the mock Soda Cloud of a test reports the dataset as a
Databricks one.

A verification the plugin can't route runs as usual: no data source named like the contract's, and a contract whose
SQL names its table, which reads that table under execution_dataset too. The run fails when a test outside
``NOT_ROUTABLE`` had one, so routing can't quietly stop matching. It also fails when a routed run errors on the
renamed table, since a test that only asserts the contract fails would pass on that error.
"""

from __future__ import annotations

import os
import re

from helpers.mock_soda_cloud import MockSodaCloud
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.contracts.contract_verification import ContractVerificationSession

NAMES_ITS_TABLE = "its SQL names the table"

# Tests whose verifications can't be routed, by pytest node id under soda-tests/tests/integration, with the reason.
NOT_ROUTABLE: dict[str, str] = {
    f"soda-tests/tests/integration/{test}": reason
    for test, reason in {
        "test_failed_rows_rows_tested_query.py::test_failed_rows_percent_without_rows_tested_query_emits_error": (
            "no data source named like the contract's"
        ),
        "test_failed_keys_check.py::test_failed_keys_query_counts_failures_and_keeps_dataset_rows_tested": NAMES_ITS_TABLE,
        "test_failed_keys_check.py::test_failed_keys_query_with_mapping_is_accepted_and_counts": NAMES_ITS_TABLE,
        "test_failed_keys_check.py::test_failed_keys_query_with_rows_tested_query": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_query_with_rows_tested_query": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_query_with_rows_tested_query_percent_threshold": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_query_without_rows_tested_query_backward_compat": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_rows_tested_query_does_not_leak_to_other_checks": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_rows_tested_query_percent_does_not_leak_to_other_checks": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_rows_tested_query_returning_non_numeric_is_rejected": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_rows_tested_query_returns_null": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_rows_tested_query_returns_zero": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_rows_tested_query_sampling_not_applied": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_failed_rows_rows_tested_query_with_expression_emits_warning": NAMES_ITS_TABLE,
        "test_failed_rows_rows_tested_query.py::test_two_failed_rows_checks_resolve_to_distinct_metrics": NAMES_ITS_TABLE,
        "test_scope_activation.py::test_a_scoped_query_check_carries_no_scope_rows_tested": NAMES_ITS_TABLE,
        "test_udf_failed_rows_check.py::test_failed_rows_query": NAMES_ITS_TABLE,
        "test_udf_failed_rows_check.py::test_failed_rows_query_with_cte_produces_correct_count": NAMES_ITS_TABLE,
        "test_udf_metric_check.py::test_metric_query": NAMES_ITS_TABLE,
        "test_udf_metric_check.py::test_metric_query_returning_a_non_scalar_result_warns_and_evaluates_the_first_cell[multi_column]": NAMES_ITS_TABLE,
        "test_udf_metric_check.py::test_metric_query_returning_a_non_scalar_result_warns_and_evaluates_the_first_cell[multi_row]": NAMES_ITS_TABLE,
        "test_udf_metric_check.py::test_metric_query_returning_a_non_scalar_result_warns_and_evaluates_the_first_cell[multi_row_and_multi_column]": NAMES_ITS_TABLE,
    }.items()
}

ROUTED: list[str] = []
NOT_ROUTED: dict[str, str] = {}
# Routed tests whose run read the dataset's table, renamed away, instead of the view. A test that only asserts the
# contract fails would pass on such a run, so the plugin fails it.
READ_THE_TABLE: dict[str, str] = {}
_original_execute = ContractVerificationSession.execute.__func__


def _current_test() -> str:
    return os.environ.get("PYTEST_CURRENT_TEST", "?").rsplit(" ", 1)[0]


def _not_routed(reason: str) -> None:
    NOT_ROUTED.setdefault(_current_test(), reason)


def _routed_execute(cls, *args, **kwargs):
    text: str = kwargs["contract_yaml_sources"][0].yaml_str
    match = re.search(r"^dataset:\s*(\S+)\s*$", text, re.MULTILINE)
    # A test that passes its own execution_dataset keeps it.
    if not match or kwargs.get("execution_dataset") is not None:
        _not_routed("no dataset key, or its own execution_dataset")
        return _original_execute(cls, *args, **kwargs)
    dataset: str = match.group(1)
    identifier: DatasetIdentifier = DatasetIdentifier.parse(dataset)
    name: str = identifier.dataset_name
    data_source_impl = next(
        (d for d in kwargs.get("data_source_impls") or [] if d.name == identifier.data_source_name), None
    )
    if data_source_impl is None:
        _not_routed("no data source named like the contract's")
        return _original_execute(cls, *args, **kwargs)
    if re.search(rf"\b{re.escape(name)}\b", text[: match.start()] + text[match.end() :]):
        _not_routed(NAMES_ITS_TABLE)
        return _original_execute(cls, *args, **kwargs)
    session = data_source_impl.connection.session
    schema: str = identifier.prefixes[0]

    table: str = f"`{schema}`.`{name}`"
    away: str = f"`{schema}`.`{name}_away`"
    keyword: str = "VIEW" if session.catalog.getTable(f"{schema}.{name}").tableType == "VIEW" else "TABLE"
    view: str = f"{name}_df"
    # Checkpointed, so the view holds the rows and doesn't follow the table when it is renamed away.
    session.table(table).localCheckpoint().createTempView(view)
    soda_cloud = kwargs.get("soda_cloud_impl")
    if isinstance(soda_cloud, MockSodaCloud):
        soda_cloud.set_data_source_type(dataset, "databricks")
    session.sql(f"ALTER {keyword} {table} RENAME TO {away}")
    try:
        ROUTED.append(dataset)
        result = _original_execute(cls, *args, **{**kwargs, "execution_dataset": view})
        errors: str = result.get_errors_str()
        if "TABLE_OR_VIEW_NOT_FOUND" in errors and name in errors:
            READ_THE_TABLE.setdefault(_current_test(), errors)
        return result
    finally:
        session.sql(f"ALTER {keyword} {away} RENAME TO {table}")
        session.catalog.dropTempView(view)


def pytest_configure() -> None:
    ContractVerificationSession.execute = classmethod(_routed_execute)


def _unexpected() -> dict[str, str]:
    return {test: reason for test, reason in NOT_ROUTED.items() if test not in NOT_ROUTABLE}


def pytest_sessionfinish(session, exitstatus) -> None:
    if exitstatus == 0 and (not ROUTED or _unexpected() or READ_THE_TABLE):
        session.exitstatus = 1


def pytest_terminal_summary(terminalreporter) -> None:
    terminalreporter.write_line(
        f"execution_dataset route: routed {len(ROUTED)} verifications, {len(NOT_ROUTED)} tests not routed"
    )
    for test, reason in _unexpected().items():
        terminalreporter.write_line(
            f"execution_dataset route: FAILED, not routed and not in NOT_ROUTABLE: {test}, {reason}"
        )
    for test, errors in READ_THE_TABLE.items():
        terminalreporter.write_line(
            f"execution_dataset route: FAILED, read the table and not the view: {test}, {errors}"
        )
