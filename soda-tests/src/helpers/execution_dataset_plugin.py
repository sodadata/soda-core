"""Run every contract verification of a Spark DataFrame suite through execution_dataset.

Load with ``-p helpers.execution_dataset_plugin`` and ``TEST_DATASOURCE=sparkdf``. Before each
``ContractVerificationSession.execute`` the contract's table is copied into a DataFrame, registered as a temporary
view, and the table is renamed away. The verification then reads the view through ``execution_dataset``, as a
Spark job does, with Spark's own checks of the view in place. Any SQL that names the dataset's table without the
dialect's naming fails instead of reading data. Afterwards the table gets its name back. The view stays until the
test ends, so a query the run stored, such as a failed rows query, reads the view when the test runs it again.

A dataset in Soda Cloud has the type of its data source, so the mock Soda Cloud of a test reports the dataset as a
Databricks one.

A check's own SQL reads the tables it names, so a job passes the view's name to it in a contract variable. The
plugin does the same by writing the view's name over the table's in the contract before the run. A verification
of a contract read from a file, with no Spark data source named like the contract's, or whose SQL names the table
in another form, runs as usual, and the summary lists it. So does every verification of a test marked
``@pytest.mark.execution_dataset_not_routable(reason)``, for a test that can route but checks something a routed
run changes. The run fails when nothing was routed, and when a routed run errors on the renamed table, since a test
that only asserts the contract fails would pass on that error.

A verification that passes its own execution_dataset runs as usual. soda-extensions stores failed rows under
execution_dataset only in the data source that reads the dataset, so in a routed run the mock Soda Cloud's
diagnostics warehouse configuration reuses the data source. On Spark a separate warehouse is the same session anyway.
"""

from __future__ import annotations

import re
from typing import Optional

import pytest
from helpers.mock_soda_cloud import MockSodaCloud
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.contracts.contract_verification import ContractVerificationSession

NOT_ROUTABLE = "execution_dataset_not_routable"
WAREHOUSE_CONFIGURATION = "sodaCoreDatasetDiagnosticsWarehouseConfiguration"

ROUTED: list[str] = []
# Tests with a verification the plugin didn't route, with the reason.
NOT_ROUTED: dict[str, str] = {}
# Routed tests whose run read the dataset's table, renamed away, instead of the view. A test that only asserts the
# contract fails would pass on such a run, so the plugin fails it.
READ_THE_TABLE: dict[str, str] = {}
_views: list[tuple] = []
_item: Optional[pytest.Item] = None
_original_execute = ContractVerificationSession.execute.__func__


def _current_test() -> str:
    return _item.nodeid if _item else "?"


def _not_routed(reason: str) -> None:
    NOT_ROUTED.setdefault(_current_test(), reason)


def _routed_execute(cls, *args, **kwargs):
    marker = _item.get_closest_marker(NOT_ROUTABLE) if _item else None
    if marker:
        _not_routed(marker.args[0] if marker.args else NOT_ROUTABLE)
        return _original_execute(cls, *args, **kwargs)
    if kwargs.get("execution_dataset") is not None:
        return _original_execute(cls, *args, **kwargs)
    sources: list = kwargs.get("contract_yaml_sources") or []
    # A contract read from a file has no text until core parses it.
    text: Optional[str] = sources[0].yaml_str if sources else None
    if not isinstance(text, str):
        _not_routed("its contract has no YAML text, as one read from a file")
        return _original_execute(cls, *args, **kwargs)
    source = sources[0]
    match = re.search(r"^dataset:\s*(\S+)\s*$", text, re.MULTILINE)
    if not match:
        _not_routed("no dataset key")
        return _original_execute(cls, *args, **kwargs)
    dataset: str = match.group(1)
    identifier: DatasetIdentifier = DatasetIdentifier.parse(dataset)
    name: str = identifier.dataset_name
    data_source_impl = next(
        (d for d in kwargs.get("data_source_impls") or [] if d.name == identifier.data_source_name), None
    )
    session = getattr(getattr(data_source_impl, "connection", None), "session", None)
    if session is None:
        _not_routed("no Spark data source named like the contract's")
        return _original_execute(cls, *args, **kwargs)
    schema: str = identifier.prefixes[0]

    table: str = f"`{schema}`.`{name}`"
    away: str = f"`{schema}`.`{name}_away`"
    view: str = f"{name}_df"
    before, after = (
        part.replace(table, view).replace(f"{schema}.{name}", view)
        for part in (text[: match.start()], text[match.end() :])
    )
    if re.search(rf"\b{re.escape(name)}\b", before + after):
        _not_routed("its SQL names the table in another form")
        return _original_execute(cls, *args, **kwargs)
    if before + after != text[: match.start()] + text[match.end() :]:
        routed_source = type(source).from_str(before + match.group(0) + after, file_path=source.file_path)
        kwargs = {**kwargs, "contract_yaml_sources": [routed_source, *sources[1:]]}
    keyword: str = "VIEW" if session.catalog.getTable(f"{schema}.{name}").tableType == "VIEW" else "TABLE"
    # Checkpointed, so the view holds the rows and doesn't follow the table when it is renamed away.
    session.table(table).localCheckpoint().createOrReplaceTempView(view)
    _views.append((session, view))
    soda_cloud = kwargs.get("soda_cloud_impl")
    # A handler the test set on the instance comes back after the run.
    own_http_handle = None
    if isinstance(soda_cloud, MockSodaCloud):
        own_http_handle = soda_cloud.__dict__.get("_http_handle")
        soda_cloud.set_data_source_type(dataset, "databricks")
        soda_cloud._http_handle = _reusing_the_data_source(soda_cloud._http_handle)
    session.sql(f"ALTER {keyword} {table} RENAME TO {away}")
    try:
        ROUTED.append(dataset)
        # A publishing override must decide on the columns. The view has the table's, so they go up as on a plain run.
        publish_schema_metadata: bool = kwargs.get("publish_schema_metadata") is not False
        result = _original_execute(
            cls, *args, **{**kwargs, "execution_dataset": view, "publish_schema_metadata": publish_schema_metadata}
        )
        errors: str = result.get_errors_str()
        if "TABLE_OR_VIEW_NOT_FOUND" in errors and name in errors:
            READ_THE_TABLE.setdefault(_current_test(), errors)
        return result
    finally:
        session.sql(f"ALTER {keyword} {away} RENAME TO {table}")
        if isinstance(soda_cloud, MockSodaCloud):
            if own_http_handle is None:
                del soda_cloud._http_handle
            else:
                soda_cloud._http_handle = own_http_handle


def _reusing_the_data_source(handle):
    def reusing(method, url, headers, json, data):
        response = handle(method=method, url=url, headers=headers, json=json, data=data)
        body: Optional[dict] = getattr(response, "json_object", None)
        failed_rows = (body or {}).get("failedRowsConfiguration")
        if (json or {}).get("type") == WAREHOUSE_CONFIGURATION and isinstance(failed_rows, dict):
            response.set_json_object({**body, "failedRowsConfiguration": {**failed_rows, "reuseDataSource": True}})
        return response

    return reusing


def pytest_configure(config) -> None:
    config.addinivalue_line("markers", f"{NOT_ROUTABLE}(reason): the plugin leaves this test's verifications alone.")
    ContractVerificationSession.execute = classmethod(_routed_execute)


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item) -> None:
    global _item
    _item = item


def pytest_runtest_teardown(item, nextitem) -> None:
    while _views:
        session, view = _views.pop()
        session.catalog.dropTempView(view)


def pytest_sessionfinish(session, exitstatus) -> None:
    if exitstatus == 0 and (not ROUTED or READ_THE_TABLE):
        session.exitstatus = 1


def pytest_terminal_summary(terminalreporter) -> None:
    terminalreporter.write_line(
        f"execution_dataset route: routed {len(ROUTED)} verifications, {len(NOT_ROUTED)} tests not routed"
    )
    for test, reason in NOT_ROUTED.items():
        terminalreporter.write_line(f"execution_dataset route: not routed: {test}, {reason}")
    for test, errors in READ_THE_TABLE.items():
        terminalreporter.write_line(
            f"execution_dataset route: FAILED, read the table and not the view: {test}, {errors}"
        )
