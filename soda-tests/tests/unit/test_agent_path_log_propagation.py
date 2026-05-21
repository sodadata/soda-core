"""N3 regression: errors logged during ``ContractImpl.__init__`` in the
agent path must surface on ``result.log_records``.

Background
----------
``_execute_on_agent`` now constructs a ``ContractImpl`` purely to dispatch
``verify_on_agent``. That construction runs ``_parse_columns`` and
``_parse_checks`` in the engine — work that did not happen client-side in
the agent path before this PR. Errors emitted during those parses flow
into the impl's ``self.logs``, but ``soda_cloud.verify_contract_on_agent``
builds the returned result's ``log_records`` from its own late-bound
``Logs()`` started AFTER the impl was constructed, so the init-time
records get silently dropped.

The fix in ``_execute_on_agent`` pops the impl's init-time records and
prepends them to ``result.log_records`` after ``verify_on_agent`` returns.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from soda_core.common.logging_constants import soda_logger
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    Contract,
    ContractVerificationResult,
    ContractVerificationSession,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.contract_verification_impl import ContractImpl


def test_execute_on_agent_propagates_impl_init_logs_to_result(monkeypatch):
    init_error_msg = "synthetic parse error during impl init"

    original_init = ContractImpl.__init__

    def init_with_error(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        # Emit AFTER super().__init__() has set up self.logs, so the record
        # lands in the impl's capturer the same way real parse errors would.
        soda_logger.error(init_error_msg)

    monkeypatch.setattr(ContractImpl, "__init__", init_with_error)

    now = datetime.now(tz=timezone.utc)

    def fake_verify_on_agent(self, **kwargs):
        return ContractVerificationResult(
            check_collection=Contract(
                data_source_name="ds",
                dataset_prefix=["db", "schema"],
                dataset_name="orders",
                soda_qualified_dataset_name="ds/db/schema/orders",
                source=YamlFileContentInfo(source_content_str=None, local_file_path=None),
            ),
            data_source=None,
            data_timestamp=None,
            started_timestamp=now,
            ended_timestamp=now,
            status=CheckCollectionStatus.UNKNOWN,
            measurements=[],
            check_results=[],
            sending_results_to_soda_cloud_failed=False,
            log_records=[],
        )

    monkeypatch.setattr(ContractImpl, "verify_on_agent", fake_verify_on_agent)

    yaml_str = "dataset: ds/db/schema/orders\ncolumns:\n  - name: id\n"
    session_result = ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(yaml_str)],
        soda_cloud_use_agent=True,
    )

    assert len(session_result.contract_verification_results) == 1
    result = session_result.contract_verification_results[0]
    log_msgs = [r.getMessage() for r in (result.log_records or [])]
    assert init_error_msg in log_msgs, (
        f"Expected init-time error to be propagated onto result.log_records, " f"but got: {log_msgs}"
    )


def test_execute_on_agent_does_not_leak_impl_logs_to_root_logger_across_items(monkeypatch):
    """The impl's ``self.logs`` is created during ``__init__`` and attaches a
    handler to the root logger. Without an explicit detach the handler stays
    attached across loop iterations, leaking into subsequent items' captures.
    The fix detaches each impl's logs after verify_on_agent returns.
    """
    handler_count_before = len(logging.root.handlers)

    now = datetime.now(tz=timezone.utc)

    def fake_verify_on_agent(self, **kwargs):
        return ContractVerificationResult(
            check_collection=Contract(
                data_source_name="ds",
                dataset_prefix=["db", "schema"],
                dataset_name="orders",
                soda_qualified_dataset_name="ds/db/schema/orders",
                source=YamlFileContentInfo(source_content_str=None, local_file_path=None),
            ),
            data_source=None,
            data_timestamp=None,
            started_timestamp=now,
            ended_timestamp=now,
            status=CheckCollectionStatus.UNKNOWN,
            measurements=[],
            check_results=[],
            sending_results_to_soda_cloud_failed=False,
            log_records=[],
        )

    monkeypatch.setattr(ContractImpl, "verify_on_agent", fake_verify_on_agent)

    yaml_str = "dataset: ds/db/schema/orders\ncolumns:\n  - name: id\n"
    ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(yaml_str)],
        soda_cloud_use_agent=True,
    )

    # After the session returns, the only root-logger handler delta should
    # be the session-level Logs() (which is itself cleaned up by the public
    # entry's lifecycle). The impl's own LogCapturer must have been removed.
    # Allow at most one residual (the session-level capturer) above baseline.
    assert len(logging.root.handlers) <= handler_count_before + 1, (
        f"Impl LogCapturer leaked to root logger. "
        f"Before: {handler_count_before}, after: {len(logging.root.handlers)}"
    )
