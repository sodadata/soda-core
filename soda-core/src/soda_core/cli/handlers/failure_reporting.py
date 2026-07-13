"""Shared early-failure reporting for CLI flows that publish results to Soda Cloud.

When a run errors before producing results, the failure should still land on the
pre-created Cloud scan with the run's full log records — otherwise the managed
launcher only sees an exit code and marks the scan failed with a single generic
line, losing the engine's diagnostics. This module is the stable import point
for that decision + send step; external result-publishing flows (e.g. in
soda-extensions) reuse it alongside the discovery handler.
"""

from logging import LogRecord
from typing import Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.soda_cloud import SodaCloud


class ScanExecutionFailedException(Exception):
    """Raise with a user-facing message for expected/validation failures.

    The exception carries the message; nothing is logged at the raise site.
    The CLI wiring (``dependencies.run_with_failure_reporting``) is the single
    logging site: it logs this message clean (no traceback) and reports via
    ``report_scan_execution_failure``. Unexpected failures should propagate
    raw instead — the wiring logs those with the traceback."""


def report_scan_execution_failure(
    soda_cloud: Optional[SodaCloud],
    log_records: Optional[list[LogRecord]] = None,
) -> ExitCode:
    """Report a run that errored before producing results; returns the exit code.

    For a managed scan (``SODA_SCAN_ID`` set by the Runner/launcher) the
    pre-created Cloud scan is marked FAILED with the captured log records, and
    ``LOG_ERRORS`` tells the launcher the failure already reached Cloud.
    ``RESULTS_NOT_SENT_TO_CLOUD`` means nothing reached Cloud: the launcher
    treats exit codes > 3 as undelivered and marks the scan failed itself
    (generic message, no engine logs). Ad-hoc runs have no Cloud scan to
    update, so they exit ``LOG_ERRORS`` without sending anything.
    """
    scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
    if not scan_id:
        # Ad-hoc run: the errors are already on the console.
        return ExitCode.LOG_ERRORS
    if soda_cloud is None:
        soda_logger.error(
            f"{Emoticons.POLICE_CAR_LIGHT} Cannot report the failure of scan '{scan_id}' to Soda Cloud: "
            f"no usable Soda Cloud configuration."
        )
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    try:
        marked_as_failed: bool = soda_cloud.mark_scan_as_failed(scan_id=scan_id, logs=log_records)
    except Exception as exc:
        soda_logger.exception(f"Marking scan '{scan_id}' as failed on Soda Cloud raised: {exc}")
        marked_as_failed = False
    if marked_as_failed:
        soda_logger.info(f"Reported the failure of scan '{scan_id}' to Soda Cloud with the captured logs.")
        return ExitCode.LOG_ERRORS
    soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} Could not mark scan '{scan_id}' as failed on Soda Cloud.")
    return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
