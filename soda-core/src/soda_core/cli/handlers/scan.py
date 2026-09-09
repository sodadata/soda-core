"""The bracket every CLI results-publishing command runs under.

``run_scan`` installs the ``ScanContext`` variant, owns the ``Logs`` lifecycle, maps every escaped
failure to a failure report and exit code, and closes the scan's ingestion. Flows read the installed
context with ``get_scan_context()``.

The scan start happens inside the wrapped command, not at bracket time: ``sodaCoreScanStart`` needs
the scan-definition name, data source name and data timestamp, which a flow only knows once its
dependencies resolve. The flow calls ``get_scan_context().start_scan(...)`` before its engine work,
so that phase streams its logs.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.failure_reporting import ScanExecutionFailedException, report_scan_execution_failure
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs
from soda_core.common.scan_context import AtomicScanContext, BatchedScanContext, ScanContext, using_scan_context

if TYPE_CHECKING:
    from soda_core.common.soda_cloud import SodaCloud


def run_scan(
    soda_cloud: Optional[SodaCloud],
    command: Callable[[Logs], ExitCode],
    batched: bool = False,
) -> ExitCode:
    """Run a results-publishing CLI command and return its exit code.

    Owns everything around the command: installs the ScanContext (readable inside via
    ``get_scan_context()``), creates and closes the run's ``Logs``, reports an escaped
    failure to Soda Cloud exactly once, and ends the scan's ingestion after a clean run.

    Returns the command's own exit code on a clean run; ``LOG_ERRORS`` when a failure was
    reported to Soda Cloud; ``RESULTS_NOT_SENT_TO_CLOUD`` when it could not be — the
    launcher then marks the scan failed itself.

    @param soda_cloud: The failure-reporting channel; None when the command can run
        without Cloud.
    @param command: The flow to run; receives the run's ``Logs``.
    @param batched: Whether this flow participates in batched ingestion. Only takes effect
        on a managed run (SODA_SCAN_ID set). Flows that ingest synchronously even when
        managed, like contract verification, keep the default.
    """
    scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
    logs: Logs = Logs()
    context: ScanContext = (
        BatchedScanContext(soda_cloud, scan_id, logs)
        if batched and scan_id and soda_cloud
        else AtomicScanContext(soda_cloud)
    )
    with using_scan_context(context):
        try:
            exit_code: ExitCode = command(logs)
        except ScanExecutionFailedException as exc:
            # Expected failure: the message is logged without a traceback.
            soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} {exc}")
            return report_scan_execution_failure(soda_cloud, logs.records_for_failure_report())
        except Exception as exc:
            soda_logger.exception(f"Scan execution failed: {exc}")
            return report_scan_execution_failure(soda_cloud, logs.records_for_failure_report())
        finally:
            logs.close()  # the stream's final flush, before end_scan
    # Only reached when no failure was reported: a scan must never receive both
    # sodaCoreMarkScanFailed and sodaCoreScanEndAsync. A rejected end returns an exit code
    # instead of raising — exit 1 would read as "checks failed" to the launcher.
    if not context.end_scan():
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    return exit_code
