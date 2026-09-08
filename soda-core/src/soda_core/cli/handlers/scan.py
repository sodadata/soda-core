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
    """Run a results-publishing command under an installed ``ScanContext``, with every failure
    mapped to an exit code and reported to Soda Cloud in one place.

    ``batched`` declares that the flow participates in batched ingestion; ``SODA_SCAN_ID`` decides
    whether that activates. Contract verification keeps the default: it ingests synchronously even
    when managed, so its context must not claim ``is_batched``.

    This is the single Cloud-marking site for exceptions escaping a command, so exactly one
    ``sodaCoreMarkScanFailed`` reaches the backend per failed run. ``soda_cloud`` is the reporting
    channel (``None`` when the command can run without Cloud: an escaped failure then exits 4 for a
    managed run, 3 ad-hoc). A ``ScanExecutionFailedException`` is logged clean; any other exception
    with its traceback. Both report with the records the gatherer selects for a failure report.
    Otherwise the command's exit code is returned unchanged.

    Ordering: results insert (inside the command), final log flush (the ``finally`` close), then
    ``end_scan``. The end is only reached on a clean return, so a failed or cancelled run never
    sends a second terminal transition after ``sodaCoreMarkScanFailed``. A rejected end exits
    ``RESULTS_NOT_SENT_TO_CLOUD`` rather than raising: exit 1 would read as "checks failed".
    """
    scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
    context: ScanContext = (
        BatchedScanContext(soda_cloud, scan_id) if batched and scan_id and soda_cloud else AtomicScanContext(soda_cloud)
    )
    logs: Logs = Logs()
    context.logs = logs
    with using_scan_context(context):
        try:
            exit_code: ExitCode = command(logs)
        except ScanExecutionFailedException as exc:
            soda_logger.error(f"{Emoticons.POLICE_CAR_LIGHT} {exc}")
            return report_scan_execution_failure(soda_cloud, logs.records_for_failure_report())
        except Exception as exc:
            soda_logger.exception(f"Scan execution failed: {exc}")
            return report_scan_execution_failure(soda_cloud, logs.records_for_failure_report())
        finally:
            logs.close()
    if not context.end_scan():
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    return exit_code
