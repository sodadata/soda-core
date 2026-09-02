"""Batched scan ingestion for Cloud-launched CLI flows.

A managed scan (``SODA_SCAN_ID`` set by the Runner/launcher) delivers nothing to Soda Cloud until the run
ends: logs and results travel in one end-of-run payload. ``run_batched_scan`` brackets a results-publishing
command so results go through the async ingestion pipeline (``sodaCoreScanStart`` →
``sodaCoreInsertScanDataBatch`` → ``sodaCoreScanEndAsync``) and logs stream while it runs (scan-id-keyed
``batchV4``). Without a scan id it is exactly ``run_with_failure_reporting`` + today's sync inserts.

The scan start cannot happen at bracket time: ``sodaCoreScanStart`` requires the scan-definition name, data
source name and data timestamp, which each flow only knows once its dependencies resolve inside the wrapped
command — and the backend only accepts ``batchV4`` log uploads after a successful start. The flow therefore
calls ``context.start_scan(...)`` as soon as it has resolved those values (before its engine work, so the
expensive phase streams); the context then upgrades the run's ``Logs`` from the in-memory collector to a
streaming queue, replaying what was already captured. A run that never starts (ad-hoc, or a rejected start)
stays fully in-memory, so its results payload carries the logs exactly as today and nothing is ever lost to a
stream the backend would reject.

This is opt-in composition sugar: the pieces (``BatchedScanContext``, the ``SodaCloud`` transport methods,
``run_with_failure_reporting``) are usable directly by any target that needs a different shape.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.batched_scan import BatchedScanContext
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs

if TYPE_CHECKING:
    from soda_core.common.soda_cloud import SodaCloud


def run_batched_scan(
    soda_cloud: SodaCloud,
    command: Callable[[BatchedScanContext], ExitCode],
) -> ExitCode:
    """Run a results-publishing command with batched ingestion when managed.

    Policies:
    - No ``SODA_SCAN_ID`` → fully today's behavior: in-memory ``Logs``, and ``context.insert_results`` is the
      sync ``insert_scan_results``.
    - The command opens the bracket itself via ``context.start_scan`` once its dependencies resolve; a
      failed/absent start degrades to the sync insert with in-memory logs.
    - Ordering: results insert (inside the command) → final log flush (the wrapper's ``finally`` close) →
      ``scan_end_async``. On an escaped failure the report goes out first, attaching the records the gatherer
      selects (unsent errors when streaming).
    - ``scan_end_async`` is only sent for a started scan whose results were acknowledged; a run that could not
      deliver leaves the scan un-ended, so the failure report / the launcher's exit-code fallback owns its
      terminal state instead of an empty "clean" end. An unaccepted end is a warning, not fatal.
    """
    from soda_core.cli.handlers.dependencies import run_with_failure_reporting

    scan_id = EnvConfigHelper().soda_scan_id
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id=scan_id)
    # run_with_failure_reporting owns the Logs lifecycle: the failure report happens inside it, and its
    # finally-close is the stream's final flush — both before scan_end_async below. Deliberately NOT a
    # try/finally around the end: a BaseException (KeyboardInterrupt, SystemExit) must not end the scan either.
    exit_code: ExitCode = run_with_failure_reporting(soda_cloud, lambda _logs: command(context), logs=context.logs)
    if context.scan_reference is not None and context.results_delivered:
        if not soda_cloud.scan_end_async(context.scan_reference):
            soda_logger.warning(
                f"sodaCoreScanEndAsync for scanReference '{context.scan_reference}' was not accepted; "
                f"the scan's results were already acknowledged."
            )
    return exit_code
