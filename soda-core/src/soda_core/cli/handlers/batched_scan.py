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
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs

if TYPE_CHECKING:
    from soda_core.common.soda_cloud import SodaCloud

SCAN_END_ATTEMPTS = 3


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
      ``scan_end_async``. On an escaped failure the stream is flushed and the report attaches only what it
      could not deliver (the gatherer owns that rule — ``records_for_failure_report``).
    - ``scan_end_async`` is only sent for a run that actually completed: the scan started, every upload was
      acknowledged, and no failure escaped the command. A failure after a delivered upload already sent
      ``sodaCoreMarkScanFailed`` — ending the scan too would send two terminal transitions for one run. A
      ``BaseException`` (SIGTERM, pod eviction) propagates before the end is reached, so a cancelled run
      never ends the scan either.
    - A rejected ``scan_end_async`` is fatal: batch uploads only reach object storage — nothing ingests them
      until the end command triggers reassembly, and no backend sweeper does it later. A lost end therefore
      loses the whole run's results, so it is retried and then mapped to ``RESULTS_NOT_SENT_TO_CLOUD``
      (never a raw exception: an uncaught raise would exit 1, which the launcher reads as "checks failed"
      rather than "results never ingested").
    """
    from soda_core.cli.handlers.dependencies import run_with_failure_reporting

    scan_id = EnvConfigHelper().soda_scan_id
    context = BatchedScanContext(logs=Logs(), soda_cloud=soda_cloud, scan_id=scan_id)

    escaped_failure = False

    def guarded_command(_logs: Logs) -> ExitCode:
        nonlocal escaped_failure
        try:
            return command(context)
        except BaseException:
            # Seen here first so the end-of-scan gate below knows a failure escaped; the exception
            # continues into run_with_failure_reporting (reporting + exit-code mapping) or, for a
            # BaseException, out of the CLI entirely.
            escaped_failure = True
            raise

    # run_with_failure_reporting owns the Logs lifecycle: the failure report happens inside it, and its
    # finally-close is the stream's final flush — both before scan_end_async below.
    exit_code: ExitCode = run_with_failure_reporting(soda_cloud, guarded_command, logs=context.logs)
    if (
        context.scan_reference is not None
        and context.results_delivered
        and not context.results_rejected
        and not escaped_failure
    ):
        if not _end_scan(soda_cloud, context.scan_reference):
            return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    return exit_code


def _end_scan(soda_cloud: SodaCloud, scan_reference: str) -> bool:
    for attempt in range(SCAN_END_ATTEMPTS):
        try:
            if soda_cloud.scan_end_async(scan_reference):
                return True
            reason = "not accepted"
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
        if attempt < SCAN_END_ATTEMPTS - 1:
            soda_logger.warning(f"sodaCoreScanEndAsync was {reason}; retrying {attempt + 2}/{SCAN_END_ATTEMPTS}")
    soda_logger.error(
        f"{Emoticons.POLICE_CAR_LIGHT} sodaCoreScanEndAsync for scanReference '{scan_reference}' failed "
        f"({reason}) after {SCAN_END_ATTEMPTS} attempts. The uploaded batches are not ingested without it, "
        f"so the run's results did not reach Soda Cloud."
    )
    return False
