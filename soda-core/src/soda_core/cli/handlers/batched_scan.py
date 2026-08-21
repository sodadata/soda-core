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

This is opt-in composition sugar: the pieces (``build_streaming_logs``, the ``SodaCloud`` transport methods,
``run_with_failure_reporting``) are usable directly by any target that needs a different shape.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs

if TYPE_CHECKING:
    from soda_core.common.soda_cloud import SodaCloud
    from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO


@dataclass
class BatchedScanContext:
    """What a batched-scan command needs from the bracket around it.

    ``scan_id`` discriminates managed from ad-hoc runs (targets that must keep a distinct ad-hoc path branch on
    it); ``scan_reference`` is set by a successful ``start_scan`` — ``insert_results`` falls back to the sync
    upload without it, so callers never branch on it for sending. ``results_delivered`` records an acknowledged
    upload: the bracket only closes the scan (``scan_end_async``) when it is set, so a run whose results never
    made it leaves the scan open for the failure report / launcher fallback instead of ending it "cleanly".
    """

    logs: Logs
    soda_cloud: SodaCloud
    scan_id: Optional[str]
    stage: str = "main"
    scan_reference: Optional[str] = None
    results_delivered: bool = field(default=False, repr=False)
    _start_attempted: bool = field(default=False, repr=False)

    def start_scan(
        self,
        definition_name: str,
        default_data_source: str,
        data_timestamp: Optional[datetime] = None,
    ) -> None:
        """Open the async ingestion bracket once the flow has resolved the backend-mandatory scan coordinates.
        Call before the engine work: on success the run's logs switch to the scan's Cloud log stream (with the
        records captured so far replayed into it), so the expensive phase is visible mid-run. No-op on ad-hoc
        runs and on repeat calls; a rejected start warns and leaves the run on the sync path with in-memory
        logs.
        """
        # Deferred: importing logs_queue (and via it soda_cloud) before the contracts package initializes trips
        # the pre-existing soda_cloud<->contracts import cycle — this module is imported early by cli.py.
        from soda_core.common.logs_queue import LogsQueue

        if not self.scan_id or self._start_attempted:
            return
        self._start_attempted = True
        scan_reference: Optional[str] = self.soda_cloud.scan_start(
            self.scan_id, definition_name, default_data_source, data_timestamp
        )
        if scan_reference is None:
            soda_logger.warning(
                "Could not start the batched-ingestion scan on Soda Cloud; "
                "falling back to the synchronous results upload with in-memory logs."
            )
            return
        self.scan_reference = scan_reference
        # The queue is adopted by the run's existing Logs (which stays the active capture target; the bracket
        # owns its lifecycle).
        self.logs.switch_gatherer(
            LogsQueue(soda_cloud=self.soda_cloud, stage=self.stage, scan_id=self.scan_id, dataset="")
        )

    def insert_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        accepted: bool = (
            self.soda_cloud.insert_scan_data_batch(payload, self.scan_reference)
            if self.scan_reference
            else self.soda_cloud.insert_scan_results(payload)
        )
        if accepted:
            self.results_delivered = True
        return accepted


def run_batched_scan(
    soda_cloud: SodaCloud,
    stage: str,
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

    scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
    logs = Logs()
    context = BatchedScanContext(logs=logs, soda_cloud=soda_cloud, scan_id=scan_id, stage=stage)
    # run_with_failure_reporting owns the Logs lifecycle: the failure report happens inside it, and its
    # finally-close is the stream's final flush — both before scan_end_async below. Deliberately NOT a
    # try/finally around the end: a BaseException (KeyboardInterrupt, SystemExit) must not end the scan either.
    exit_code: ExitCode = run_with_failure_reporting(soda_cloud, lambda _logs: command(context), logs=logs)
    if context.scan_reference is not None and context.results_delivered:
        if not soda_cloud.scan_end_async(context.scan_reference):
            soda_logger.warning(
                f"sodaCoreScanEndAsync for scanReference '{context.scan_reference}' was not accepted; "
                f"the scan's results were already acknowledged."
            )
    return exit_code
