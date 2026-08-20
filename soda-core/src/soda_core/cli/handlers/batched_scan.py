"""Batched scan ingestion for Cloud-launched CLI flows.

A managed scan (``SODA_SCAN_ID`` set by the Runner/launcher) delivers nothing
to Soda Cloud until the run ends: logs and results travel in one end-of-run
payload. ``run_batched_scan`` brackets a results-publishing command so logs
stream while it runs (scan-id-keyed ``batchV4``) and results go through the
async ingestion pipeline (``sodaCoreScanStart`` → ``sodaCoreInsertScanDataBatch``
→ ``sodaCoreScanEndAsync``). Without a scan id it is exactly
``run_with_failure_reporting`` + today's sync inserts.

This is opt-in composition sugar: the pieces (``build_streaming_logs``, the
``SodaCloud`` transport methods, ``run_with_failure_reporting``) are usable
directly by any target that needs a different shape.
"""

from dataclasses import dataclass
from typing import Callable, Optional

from soda_core.cli.exit_codes import ExitCode
from soda_core.cli.handlers.data_source import build_streaming_logs
from soda_core.cli.handlers.dependencies import run_with_failure_reporting
from soda_core.common.env_config_helper import EnvConfigHelper
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO


@dataclass
class BatchedScanContext:
    """What a batched-scan command needs from the bracket around it.

    ``scan_id`` discriminates managed from ad-hoc runs (targets that must keep
    a distinct ad-hoc path branch on it); ``scan_reference`` is None when the
    run is ad-hoc or ``scan_start`` degraded — ``insert_results`` then falls
    back to the sync upload, so callers never branch on it for sending.
    """

    logs: Logs
    soda_cloud: SodaCloud
    scan_id: Optional[str]
    scan_reference: Optional[str]

    def insert_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        if self.scan_reference:
            return self.soda_cloud.insert_scan_data_batch(payload, self.scan_reference)
        return self.soda_cloud.insert_scan_results(payload)


def run_batched_scan(
    soda_cloud: SodaCloud,
    stage: str,
    command: Callable[[BatchedScanContext], ExitCode],
    definition_name: Optional[str] = None,
    default_data_source: Optional[str] = None,
) -> ExitCode:
    """Run a results-publishing command with batched ingestion when managed.

    Policies:
    - No ``SODA_SCAN_ID`` → fully today's behavior: in-memory ``Logs``, and
      ``context.insert_results`` is the sync ``insert_scan_results``.
    - ``scan_start`` failure → warn and degrade to the sync insert; log
      streaming keeps running (``batchV4`` is keyed by scan id, not
      scanReference).
    - Ordering: results insert (inside the command) → final log flush (the
      wrapper's ``finally`` close) → ``scan_end_async``. On an escaped
      failure the report goes out first, attaching the records the gatherer
      selects (unsent errors when streaming).
    - ``scan_end_async`` failure → warning, not fatal: results were already
      acknowledged (or the failure already reported).
    """
    scan_id: Optional[str] = EnvConfigHelper().soda_scan_id
    logs: Optional[Logs] = build_streaming_logs(soda_cloud, stage, scan_id) if scan_id else None
    logs = logs if logs is not None else Logs()

    scan_reference: Optional[str] = None
    if scan_id:
        scan_reference = soda_cloud.scan_start(scan_id, definition_name, default_data_source)
        if scan_reference is None:
            soda_logger.warning(
                "Could not start the batched-ingestion scan on Soda Cloud; "
                "falling back to the synchronous results upload."
            )

    context = BatchedScanContext(logs=logs, soda_cloud=soda_cloud, scan_id=scan_id, scan_reference=scan_reference)
    try:
        # run_with_failure_reporting owns the Logs lifecycle: the failure
        # report happens inside it, and its finally-close is the stream's
        # final flush — both before scan_end_async below.
        return run_with_failure_reporting(soda_cloud, lambda _logs: command(context), logs=logs)
    finally:
        if scan_reference is not None:
            if not soda_cloud.scan_end_async(scan_reference):
                soda_logger.warning(
                    f"sodaCoreScanEndAsync for scanReference '{scan_reference}' was not accepted; "
                    f"the scan's results were already acknowledged."
                )
