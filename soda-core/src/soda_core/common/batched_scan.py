"""The context a batched-scan command receives from its CLI bracket.

A managed scan (``SODA_SCAN_ID`` set by the Runner/launcher) sends its results through the async
ingestion pipeline (``sodaCoreScanStart`` → ``sodaCoreInsertScanDataBatch`` → ``sodaCoreScanEndAsync``)
and streams its logs mid-run over the scan-id-keyed ``batchV4`` endpoint. ``BatchedScanContext``
carries what a flow needs to participate: the run's ``Logs``, the Cloud client, and the scan
coordinates as they resolve. It lives in ``common`` because the engine (``check_collections``)
receives it — a data holder over ``Logs`` + ``SodaCloud`` belongs below the CLI layer. Only
``run_batched_scan``, the CLI bracket that creates one and owns its lifecycle, lives in
``cli.handlers.batched_scan``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Optional

from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs

if TYPE_CHECKING:
    from soda_core.common.soda_cloud import SodaCloud
    from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO


@dataclass
class BatchedScanContext:
    """What a batched-scan command needs from the bracket around it.

    ``scan_id`` discriminates managed from ad-hoc runs (targets that must keep a distinct ad-hoc path
    branch on it); ``scan_reference`` is set by a successful ``start_scan`` — ``insert_results`` falls
    back to the sync upload without it, so callers never branch on it for sending.
    ``results_delivered`` / ``results_rejected`` record upload outcomes: the bracket only closes the
    scan (``scan_end_async``) for a run whose every upload was acknowledged, so a run that could not
    deliver leaves the scan's terminal state to the failure report / launcher fallback instead of
    ending it "cleanly" with results missing.
    """

    logs: Logs
    soda_cloud: SodaCloud
    scan_id: Optional[str]
    scan_reference: Optional[str] = None
    results_delivered: bool = field(default=False, repr=False)
    results_rejected: bool = field(default=False, repr=False)
    _start_attempted: bool = field(default=False, repr=False)

    def start_scan(
        self,
        definition_name: str,
        default_data_source: str,
        data_timestamp: Optional[datetime] = None,
    ) -> None:
        """Open the async ingestion bracket once the flow has resolved the backend-mandatory scan
        coordinates. Call before the engine work: on success the run's logs switch to the scan's
        Cloud log stream (with the records captured so far replayed into it), so the expensive phase
        is visible mid-run. No-op on ad-hoc runs and on repeat calls; a rejected start warns and
        leaves the run on the sync path with in-memory logs — the backend refuses ``batchV4`` uploads
        for a never-started scan, so streaming would lose the run's logs entirely.
        """
        # Deferred: logs_queue imports soda_cloud, and pulling that chain in at module-import time
        # (the CLI wiring imports this module early) trips the pre-existing soda_cloud<->contracts
        # import cycle.
        from soda_core.common.logs_queue import build_streaming_gatherer

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
        gatherer = build_streaming_gatherer(self.soda_cloud, scan_id=self.scan_id)
        if gatherer is not None:
            # Adopted by the run's existing Logs, which stays the active capture target; the
            # bracket owns its lifecycle.
            self.logs.switch_gatherer(gatherer)

    def insert_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        """Send one results payload: the async batch pipeline when the scan started, the sync
        command otherwise. The context owns the command type stamp (on a copy), so flows build one
        payload and never branch on the mode."""
        accepted: bool = (
            self.soda_cloud.insert_scan_data_batch(payload, self.scan_reference)
            if self.scan_reference
            else self.soda_cloud.insert_scan_results({**payload, "type": "sodaCoreInsertScanResults"})
        )
        if accepted:
            self.results_delivered = True
        else:
            self.results_rejected = True
        return accepted
