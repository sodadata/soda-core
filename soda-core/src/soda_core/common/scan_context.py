"""The scan context a results-publishing flow runs under.

The CLI bracket (``cli.handlers.scan.run_scan``) picks a variant and installs it for the run;
flows read it with ``get_scan_context()`` instead of receiving it as a parameter. Outside any
bracket the accessor returns an inert atomic default, so callers never check for None.

- ``AtomicScanContext``: one synchronous end-of-run ``sodaCoreInsertScanResults`` upload.
- ``BatchedScanContext``: the async ingestion pipeline of a managed scan (``sodaCoreScanStart`` →
  ``sodaCoreInsertScanDataBatch`` → ``sodaCoreScanEndAsync``), with logs streaming mid-run over
  the scan-id-keyed ``batchV4`` endpoint.

Lives in ``common`` because engine code (``check_collections`` subtypes) reads the context.
Sourcing mirrors ``logs._active_logs``: a ContextVar, set and reset by ``using_scan_context``.
"""

from __future__ import annotations

import contextvars
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Iterator, Optional

from soda_core.common.exceptions import ScanExecutionFailedException
from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs

if TYPE_CHECKING:
    from soda_core.common.soda_cloud import SodaCloud
    from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO


class ScanContext(ABC):
    """What a results-publishing flow needs from the run around it.

    ``end_scan`` only closes a scan whose every upload was acknowledged.
    """

    # Whether the results insert hands back the Cloud-minted ids (scan, dataset, check) that
    # post-processing needs. False for a batch upload: it lands in object storage.
    provides_result_handles: bool = True

    def __init__(self, soda_cloud: Optional[SodaCloud]):
        self.soda_cloud: Optional[SodaCloud] = soda_cloud
        # The launcher-created scan this run reports into; None on an ad-hoc run. The run's
        # identity, so flows read it here instead of the environment.
        self.scan_id: Optional[str] = None
        self.results_delivered: bool = False
        self.results_rejected: bool = False

    def start_scan(
        self,
        definition_name: str,
        default_data_source: str,
        data_timestamp: Optional[datetime] = None,
    ) -> None:
        """Open the run's ingestion once the scan coordinates are resolved. A no-op except on a
        batched context, where it must run before the engine work so that phase streams its logs."""

    def insert_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        """Send one results payload; returns True when Soda Cloud accepted it."""
        accepted: bool = self._send_results(payload)
        if accepted:
            self.results_delivered = True
        else:
            self.results_rejected = True
        return accepted

    @abstractmethod
    def _send_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        pass

    def end_scan(self) -> bool:
        """Close the run's ingestion after a clean, unreported run. False means the results did not
        reach Soda Cloud; the bracket maps it to ``RESULTS_NOT_SENT_TO_CLOUD``."""
        return True


class AtomicScanContext(ScanContext):
    """One synchronous end-of-run upload — the ad-hoc behavior."""

    def _send_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        assert self.soda_cloud is not None, (
            "insert_results needs a Soda Cloud client: run the flow under cli.handlers.scan.run_scan "
            "(or inside using_scan_context with a context that has one)."
        )
        # The command type is stamped on a copy: flows build one payload regardless of the mode.
        return self.soda_cloud.insert_scan_results({**payload, "type": "sodaCoreInsertScanResults"})


class BatchedScanContext(ScanContext):
    """The async ingestion pipeline of a managed scan (``SODA_SCAN_ID`` set by the launcher)."""

    provides_result_handles = False

    def __init__(self, soda_cloud: SodaCloud, scan_id: str, logs: Logs):
        super().__init__(soda_cloud)
        self.scan_id = scan_id
        # The run's Logs, whose gatherer start_scan upgrades to the Cloud log stream.
        self.logs = logs
        self.scan_reference: Optional[str] = None
        self._start_attempted: bool = False

    def start_scan(
        self,
        definition_name: str,
        default_data_source: str,
        data_timestamp: Optional[datetime] = None,
    ) -> None:
        """Send ``sodaCoreScanStart`` and switch the run's logs to the Cloud log stream, replaying
        what was already captured. Only the first call starts; repeat calls are no-ops. Raises
        ``ScanExecutionFailedException`` when the start is rejected or the client call fails.
        """
        # Imported here: logs_queue pulls in soda_cloud, which at module-import time trips the
        # soda_cloud<->contracts import cycle.
        from soda_core.common.logs_queue import build_streaming_gatherer

        if self._start_attempted:
            return
        self._start_attempted = True
        # A failed start fails the run rather than falling back to the sync upload: the backend
        # refuses these deliberately, and continuing would hide the problem. Nothing has streamed
        # yet, so the failure report carries the full record list.
        try:
            scan_reference: Optional[str] = self.soda_cloud.scan_start(
                self.scan_id, definition_name, default_data_source, data_timestamp
            )
        except Exception as exc:
            raise ScanExecutionFailedException(
                f"Could not start the batched-ingestion scan '{self.scan_id}' on Soda Cloud: {exc}"
            ) from exc
        if scan_reference is None:
            raise ScanExecutionFailedException(
                f"Soda Cloud did not accept sodaCoreScanStart for scan '{self.scan_id}'."
            )
        self.scan_reference = scan_reference
        # The backend accepts batchV4 uploads only after the start, so the stream begins here.
        self.logs.switch_gatherer(build_streaming_gatherer(self.soda_cloud, scan_id=self.scan_id))

    def _send_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        # Inserting before start_scan is a flow bug; a failed start already failed the run.
        assert self.scan_reference, "insert_results on a batched scan requires a successful start_scan first."
        return self.soda_cloud.insert_scan_data_batch(payload, self.scan_reference)

    def end_scan(self) -> bool:
        """Send ``sodaCoreScanEndAsync``; False means the run's results did not reach Soda Cloud
        (nothing ingests the uploaded batches without the end command)."""
        # End only a scan whose every upload was acknowledged; otherwise the terminal state
        # belongs to the failure report / launcher fallback.
        if self.scan_reference is None or not self.results_delivered or self.results_rejected:
            return True
        try:
            # Runs after the failure boundary has closed: a raise here would exit 1, which the
            # launcher reads as "checks failed" instead of "results never ingested".
            if self.soda_cloud.scan_end_async(self.scan_reference):
                return True
            reason = "was not accepted"
        except Exception as exc:
            reason = f"raised {type(exc).__name__}: {exc}"
        soda_logger.error(
            f"{Emoticons.POLICE_CAR_LIGHT} sodaCoreScanEndAsync for scanReference '{self.scan_reference}' "
            f"{reason}. The uploaded batches are not ingested without it, so the run's results "
            f"did not reach Soda Cloud."
        )
        return False


_scan_context: contextvars.ContextVar[Optional[ScanContext]] = contextvars.ContextVar("soda_scan_context", default=None)


def get_scan_context() -> ScanContext:
    """The installed scan context, or a fresh inert atomic one outside any bracket.

    Fresh rather than shared: a context carries per-run state, so a process-wide instance would
    carry it between runs.
    """
    installed: Optional[ScanContext] = _scan_context.get()
    return installed if installed is not None else AtomicScanContext(soda_cloud=None)


@contextmanager
def using_scan_context(scan_context: ScanContext) -> Iterator[ScanContext]:
    """Make ``scan_context`` the run's context for the block."""
    token = _scan_context.set(scan_context)
    try:
        yield scan_context
    finally:
        _scan_context.reset(token)
