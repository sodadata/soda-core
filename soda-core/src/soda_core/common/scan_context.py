"""The scan context a results-publishing flow runs under — always present, sourced from a ContextVar.

Every CLI results-publishing run has a ``ScanContext``: the CLI bracket (``cli.handlers.scan.run_scan``)
selects a variant and installs it for the duration of the run, and flows source it with
``get_scan_context()`` wherever they need it — no parameter threading. Outside any bracket (library use,
direct handler invocation) the variable falls back to an inert atomic context, so the notion never
degenerates to ``None`` checks:

- ``AtomicScanContext`` — one indivisible end-of-run upload: ``start_scan`` is a no-op, results go out
  as a single synchronous ``sodaCoreInsertScanResults``, and there is nothing to close.
- ``BatchedScanContext`` — a managed scan (``SODA_SCAN_ID`` set by the Runner/launcher) sends results
  through the async ingestion pipeline (``sodaCoreScanStart`` → ``sodaCoreInsertScanDataBatch`` →
  ``sodaCoreScanEndAsync``) and streams its logs mid-run over the scan-id-keyed ``batchV4`` endpoint.

The variant choice is the single place the managed/ad-hoc discrimination happens; consumers program
against the shared interface and only consult ``is_batched`` where behavior genuinely diverges beyond
it (e.g. metric monitoring's payload-route choice).

The module lives in ``common`` because the engine (``check_collections`` subtypes) sources the context —
a coordinator over ``Logs`` + ``SodaCloud`` belongs below the CLI layer. Only the bracket that selects,
installs, and closes a context lives in ``cli.handlers.scan``.

Sourcing mirrors ``logs._active_logs``: a ContextVar, set/reset by ``install_scan_context``. The only
worker thread in a run is the log stream's flusher, which never reads the context, so consumers always
see the bracket's installation.
"""

from __future__ import annotations

import contextvars
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Iterator, Optional

from soda_core.common.logging_constants import Emoticons, soda_logger
from soda_core.common.logs import Logs

if TYPE_CHECKING:
    from soda_core.common.soda_cloud import SodaCloud
    from soda_core.common.soda_cloud_dto import SodaCoreInsertScanResultsDTO


class ScanContext(ABC):
    """What a results-publishing flow needs from the run around it.

    ``results_delivered`` / ``results_rejected`` record upload outcomes so ``end_scan`` only closes a
    scan whose every upload was acknowledged — a run that could not deliver leaves the scan's terminal
    state to the failure report / launcher fallback instead of ending it "cleanly" with results missing.
    ``logs`` is the run-level ``Logs``; the bracket sets it so a mid-run ``start_scan`` upgrades the
    right capture target (the engine may have per-collection targets active at that moment).
    """

    is_batched: bool = False

    def __init__(self, soda_cloud: Optional[SodaCloud]):
        self.soda_cloud: Optional[SodaCloud] = soda_cloud
        self.logs: Optional[Logs] = None
        self.results_delivered: bool = False
        self.results_rejected: bool = False

    def start_scan(
        self,
        definition_name: str,
        default_data_source: str,
        data_timestamp: Optional[datetime] = None,
    ) -> None:
        """Open the run's ingestion once the flow has resolved the backend-mandatory scan
        coordinates. A no-op except on a batched context, where it must be called before the engine
        work so the expensive phase streams its logs."""

    def insert_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        """Send one results payload the way this context ingests. The context owns the command type
        stamp (on a copy), so flows build one payload and never branch on the mode."""
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
        """Close the run's ingestion after a clean, unreported run; False means the run's results did
        not reach Soda Cloud (the bracket maps it to ``RESULTS_NOT_SENT_TO_CLOUD``)."""
        return True


class AtomicScanContext(ScanContext):
    """One indivisible end-of-run upload — today's ad-hoc behavior.

    The module default is an inert instance without a Cloud client: flows running outside any bracket
    can source and query it, but inserting results requires a bracket-installed context.
    """

    def _send_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        assert self.soda_cloud is not None, (
            "insert_results needs a Soda Cloud client: run the flow under cli.handlers.scan.run_scan "
            "(or install_scan_context a context that has one)."
        )
        return self.soda_cloud.insert_scan_results({**payload, "type": "sodaCoreInsertScanResults"})


class BatchedScanContext(ScanContext):
    """The async ingestion pipeline of a managed scan (``SODA_SCAN_ID`` set by the Runner/launcher).

    ``scan_reference`` is set by a successful ``start_scan``; without it (rejected start) the context
    degrades to the atomic send with in-memory logs, so callers never branch on it.
    """

    is_batched = True

    def __init__(self, soda_cloud: SodaCloud, scan_id: str):
        super().__init__(soda_cloud)
        self.scan_id: str = scan_id
        self.scan_reference: Optional[str] = None
        self._start_attempted: bool = False

    def start_scan(
        self,
        definition_name: str,
        default_data_source: str,
        data_timestamp: Optional[datetime] = None,
    ) -> None:
        """Send ``sodaCoreScanStart``. On success the run's logs switch to the scan's Cloud log
        stream (with the records captured so far replayed into it), so the phases after it are
        visible mid-run. No-op on repeat calls; a rejected start warns and leaves the run on the
        atomic send with in-memory logs — the backend refuses ``batchV4`` uploads for a
        never-started scan, so streaming would lose the run's logs entirely.
        """
        # Deferred: logs_queue imports soda_cloud, and pulling that chain in at module-import time
        # (the CLI wiring imports this module early) trips the pre-existing soda_cloud<->contracts
        # import cycle.
        from soda_core.common.logs_queue import build_streaming_gatherer

        if self._start_attempted:
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
        if gatherer is not None and self.logs is not None:
            # Adopted by the run's existing Logs, which stays the active capture target; the
            # bracket owns its lifecycle.
            self.logs.switch_gatherer(gatherer)

    def _send_results(self, payload: SodaCoreInsertScanResultsDTO) -> bool:
        if self.scan_reference:
            return self.soda_cloud.insert_scan_data_batch(payload, self.scan_reference)
        return self.soda_cloud.insert_scan_results({**payload, "type": "sodaCoreInsertScanResults"})

    def end_scan(self) -> bool:
        """Send ``sodaCoreScanEndAsync`` — only for a run whose scan started and whose every upload
        was acknowledged; otherwise the terminal state is left to the failure report / launcher
        fallback (ending anyway would close the scan "cleanly" with results missing, or send a
        second terminal transition after a ``sodaCoreMarkScanFailed``).

        A rejected end is fatal: batch uploads only reach object storage — nothing ingests them
        until the end command triggers reassembly, and no backend sweeper does it later, so a lost
        end loses the whole run's results. No retries here (deliberate, for now): Cloud-command
        retries are a global concern tracked separately, not per-command loops.
        """
        if self.scan_reference is None or not self.results_delivered or self.results_rejected:
            return True
        try:
            # This runs after the bracket's failure boundary has closed: a raise escaping here
            # would exit 1, which the launcher reads as "checks failed" rather than "results
            # never ingested" — so every failure shape becomes the False return.
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

# What get_scan_context falls back to outside any bracket. Shared and stateless-by-convention:
# nothing should insert results through it (its assert says how to get a real one).
_default_scan_context = AtomicScanContext(soda_cloud=None)


def get_scan_context() -> ScanContext:
    """The installed scan context, or the inert atomic default outside any bracket."""
    installed: Optional[ScanContext] = _scan_context.get()
    return installed if installed is not None else _default_scan_context


@contextmanager
def install_scan_context(scan_context: ScanContext) -> Iterator[ScanContext]:
    """Make ``scan_context`` the run's context for the block — the bracket around every CLI
    results-publishing command; tests use it to run flows against a chosen variant."""
    token = _scan_context.set(scan_context)
    try:
        yield scan_context
    finally:
        _scan_context.reset(token)
