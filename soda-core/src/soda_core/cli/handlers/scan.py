"""The bracket every CLI results-publishing command runs under.

``run_scan`` owns the run's lifecycle in one place: it selects and installs the ``ScanContext``
variant (batched when ``SODA_SCAN_ID`` is set by the Runner/launcher, atomic otherwise — the single
site of that discrimination), owns the ``Logs`` lifecycle, maps every escaped failure to a failure
report + exit code, and closes the scan's ingestion. Flows source the installed context with
``get_scan_context()`` — no parameter threading.

The scan start cannot happen at bracket time: ``sodaCoreScanStart`` requires the scan-definition name,
data source name and data timestamp, which each flow only knows once its dependencies resolve inside
the wrapped command — and the backend only accepts ``batchV4`` log uploads after a successful start.
The flow therefore calls ``get_scan_context().start_scan(...)`` as soon as it has resolved those
values (before its engine work, so the expensive phase streams); the context then upgrades the run's
``Logs`` from the in-memory collector to a streaming queue, replaying what was already captured. An
ad-hoc run stays fully in-memory, so its results payload carries the logs exactly as today; a managed
run whose start fails is failed by ``start_scan`` itself and takes the standard failure mapping below.
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

    ``batched`` is the wiring's declaration that its flow participates in batched ingestion
    (calls ``start_scan``/``insert_results`` on the context); the environment (``SODA_SCAN_ID``)
    only decides whether that participation activates. A flow that ingests atomically regardless
    of being managed — contract verification — keeps the default, so its context never claims
    ``is_batched`` (an engine consumer sourcing the context mid-session must see the run's real
    ingestion mode, not the environment's potential).

    This is the single Cloud-marking site for exceptions escaping a command: the engine layers
    underneath re-raise without touching Cloud, so exactly one ``sodaCoreMarkScanFailed`` reaches
    the backend per failed run. Receives the already-constructed reporting channel (``soda_cloud``;
    ``None`` when the command can run without Cloud — an escaped failure then exits 4 for a managed
    run, 3 ad-hoc); dependency construction lives at the wiring layer, which decides what resolves
    inside the command.

    Owns the ``Logs`` lifecycle — the collector starts before the command, so in-command resolution
    failures are captured too. The ``logs`` handed to the command is the run-level capture target: a
    command running a check-collection session threads it through (each impl built with
    ``logs=logs``), and the context holds it so a mid-run ``start_scan`` upgrades it to the scan's
    Cloud log stream. This is the single logging site for command failures; the two except arms only
    pick the log form:

    - ``ScanExecutionFailedException``: an expected/validation failure — its user-facing message is
      logged clean, without a traceback.
    - Any other exception: unexpected — logged with the traceback.

    Both then report via ``report_scan_execution_failure`` with the records the gatherer selects
    (everything for the in-memory collector, only unsent error records for a streaming queue; the
    failure line is logged before the records are captured, so it is part of the report). Otherwise
    the command's own exit code is returned unchanged.

    Ordering: results insert (inside the command) → final log flush (the ``finally`` close) →
    ``end_scan``. The end is only reached on a clean return — a reported failure returns from its
    except arm and a ``BaseException`` (SIGTERM, pod eviction) propagates, so a failed or cancelled
    run never sends a second terminal transition after ``sodaCoreMarkScanFailed``; whether a clean
    run actually ends its scan is the context's own gate (every upload acknowledged). A rejected end
    means the run's results did not reach Soda Cloud: ``RESULTS_NOT_SENT_TO_CLOUD``, never a raw
    exception — an uncaught raise would exit 1, which the launcher reads as "checks failed" rather
    than "results never ingested".
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
