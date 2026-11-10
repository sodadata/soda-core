from enum import IntEnum
from typing import Protocol


class ExitCode(IntEnum):
    """Possible exit codes and their meaning.

    0: all checks passed, no errors
    1: one or more checks failed
    2: one or more checks warned (no failures)
    3: errors occurred during execution (e.g. parse or engine errors)
    4: results could not be sent to Soda Cloud

    A contract verification whose results Soda Cloud refused only because the user cancelled the
    scan exits with its check outcome (0, 1, 2 or 3): the results were discarded on purpose. Other
    runs that publish to Soda Cloud, such as ``soda data-source discover``, do not detect a
    cancelled scan and still exit 4 when Soda Cloud refuses their results.
    """

    OK = 0
    CHECK_FAILURES = 1
    CHECK_WARNINGS = 2
    LOG_ERRORS = 3
    RESULTS_NOT_SENT_TO_CLOUD = 4


OK_CODES = {ExitCode.OK, ExitCode.CHECK_FAILURES}


class _HasRollups(Protocol):
    """Structural shape any session-style result must expose for exit-code mapping."""

    @property
    def has_errors(self) -> bool:
        ...

    @property
    def is_failed(self) -> bool:
        ...

    @property
    def is_warned(self) -> bool:
        ...

    @property
    def sending_results_to_soda_cloud_failed(self) -> bool:
        ...


def session_result_to_exit_code(session_result: _HasRollups) -> ExitCode:
    """Map a session result's rollup properties to a CLI exit code.

    Subtype-neutral: works on any session-style result that exposes the
    four rollup properties — ``CheckCollectionSessionResult`` and its
    subtypes (``DataStandardSessionResult``, ...). The legacy
    ``ContractVerificationSessionResult`` is NOT a structural match (it
    lacks ``sending_results_to_soda_cloud_failed``); contract-only CLI
    handlers map exit codes directly. Priority order:

    1. Cloud-send failure trumps everything (the user wants to know
       results didn't land in Soda Cloud). When every failed send was refused
       because the user cancelled the scan, it is ignored and the check
       outcome decides, see ``results_discarded_because_scan_canceled``.
    2. Engine/parse ERROR status next.
    3. Check FAILURES.
    4. Check WARNINGS.
    5. Otherwise OK.
    """
    if session_result.sending_results_to_soda_cloud_failed and not results_discarded_because_scan_canceled(
        session_result
    ):
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    if session_result.has_errors:
        return ExitCode.LOG_ERRORS
    if session_result.is_failed:
        return ExitCode.CHECK_FAILURES
    if session_result.is_warned:
        return ExitCode.CHECK_WARNINGS
    return ExitCode.OK


def results_discarded_because_scan_canceled(session_result: object) -> bool:
    """Whether every failed upload was refused because the user cancelled the scan in Soda Cloud.

    Such a run did what the user asked, so its exit code is the check outcome, never
    ``RESULTS_NOT_SENT_TO_CLOUD``. Any other send failure alongside it keeps exit code 4. Read with
    ``getattr`` so session types that don't expose the property (e.g. outside soda-core) keep the
    send-failure exit code.
    """
    return getattr(session_result, "results_discarded_because_scan_canceled", False) is True
