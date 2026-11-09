from enum import IntEnum
from typing import Protocol


class ExitCode(IntEnum):
    """Possible exit codes and their meaning.

    0: all checks passed, no errors
    1: one or more checks failed
    2: one or more checks warned (no failures)
    3: errors occurred during execution (e.g. parse or engine errors)
    4: results could not be sent to Soda Cloud
    5: Soda Cloud rejected the results for a reason in the contract or the organization's
       configuration, such as a check attribute the organization does not have. The results
       were not stored. Fix the contract or the organization's settings; retrying won't help.
    """

    OK = 0
    CHECK_FAILURES = 1
    CHECK_WARNINGS = 2
    LOG_ERRORS = 3
    RESULTS_NOT_SENT_TO_CLOUD = 4
    RESULTS_REJECTED_BY_SODA_CLOUD = 5


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
       results didn't land in Soda Cloud). When every failed send was a
       customer-caused rejection by Soda Cloud, see
       ``results_send_failure_exit_code``.
    2. Engine/parse ERROR status next.
    3. Check FAILURES.
    4. Check WARNINGS.
    5. Otherwise OK.
    """
    if session_result.sending_results_to_soda_cloud_failed:
        return results_send_failure_exit_code(session_result)
    if session_result.has_errors:
        return ExitCode.LOG_ERRORS
    if session_result.is_failed:
        return ExitCode.CHECK_FAILURES
    if session_result.is_warned:
        return ExitCode.CHECK_WARNINGS
    return ExitCode.OK


def results_send_failure_exit_code(session_result: object) -> ExitCode:
    """The exit code for a run whose results did not reach Soda Cloud.

    ``RESULTS_REJECTED_BY_SODA_CLOUD`` when Soda Cloud rejected every failed upload for a
    customer-caused reason (the session's ``results_rejected_by_soda_cloud``), so the Soda
    Runner can tell a contract mistake apart from a platform failure.
    ``RESULTS_NOT_SENT_TO_CLOUD`` otherwise. Read with ``getattr`` so session types that don't
    expose the property (e.g. outside soda-core) keep exit code 4.
    """
    if getattr(session_result, "results_rejected_by_soda_cloud", False) is True:
        return ExitCode.RESULTS_REJECTED_BY_SODA_CLOUD
    return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
