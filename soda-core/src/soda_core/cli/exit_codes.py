from enum import IntEnum
from typing import Protocol


class ExitCode(IntEnum):
    """Possible exit codes and their meaning.

    See https://docs.soda.io/soda-library/programmatic.html#scan-exit-codes
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
    four rollup properties (``CheckCollectionSessionResult`` and its
    subtypes do; ``ContractVerificationSessionResult`` does too, with
    its own equivalents). Priority order:

    1. Cloud-send failure trumps everything (the user wants to know
       results didn't land in Soda Cloud).
    2. Engine/parse ERROR status next.
    3. Check FAILURES.
    4. Check WARNINGS.
    5. Otherwise OK.
    """
    if session_result.sending_results_to_soda_cloud_failed:
        return ExitCode.RESULTS_NOT_SENT_TO_CLOUD
    if session_result.has_errors:
        return ExitCode.LOG_ERRORS
    if session_result.is_failed:
        return ExitCode.CHECK_FAILURES
    if session_result.is_warned:
        return ExitCode.CHECK_WARNINGS
    return ExitCode.OK
