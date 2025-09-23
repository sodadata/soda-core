from enum import IntEnum


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
