import os
from typing import Any

import pandas as pd

from soda.scientific.anomaly_detection_v2.globals import DETECTOR_MESSAGES
from soda.scientific.anomaly_detection_v2.pydantic_models import FreqDetectionResult


class SuppressStdoutStderr:
    """
    Contex manager to do deep log suppression.

    Suppresses stdout and stderr in
    Python, i.e. will suppress all print, even if the print originates in a
    compiled C/Fortran sub-function.
       This will not suppress raised exceptions, since exceptions are printed
    to stderr just before a script exits, and after the context manager has
    exited (at least, I think that is why it lets exceptions through).
    """

    def __init__(self) -> None:
        # Open a pair of null files
        self.null_fds = [os.open(os.devnull, os.O_RDWR) for x in range(2)]
        # Save the actual stdout (1) and stderr (2) file descriptors.
        self.save_fds = [os.dup(1), os.dup(2)]

    def __enter__(self) -> None:
        # Assign the null pointers to stdout and stderr.
        os.dup2(self.null_fds[0], 1)
        os.dup2(self.null_fds[1], 2)

    def __exit__(self, *_: Any) -> None:
        # Re-assign the real stdout/stderr back to (1) and (2)
        os.dup2(self.save_fds[0], 1)
        os.dup2(self.save_fds[1], 2)
        # Close the null files
        for fd in self.null_fds + self.save_fds:
            os.close(fd)


def get_not_enough_measurements_freq_result(n_data_points: int) -> FreqDetectionResult:
    return FreqDetectionResult(
        inferred_frequency=None,
        df=pd.DataFrame(),
        freq_detection_strategy="not_enough_measurements",
        error_code_int=DETECTOR_MESSAGES["not_enough_measurements"].error_code_int,
        error_code=DETECTOR_MESSAGES["not_enough_measurements"].error_code_str,
        error_severity=DETECTOR_MESSAGES["not_enough_measurements"].severity,
        error_message=DETECTOR_MESSAGES["not_enough_measurements"].log_message.format(n_data_points=n_data_points),
    )
