from typing import Dict

from soda.scientific.anomaly_detection_v2.pydantic_models import (
    DetectorMessageComponent,
)

ERROR_CODE_LEVEL_CUTTOFF = 99

DETECTOR_MESSAGES: Dict[str, DetectorMessageComponent] = {
    "native_freq": DetectorMessageComponent(
        log_message="native frequency detected",
        severity="info",
        error_code_int=0,
        error_code_str="Native frequency is detected successfully",
    ),
    "converted_daily_no_dupes": DetectorMessageComponent(
        log_message="converted to daily frequency no dupes with time info removed",
        severity="info",
        error_code_int=1,
        error_code_str="",
    ),
    "coerced_daily": DetectorMessageComponent(
        log_message="Coerced to daily frequency with last daily time point kept",
        severity="warn",
        error_code_int=2,
        error_code_str="made_daily_keeping_last_point_only",
    ),
    "last_four": DetectorMessageComponent(
        log_message="Frequency inferred from the last 4 stable data points",
        severity="warn",
        error_code_int=3,
        error_code_str="frequency_from_last_4_points",
    ),
    "manual_freq": DetectorMessageComponent(
        log_message="Manual frequency provided by SodaCL",
        severity="info",
        error_code_int=4,
        error_code_str="Manual frequency is detected successfully",
    ),
    "not_enough_measurements": DetectorMessageComponent(
        log_message="Data frame must have at least 4 measurements",
        severity="error",
        error_code_int=100,
        error_code_str="not_enough_measurements",
    ),
    "bailing_out": DetectorMessageComponent(
        log_message="All attempts to detect the datset frequency failed. Process terminated.",
        severity="error",
        error_code_int=ERROR_CODE_LEVEL_CUTTOFF,
        error_code_str="all_freq_detection_attempts_failed",
    ),
}
