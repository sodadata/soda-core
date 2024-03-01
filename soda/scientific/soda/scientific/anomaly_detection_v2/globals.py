from typing import Dict

from soda.scientific.anomaly_detection_v2.pydantic_models import (
    DetectorMessageComponent,
)

ERROR_CODE_LEVEL_CUTTOFF = 99

DETECTOR_MESSAGES: Dict[str, DetectorMessageComponent] = {
    "native_freq": DetectorMessageComponent(
        log_message="Frequency Detection Info: native frequency detected",
        severity="info",
        error_code_int=0,
        error_code_str="Native frequency is detected successfully",
    ),
    "converted_daily_no_dupes": DetectorMessageComponent(
        log_message="Frequency Detection Info: Converted to daily frequency no dupes with time info removed",
        severity="info",
        error_code_int=1,
        error_code_str="converted_daily_no_dupes",
    ),
    "coerced_daily": DetectorMessageComponent(
        log_message=(
            "Frequency Detection Warning: Due to unpredictable time intervals, "
            "we have assumed a daily frequency. If more than 1 data point occurs "
            "in one day we take the last record. Free free to set the "
            "frequency manually in your SodaCL."
        ),
        severity="warn",
        error_code_int=2,
        error_code_str="made_daily_keeping_last_point_only_custom",
    ),
    "last_four": DetectorMessageComponent(
        log_message="Frequency Detection Warning: Frequency inferred from the last 4 stable data points",
        severity="warn",
        error_code_int=3,
        error_code_str="frequency_from_last_4_points",
    ),
    "manual_freq": DetectorMessageComponent(
        log_message="Frequency Detection Info: Frequency is set to '{frequency}' manually.",
        severity="info",
        error_code_int=4,
        error_code_str="manual_frequency",
    ),
    "not_enough_measurements_custom": DetectorMessageComponent(
        log_message=(
            "Anomaly Detection Insufficient Training Data Warning:"
            " The model requires a minimum of 5 historical measurements"
            " for accurate predictions, but currently has only {n_data_points}"
            " check results available."
        ),
        severity="error",
        error_code_int=100,
        error_code_str="not_enough_measurements_custom",
    ),
    "bailing_out": DetectorMessageComponent(
        log_message="Frequency Detection Error: All attempts to detect the datset frequency failed. Process terminated.",
        severity="error",
        error_code_int=ERROR_CODE_LEVEL_CUTTOFF,
        error_code_str="all_freq_detection_attempts_failed",
    ),
}

MANUAL_FREQUENCY_MAPPING = {
    "T": "T (minute)",
    "H": "H (hourly)",
    "D": "D (daily)",
    "W": "W (weekly)",
    "M": "M (monthly end)",
    "MS": "MS (monthly start)",
    "Q": "Q (quarterly end)",
    "QS": "QS (quarterly start)",
    "A": "A (yearly end)",
    "AS": "AS (yearly start)",
}

FEEDBACK_REASONS = {
    "expectedWeeklySeasonality": {
        "internal_remap": "weekly_seasonality",
        "frequency_unit": "W",
        "frequency_value": 1,
    },
    "expectedMonthlySeasonality": {
        "internal_remap": "monthly_seasonality",
        "frequency_unit": "M",
        "frequency_value": 1,
    },
    "expectedYearlySeasonality": {
        "internal_remap": "yearly_seasonality",
        "frequency_unit": "Y",
        "frequency_value": 1,
    },
}

EXTERNAL_REGRESSOR_COLUMNS = ["external_regressor_weekly", "external_regressor_monthly", "external_regressor_yearly"]

REQUIRED_FEEDBACK_COLUMNS = ["ds", "y", "skipMeasurements"]
