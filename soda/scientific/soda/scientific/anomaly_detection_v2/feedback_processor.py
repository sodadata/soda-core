"""Handles user feedback consumption."""

# extract the regularity
# generate a "fake" custom regressor which:
#   # captures the delta between the predicted and actual value,
#   # reproduces is forward at captured regularity with some smoothing/decay around the point

from datetime import date
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from soda.common.logs import Logs

FEEDBACK_REASONS = {
    "expectedDailySeasonality": {
        "internal_remap": "daily_seasonality",
        "frequency_unit": "D",
        "frequency_value": 1,
    },
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


WEEK_ANCHORED_OFFSETS = {
    "Monday": "MON",
    "Tuesday": "TUE",
    "Wednesday": "WED",
    "Thursday": "THU",
    "Friday": "FRI",
    "Saturday": "SAT",
    "Sunday": "SUN",
}


class FeedbackProcessor:
    """Processes user feedback."""

    def __init__(self, params: Dict[str, Any], df_historic: pd.DataFrame, logs: Logs):
        """Constructor for FeedbackProcessor."""
        self._logs = logs
        self._params = params
        self.df_historic = df_historic

    def get_processed_feedback_df(self) -> Tuple[bool, pd.DataFrame]:
        has_feedback = self.check_feedback()
        has_exegonenous_regressor = False
        if has_feedback:
            df_feedback_processed: pd.DataFrame = self.process_feedback()
            has_exegonenous_regressor, df_feedback_processed = self.derive_exogenous_regressor(
                df_feedback_processed=df_feedback_processed
            )
        else:
            df_feedback_processed = self.df_historic.copy()
        return has_exegonenous_regressor, df_feedback_processed

    def check_feedback(self) -> bool:
        df = self.df_historic.copy()
        if "feedback" in df.columns:
            return not df["feedback"].isnull().all()
        return False

    def process_feedback(self) -> pd.DataFrame:
        df = self.df_historic.copy()
        df["feedback"] = df["feedback"].fillna(pd.NA)
        df["feedback"] = df["feedback"].apply(lambda x: {} if pd.isnull(x) else x)

        feedback_array = df["feedback"].values
        df_flattened = pd.json_normalize(feedback_array)  # type: ignore
        df_feedback_processed = pd.merge(df, df_flattened, left_index=True, right_index=True)
        df_feedback_processed_cols = df_feedback_processed.columns

        if "reason" in df_feedback_processed_cols:
            df_feedback_processed["reason"] = df_feedback_processed["reason"].fillna("Invalid reason")
        else:
            df_feedback_processed["reason"] = "Invalid reason"

        df_feedback_processed["is_correctly_classified_anomaly"] = None
        # compute whether an anomaly was correctly classified
        if "isCorrectlyClassified" in df_feedback_processed_cols and "outcome" in df_feedback_processed_cols:
            df_feedback_processed["is_correctly_classified_anomaly"] = df_feedback_processed.apply(
                lambda x: self.find_is_correctly_classified_anomalies(
                    is_correctly_classified=x["isCorrectlyClassified"], outcome=x["outcome"]
                ),  # type: ignore
                axis=1,
            )
        return df_feedback_processed

    @staticmethod
    def find_is_correctly_classified_anomalies(
        is_correctly_classified: Optional[bool], outcome: Optional[str]
    ) -> Optional[bool]:
        is_fail_or_warn = outcome in ["warn", "fail"]
        if is_fail_or_warn is True and is_correctly_classified is True:
            return True
        elif is_fail_or_warn is True and is_correctly_classified is False:
            return False
        return None

    def derive_exogenous_regressor(self, df_feedback_processed: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
        has_exegonenous_regressor = False
        feedback_ref_mapping = pd.DataFrame.from_dict(FEEDBACK_REASONS, orient="index").reset_index()

        df_feedback_processed["predicted_to_real_delta"] = (
            df_feedback_processed["y"] - df_feedback_processed["anomaly_predicted_value"]
        )
        df_regressor_ref = df_feedback_processed.loc[
            df_feedback_processed["is_correctly_classified_anomaly"] == False
        ]  # noqa: E712
        self._logs.debug(f"Processing {len(df_regressor_ref)} user feedbacks")
        df_regressor_ref = df_regressor_ref.merge(feedback_ref_mapping, how="left", left_on="reason", right_on="index")

        # grab the day of the week
        df_regressor_ref["day_of_week"] = df_regressor_ref["ds"].dt.day_name()
        df_regressor_ref = df_regressor_ref.replace({"day_of_week": WEEK_ANCHORED_OFFSETS})

        # for each row in the ref table
        # build a date_range with start as misclass and end as today using frequency unit + DAY (if "W").
        offsets = pd.DataFrame()
        df_regressor_ref["ds"] = df_regressor_ref["ds"].dt.tz_localize(None)
        if not df_regressor_ref.empty:
            for _, misclassification in df_regressor_ref.iterrows():
                _offsets = pd.date_range(
                    misclassification["ds"],
                    date.today(),
                    freq=f'W-{misclassification["day_of_week"]}',
                    normalize=True,
                )
                _offsets = _offsets.to_frame(index=False, name="exg_reg_date")
                _offsets["delta"] = misclassification["predicted_to_real_delta"]
                _offsets["misclassification_start"] = misclassification["ds"]
                _offsets["chosen_reason"] = misclassification["reason"]
                _offsets["normalised_date"] = pd.to_datetime(misclassification["ds"]).normalize()

                # concat and join to main table.
                offsets = offsets.append(_offsets)  # type: ignore
            # Consider only weekly feedback # TODO: enable the other ones later.
            offsets = offsets.loc[offsets["chosen_reason"] == "expectedWeeklySeasonality"]

            # join the offsets to the main table on offset date
            df_feedback_processed["normalised_date_left"] = df_feedback_processed["ds"].dt.normalize()
            df_feedback_processed = df_feedback_processed.merge(
                offsets,
                how="left",
                left_on="normalised_date_left",
                right_on="exg_reg_date",
            )

            # drop columns we do not want anymore
            feedback_processor_params = self._params["feedback_processor_params"]["output_columns"]
            df_feedback_processed = df_feedback_processed[
                df_feedback_processed.columns[
                    df_feedback_processed.columns.isin(list(feedback_processor_params.keys()))
                ]
            ]
            # rename columns
            df_feedback_processed = df_feedback_processed.rename(columns=feedback_processor_params)
            has_exegonenous_regressor = True

            # fill nas with 0s? # TODO: We might want to revisit this if 0s mess the non
            # feedbacked data points because the model tries to learn too much from it
            df_feedback_processed.loc[df_feedback_processed["external_regressor"].isnull(), "external_regressor"] = 0
        return has_exegonenous_regressor, df_feedback_processed
