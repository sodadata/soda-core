"""Handles user feedback consumption."""

# extract the regularity
# generate a "fake" custom regressor which:
#   # captures the delta between the predicted and actual value,
#   # reproduces is forward at captured regularity with some smoothing/decay around the point

from datetime import date
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from soda.common.logs import Logs
from soda.scientific.anomaly_detection_v2.globals import (
    EXTERNAL_REGRESSOR_COLUMNS,
    FEEDBACK_REASONS,
)


class FeedbackProcessor:
    """Processes user feedback."""

    def __init__(self, params: Dict[str, Any], df_historic: pd.DataFrame, logs: Logs):
        """Constructor for FeedbackProcessor."""
        self._logs = logs
        self._params = params
        self.df_historic = df_historic

    def get_processed_feedback_df(self) -> Tuple[bool, pd.DataFrame]:
        has_feedback = self.check_feedback()
        has_external_regressor = False
        if has_feedback:
            df_feedback_processed: pd.DataFrame = self.process_feedback()
            has_external_regressor, df_feedback_processed = self.derive_external_regressor(
                df_feedback_processed=df_feedback_processed
            )
        else:
            df_feedback_processed = self.df_historic.copy()
        return has_external_regressor, df_feedback_processed

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

    def derive_external_regressor(self, df_feedback_processed: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
        df_feedback_processed = df_feedback_processed.copy()
        df_misclassifications = self.get_misclassified_anomalies_df(df_feedback_processed=df_feedback_processed)
        if df_misclassifications.empty:
            has_external_regressor = False
            return has_external_regressor, df_feedback_processed

        today = date.today()
        df_weekly_offsets = self.handle_weekly_seasonality_offsets(
            df_misclassifications=df_misclassifications, today=today
        )

        df_monthly_offsets = self.handle_monthly_seasonality_offsets(
            df_misclassifications=df_misclassifications, today=today
        )

        df_yearly_offsets = self.handle_yearly_seasonality_offsets(
            df_misclassifications=df_misclassifications, today=today
        )

        df_feedback_processed = self.join_external_regressor_offsets(
            df_feedback_processed=df_feedback_processed,
            df_weekly_offsets=df_weekly_offsets,
            df_monthly_offsets=df_monthly_offsets,
            df_yearly_offsets=df_yearly_offsets,
        )
        has_external_regressor = True

        return has_external_regressor, df_feedback_processed

    def join_external_regressor_offsets(
        self,
        df_feedback_processed: pd.DataFrame,
        df_weekly_offsets: pd.DataFrame,
        df_monthly_offsets: pd.DataFrame,
        df_yearly_offsets: pd.DataFrame,
    ) -> pd.DataFrame:
        df_feedback_processed = df_feedback_processed.copy().reset_index(drop=True)
        df_feedback_processed["normalised_date"] = df_feedback_processed["ds"].dt.normalize()

        if not df_weekly_offsets.empty:
            df_feedback_processed = df_feedback_processed.merge(
                df_weekly_offsets, how="left", left_on="normalised_date", right_on="external_regressor_date"
            )

        if not df_monthly_offsets.empty:
            df_feedback_processed = df_feedback_processed.merge(
                df_monthly_offsets, how="left", left_on="normalised_date", right_on="external_regressor_date"
            )

        if not df_yearly_offsets.empty:
            df_feedback_processed = df_feedback_processed.merge(
                df_yearly_offsets, how="left", left_on="normalised_date", right_on="external_regressor_date"
            )
        available_regressor_columns = [
            col for col in df_feedback_processed.columns if col in EXTERNAL_REGRESSOR_COLUMNS
        ]
        df_feedback_processed[available_regressor_columns] = df_feedback_processed[available_regressor_columns].fillna(
            0
        )
        return df_feedback_processed

    def get_misclassified_anomalies_df(self, df_feedback_processed: pd.DataFrame) -> pd.DataFrame:
        allowed_seasonality_reasons = list(FEEDBACK_REASONS.keys())
        df_misclassifications = df_feedback_processed.loc[
            df_feedback_processed["reason"].isin(allowed_seasonality_reasons)
        ]
        df_misclassifications = df_misclassifications.loc[
            df_misclassifications["is_correctly_classified_anomaly"] == False
        ]

        df_misclassifications_mapping = pd.DataFrame.from_dict(FEEDBACK_REASONS, orient="index").reset_index()
        df_misclassifications = df_misclassifications.merge(
            df_misclassifications_mapping, how="left", left_on="reason", right_on="index"
        )
        df_misclassifications["ds"] = df_misclassifications["ds"].dt.tz_localize(None)
        return df_misclassifications

    def handle_weekly_seasonality_offsets(self, df_misclassifications: pd.DataFrame, today: date) -> pd.DataFrame:
        df_weekly_seasonality = (
            df_misclassifications.loc[df_misclassifications["reason"] == "expectedWeeklySeasonality"]
            .copy()
            .reset_index(drop=True)
        )

        if df_weekly_seasonality.empty:
            return pd.DataFrame()

        df_weekly_seasonality["day_of_week"] = df_weekly_seasonality["ds"].dt.day_name().str[:3].str.upper()
        offsets = pd.Series(dtype="datetime64[ns]")
        for _, row in df_weekly_seasonality.iterrows():
            offsets_for_single_misclassification = pd.date_range(
                row["ds"],
                today,
                freq="W-" + str(row["day_of_week"]),
                normalize=True,
            )
            offsets = pd.concat([offsets, pd.Series(offsets_for_single_misclassification)], ignore_index=True)  # type: ignore
        df_weekly_offsets = offsets.to_frame(name="external_regressor_date")
        df_weekly_offsets["external_regressor_date"] = df_weekly_offsets["external_regressor_date"].dt.normalize()
        df_weekly_offsets["external_regressor_weekly"] = 1
        return df_weekly_offsets

    def handle_monthly_seasonality_offsets(self, df_misclassifications: pd.DataFrame, today: date) -> pd.DataFrame:
        df_monthly_seasonality = (
            df_misclassifications.loc[df_misclassifications["reason"] == "expectedMonthlySeasonality"]
            .copy()
            .reset_index(drop=True)
        )

        if df_monthly_seasonality.empty:
            return pd.DataFrame()

        offsets = pd.Series(dtype="datetime64[ns]")
        for _, row in df_monthly_seasonality.iterrows():
            offsets_for_single_misclassification = pd.date_range(
                row["ds"], today, freq="MS", normalize=True, inclusive="left"
            )
            offset_days = row["ds"].day - 1
            offsets_for_single_misclassification = offsets_for_single_misclassification + pd.DateOffset(
                days=offset_days
            )  # type: ignore
            # Append row["ds"] to the list of offsets if it is not already present
            normalized_ds = row["ds"].normalize()
            if normalized_ds not in offsets_for_single_misclassification:
                offsets_for_single_misclassification = pd.DatetimeIndex([normalized_ds]).append(
                    offsets_for_single_misclassification
                )
            offsets = pd.concat([offsets, pd.Series(offsets_for_single_misclassification)], ignore_index=True)  # type: ignore
        df_monthly_offsets = offsets.to_frame(name="external_regressor_date")
        df_monthly_offsets["external_regressor_date"] = df_monthly_offsets["external_regressor_date"].dt.normalize()
        df_monthly_offsets["external_regressor_monthly"] = 1
        return df_monthly_offsets

    def handle_yearly_seasonality_offsets(self, df_misclassifications: pd.DataFrame, today: date) -> pd.DataFrame:
        df_yearly_seasonality = (
            df_misclassifications.loc[df_misclassifications["reason"] == "expectedYearlySeasonality"]
            .copy()
            .reset_index(drop=True)
        )

        if df_yearly_seasonality.empty:
            return pd.DataFrame()

        offsets = pd.Series(dtype="datetime64[ns]")
        for _, row in df_yearly_seasonality.iterrows():
            offsets_for_single_misclassification = [
                row["ds"] + pd.DateOffset(years=i) for i in range(today.year - row["ds"].year + 1)
            ]
            offsets = pd.concat([offsets, pd.Series(offsets_for_single_misclassification)], ignore_index=True)
        df_yearly_offsets = offsets.to_frame(name="external_regressor_date")
        df_yearly_offsets["external_regressor_date"] = df_yearly_offsets["external_regressor_date"].dt.normalize()
        df_yearly_offsets["external_regressor_yearly"] = 1
        return df_yearly_offsets
