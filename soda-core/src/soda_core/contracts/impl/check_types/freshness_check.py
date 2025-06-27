from __future__ import annotations

import logging
from datetime import date, datetime, timedelta, timezone
from math import floor

from soda_core.common.datetime_conversions import convert_str_to_datetime
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import Check, CheckOutcome, CheckResult
from soda_core.contracts.impl.check_types.freshness_check_yaml import FreshnessCheckYaml
from soda_core.contracts.impl.check_types.row_count_check import RowCountMetricImpl
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    MeasurementValues,
    MetricImpl,
    ThresholdImpl,
)

logger: logging.Logger = soda_logger


class FreshnessCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["freshness"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: FreshnessCheckYaml,
    ) -> Optional[CheckImpl]:
        return FreshnessCheckImpl(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )


class FreshnessCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        column_impl: ColumnImpl,
        check_yaml: FreshnessCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=column_impl,
            check_yaml=check_yaml,
        )
        self.threshold = ThresholdImpl.create(
            threshold_yaml=check_yaml.threshold,
        )

        self.column = check_yaml.column
        self.now_variable: Optional[str] = check_yaml.now_variable
        self.unit: str = check_yaml.unit if check_yaml.unit else "hour"
        self.resolved_variable_values = contract_impl.contract_yaml.resolved_variable_values
        self.soda_variable_values = (
            contract_impl.contract_yaml.contract_yaml_source.resolve_on_read_soda_variable_values
        )

        self.max_timestamp_metric = self._resolve_metric(
            MaxTimestampMetricImpl(
                contract_impl=contract_impl,
                check_impl=self,
                column=self.column,
                now_variable=self.now_variable,
                unit=self.unit,
            )
        )
        self.row_count_metric_impl: MetricImpl = self._resolve_metric(
            RowCountMetricImpl(contract_impl=contract_impl, check_impl=self)
        )

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        max_timestamp: Optional[datetime] = self._get_max_timestamp(measurement_values)
        max_timestamp_utc: Optional[datetime] = self._get_max_timestamp_utc(max_timestamp)
        data_timestamp: datetime = self._get_now_timestamp()
        data_timestamp_utc: datetime = self._get_now_timestamp_utc(data_timestamp)
        diagnostic_metric_values: dict[str, float] = {"dataset_rows_tested": self.contract_impl.dataset_rows_tested}
        freshness: Optional[timedelta] = None
        freshness_in_seconds: Optional[int] = None
        threshold_metric_name: str = f"freshness_in_{self.unit}s"

        threshold_value: Optional[float] = None
        if max_timestamp_utc is None:
            outcome = CheckOutcome.FAILED

        elif data_timestamp_utc is not None:
            logger.debug(
                f"Calculating freshness using '{max_timestamp}' as 'max' and '{data_timestamp}' as 'now' values"
            )
            freshness = data_timestamp_utc - max_timestamp_utc
            freshness_in_seconds = floor(freshness.total_seconds())

            if self.unit == "minute":
                threshold_value = freshness_in_seconds / 60
            elif self.unit == "hour":
                threshold_value = freshness_in_seconds / (60 * 60)
            elif self.unit == "day":
                threshold_value = freshness_in_seconds / (60 * 60 * 24)

            if threshold_value is not None:
                diagnostic_metric_values[threshold_metric_name] = threshold_value

            if self.threshold:
                if self.threshold.passes(threshold_value):
                    outcome = CheckOutcome.PASSED
                else:
                    outcome = CheckOutcome.FAILED

        freshness_str: Optional[str] = str(freshness) if freshness is not None else None

        return FreshnessCheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            threshold_metric_name=threshold_metric_name,
            diagnostic_metric_values=diagnostic_metric_values,
            max_timestamp=max_timestamp,
            max_timestamp_utc=max_timestamp_utc,
            data_timestamp=data_timestamp,
            data_timestamp_utc=data_timestamp_utc,
            freshness=freshness_str,
            freshness_in_seconds=freshness_in_seconds,
            unit=self.unit,
        )

    def _get_max_timestamp(self, measurement_values: MeasurementValues) -> Optional[datetime]:
        max_timestamp: Optional[datetime] = measurement_values.get_value(self.max_timestamp_metric)
        if max_timestamp is None:
            logger.warning(
                f"Freshness metric '{self.max_timestamp_metric.type}' for column '{self.column}' "
                f"returned no value. Does the table or partition have rows?"
            )
            return None
        elif not isinstance(max_timestamp, datetime):
            logger.debug(
                f"Attempting to convert freshness value '{max_timestamp}' of data type '{type(max_timestamp).__name__}' to datetime"
            )
            if isinstance(max_timestamp, date):
                max_timestamp = datetime.combine(max_timestamp, datetime.min.time())
            elif isinstance(max_timestamp, str):
                max_timestamp = convert_str_to_datetime(max_timestamp)

        if not isinstance(max_timestamp, datetime):
            logger.error(
                f"Freshness column '{self.column}' returned value '{max_timestamp}' of data type '{type(max_timestamp).__name__}' which is not a datetime or datetime-compatible type."
            )
            max_timestamp = None

        return max_timestamp

    def _get_max_timestamp_utc(self, max_timestamp: Optional[datetime]) -> Optional[datetime]:
        return self._datetime_to_utc(max_timestamp) if isinstance(max_timestamp, datetime) else None

    def _get_now_timestamp(self) -> Optional[datetime]:
        if self.now_variable is None:
            return self.contract_impl.contract_yaml.data_timestamp
        else:
            now_timestamp_str: str = self.resolved_variable_values.get(self.now_variable)
            if now_timestamp_str is None:
                logger.error(f"Freshness variable '{self.now_variable}' not available")
                return None
            if not isinstance(now_timestamp_str, str):
                logger.error(
                    f"Freshness variable '{self.now_variable}' has wrong " f"type: {type(now_timestamp_str).__name__}"
                )
                return None
            else:
                now_timestamp: Optional[datetime] = convert_str_to_datetime(now_timestamp_str)
                if not isinstance(now_timestamp, datetime):
                    logger.error(f"Freshness variable '{self.now_variable}' is not a timestamp: {now_timestamp_str}")
                return now_timestamp

    def _get_now_timestamp_utc(self, now_timestamp: Optional[datetime]) -> Optional[datetime]:
        return self._datetime_to_utc(now_timestamp) if now_timestamp else None

    @staticmethod
    def _datetime_to_utc(input: datetime) -> datetime:
        if input.tzinfo is None:
            return input.replace(tzinfo=timezone.utc)
        return input.astimezone(timezone.utc)


class MaxTimestampMetricImpl(AggregationMetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        check_impl: FreshnessCheckImpl,
        column: str,
        now_variable: Optional[str],
        unit: Optional[str],
    ):
        self.column: str = column
        self.now_variable: Optional[str] = now_variable
        self.unit: Optional[str] = unit
        super().__init__(
            contract_impl=contract_impl,
            metric_type=check_impl.type,
            check_filter=check_impl.check_yaml.filter,
        )

    def _get_id_properties(self) -> dict[str, any]:
        id_properties: dict[str, str] = super()._get_id_properties()
        id_properties["column"] = self.column
        id_properties["now_variable"] = self.now_variable
        id_properties["unit"] = self.unit
        return id_properties

    def sql_expression(self) -> SqlExpression:
        return MAX(self.column)

    def convert_db_value(self, value) -> any:
        return value


class FreshnessCheckResult(CheckResult):
    def __init__(
        self,
        check: Check,
        outcome: CheckOutcome,
        threshold_metric_name: str,
        diagnostic_metric_values: Optional[dict[str, float]],
        max_timestamp: Optional[datetime],
        max_timestamp_utc: Optional[datetime],
        data_timestamp: datetime,
        data_timestamp_utc: datetime,
        freshness: Optional[str],
        freshness_in_seconds: Optional[int],
        unit: Optional[str],
    ):
        super().__init__(
            check=check,
            outcome=outcome,
            threshold_metric_name=threshold_metric_name,
            diagnostic_metric_values=diagnostic_metric_values,
        )
        self.max_timestamp: Optional[datetime] = max_timestamp
        self.max_timestamp_utc: Optional[datetime] = max_timestamp_utc
        self.data_timestamp: datetime = data_timestamp
        self.data_timestamp_utc: datetime = data_timestamp_utc
        self.freshness: Optional[str] = freshness
        self.freshness_in_seconds: Optional[int] = freshness_in_seconds
        self.unit: Optional[str] = unit

    # def log_summary(self, logs: Logs) -> None:
    #     super().log_summary(logs)
    #
    #     if self.max_timestamp is None:
    #         logger.info("  Max timestamp has no value. Did the table or partition have rows?")
    #     elif isinstance(self.max_timestamp, datetime):
    #         logger.debug(f"  Max timestamp was {self.max_timestamp}")
    #     else:
    #         logger.error(
    #             f"  Invalid data type for max timestamp ({type(self.max_timestamp).__name__}). "
    #             f"Is the column a timestamp?"
    #         )
    #     if isinstance(self.max_timestamp_utc, datetime):
    #         logger.info(f"  Max timestamp in UTC was {self.max_timestamp_utc}")
    #     if isinstance(self.data_timestamp, datetime):
    #         logger.debug(f"  Data timestamp was {self.data_timestamp}")
    #     if isinstance(self.data_timestamp_utc, datetime):
    #         logger.info(f"  Data timestamp in UTC was {self.data_timestamp_utc}")
    #
    #     if isinstance(self.freshness, str):
    #         logger.debug(f"  Freshness was {self.freshness}")
    #     if isinstance(self.freshness, str):
    #         logger.debug(f"  Freshness in seconds was {self.freshness_in_seconds}")
    #     if isinstance(self.unit, str):
    #         logger.debug(f"  Unit was {self.unit}")
