from __future__ import annotations

import logging
from datetime import timedelta, timezone

from soda_core.common.datetime_conversions import convert_str_to_datetime
from soda_core.common.logging_constants import soda_logger
from soda_core.common.logs import Logs
from soda_core.common.sql_dialect import *
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Contract,
)
from soda_core.contracts.impl.check_types.freshness_check_yaml import FreshnessCheckYaml
from soda_core.contracts.impl.contract_verification_impl import (
    AggregationMetricImpl,
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    MeasurementValues,
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
        self.data_time_variable: str = (
            check_yaml.data_time_variable if isinstance(check_yaml.data_time_variable, str) else "DATA_TIME"
        )
        self.unit: str = check_yaml.unit if check_yaml.unit else "hour"
        self.resolved_variable_values = contract_impl.contract_yaml.resolved_variable_values

        self.max_timestamp_metric = self._resolve_metric(
            MaxTimestampMetricImpl(
                contract_impl=contract_impl,
                check_impl=self,
                column=self.column,
                data_time_variable=self.data_time_variable,
                unit=self.unit,
            )
        )

    def evaluate(self, measurement_values: MeasurementValues, contract: Contract) -> CheckResult:
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        max_timestamp: Optional[datetime] = self._get_max_timestamp(measurement_values)
        max_timestamp_utc: Optional[datetime] = self._get_max_timestamp_utc(max_timestamp)
        data_time: datetime = self._get_data_time()
        data_time_utc: datetime = self._get_data_time_utc(data_time)
        diagnostic_metric_values: dict[str, float] = {}
        freshness: Optional[timedelta] = None
        freshness_in_seconds: Optional[float] = None
        threshold_metric_name: str = f"freshness_in_{self.unit}s"

        threshold_value: Optional[float] = None
        if data_time_utc and max_timestamp_utc:
            freshness = data_time_utc - max_timestamp_utc
            freshness_in_seconds = freshness.total_seconds()

            if self.unit == "minute":
                threshold_value = freshness_in_seconds / 60
            elif self.unit == "hour":
                threshold_value = freshness_in_seconds / (60 * 60)
            elif self.unit == "day":
                threshold_value = freshness_in_seconds / (60 * 60 * 24)

            if threshold_value is not None:
                diagnostic_metric_values = {threshold_metric_name: threshold_value}

            if self.threshold:
                if self.threshold.passes(threshold_value):
                    outcome = CheckOutcome.PASSED
                else:
                    outcome = CheckOutcome.FAILED

        return FreshnessCheckResult(
            contract=contract,
            check=self._build_check_info(),
            outcome=outcome,
            threshold_metric_name=threshold_metric_name,
            diagnostic_metric_values=diagnostic_metric_values,
            max_time=max_timestamp,
            max_time_utc=max_timestamp_utc,
            data_time=data_time,
            data_time_utc=data_time_utc,
            freshness=str(freshness),
            freshness_in_seconds=freshness_in_seconds,
            unit=self.unit,
        )

    def _get_max_timestamp(self, measurement_values: MeasurementValues) -> Optional[datetime]:
        max_timestamp: Optional[datetime] = measurement_values.get_value(self.max_timestamp_metric)
        if not isinstance(max_timestamp, datetime):
            logger.error(f"Freshness column '{self.column}' does not have timestamp values: {max_timestamp}")
        return max_timestamp

    def _get_max_timestamp_utc(self, max_timestamp: Optional[datetime]) -> Optional[datetime]:
        return self._datetime_to_utc(max_timestamp) if isinstance(max_timestamp, datetime) else None

    def _get_data_time(self) -> Optional[datetime]:
        data_time_value: str = self.resolved_variable_values.get(self.data_time_variable)

        if data_time_value is None:
            logger.error(f"Freshness variable '{self.data_time_variable}' not available")
        elif not isinstance(data_time_value, str):
            actual_type: str = type(data_time_value).__name__
            logger.error(f"Freshness variable '{self.data_time_variable}' is not a string available: `{actual_type}`")
        else:
            now_timestamp: Optional[datetime] = convert_str_to_datetime(data_time_value)
            if not isinstance(now_timestamp, datetime):
                logger.error(f"Freshness variable '{self.data_time_variable}' is not a timestamp: {data_time_value}")
            return now_timestamp

    def _get_data_time_utc(self, now_timestamp: Optional[datetime]) -> Optional[datetime]:
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
        data_time_variable: Optional[str],
        unit: Optional[str],
    ):
        self.column: str = column
        self.now_variable: Optional[str] = data_time_variable
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
        contract: Contract,
        check: Check,
        outcome: CheckOutcome,
        threshold_metric_name: str,
        diagnostic_metric_values: Optional[dict[str, float]],
        max_time: Optional[datetime],
        max_time_utc: Optional[datetime],
        data_time: datetime,
        data_time_utc: datetime,
        freshness: Optional[str],
        freshness_in_seconds: Optional[int],
        unit: Optional[str],
    ):
        super().__init__(
            contract=contract,
            check=check,
            outcome=outcome,
            threshold_metric_name=threshold_metric_name,
            diagnostic_metric_values=diagnostic_metric_values,
        )
        self.max_time: Optional[datetime] = max_time
        self.max_time_utc: Optional[datetime] = max_time_utc
        self.data_time: datetime = data_time
        self.data_time_utc: datetime = data_time_utc
        self.freshness: Optional[str] = freshness
        self.freshness_in_seconds: Optional[int] = freshness_in_seconds
        self.unit: Optional[str] = unit

    def log_summary(self, logs: Logs) -> None:
        super().log_summary(logs)

        if self.max_time is None:
            logger.info("  Max time has no value. Did the table or partition have rows?")
        elif isinstance(self.max_time, datetime):
            logger.debug(f"  Max time was {self.max_time}")
        else:
            logger.error(
                f"  Invalid data type for max time ({type(self.max_time).__name__}). " f"Is the column a timestamp?"
            )
        if isinstance(self.max_time_utc, datetime):
            logger.info(f"  Max time in UTC was {self.max_time_utc}")
        if isinstance(self.data_time, datetime):
            logger.debug(f"  Data time was {self.data_time}")
        if isinstance(self.data_time_utc, datetime):
            logger.info(f"  Data time in UTC was {self.data_time_utc}")

        if isinstance(self.freshness, str):
            logger.debug(f"  Freshness was {self.freshness}")
        if isinstance(self.freshness, str):
            logger.debug(f"  Freshness in seconds was {self.freshness_in_seconds}")
        if isinstance(self.unit, str):
            logger.debug(f"  Unit was {self.unit}")
