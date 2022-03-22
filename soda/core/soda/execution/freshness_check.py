from datetime import datetime, timezone
from typing import Dict, Optional

from soda.execution.check import Check
from soda.execution.check_outcome import CheckOutcome
from soda.execution.metric import Metric
from soda.execution.numeric_query_metric import NumericQueryMetric

MAX_COLUMN_TIMESTAMP = "max_column_timestamp"


class FreshnessCheck(Check):
    def __init__(
        self,
        check_cfg: "FreshnessCheckCfg",
        data_source_scan: "DataSourceScan",
        partition: "Partition",
        column: "Column",
    ):
        super().__init__(
            check_cfg=check_cfg,
            data_source_scan=data_source_scan,
            partition=partition,
            column=column,
            name="Freshness",
            identity_parts=check_cfg.get_identity_parts(),
        )
        self.freshness_values: Optional[dict] = None
        self.metrics[MAX_COLUMN_TIMESTAMP] = data_source_scan.resolve_metric(
            NumericQueryMetric(
                data_source_scan=self.data_source_scan,
                partition=self.partition,
                column=self.column,
                metric_name="max",
                metric_args=None,
                filter=None,
                aggregation=None,
                check_missing_and_valid_cfg=None,
                column_configurations_cfg=None,
                check=self,
            )
        )

    def evaluate(self, metrics: Dict[str, Metric], historic_values: Dict[str, object]):
        from soda.sodacl.freshness_check_cfg import FreshnessCheckCfg

        check_cfg: FreshnessCheckCfg = self.check_cfg

        now_variable_name = check_cfg.variable_name
        max_column_timestamp: Optional[datetime] = metrics.get(MAX_COLUMN_TIMESTAMP).value
        now_variable_timestamp: Optional[datetime] = None

        is_max_column_timestamp_valid = isinstance(max_column_timestamp, datetime)
        if not is_max_column_timestamp_valid and max_column_timestamp is not None:
            self.logs.error(
                f"Could not evaluate freshness: max({self.check_cfg.column_name}) "
                f"is not a datetime: {type(max_column_timestamp).__name__}",
                location=self.check_cfg.location,
            )

        now_variable_timestamp_text = self.data_source_scan.scan.get_variable(now_variable_name)
        if now_variable_timestamp_text is not None:
            try:
                now_variable_timestamp = datetime.fromisoformat(now_variable_timestamp_text)
            except:
                self.logs.error(
                    f"Could not parse variable {now_variable_name} as a timestamp: {now_variable_timestamp_text}",
                    location=check_cfg.location,
                )
        else:
            now_variable_timestamp = self.data_source_scan.scan._data_timestamp
            now_variable_name = "scan._scan_time"
            now_variable_timestamp_text = str(now_variable_timestamp)

        is_now_variable_timestamp_valid = isinstance(now_variable_timestamp, datetime)

        freshness = None
        if not is_now_variable_timestamp_valid:
            self.logs.error(
                f"Could not evaluate freshness: variable {check_cfg.variable_name} is not a datetime: {now_variable_timestamp_text}",
                location=self.check_cfg.location,
            )

        max_column_timestamp_utc = (
            self._datetime_to_utc(max_column_timestamp) if isinstance(max_column_timestamp, datetime) else None
        )
        now_timestamp_utc = (
            self._datetime_to_utc(now_variable_timestamp) if isinstance(now_variable_timestamp, datetime) else None
        )

        if is_max_column_timestamp_valid and is_now_variable_timestamp_valid:
            freshness = now_timestamp_utc - max_column_timestamp_utc
            if check_cfg.fail_staleness_threshold is not None and freshness > check_cfg.fail_staleness_threshold:
                self.outcome = CheckOutcome.FAIL
            elif check_cfg.warn_staleness_threshold is not None and freshness > check_cfg.warn_staleness_threshold:
                self.outcome = CheckOutcome.WARN
            else:
                self.outcome = CheckOutcome.PASS
        elif max_column_timestamp is None:
            self.outcome = CheckOutcome.FAIL

        self.freshness_values = {
            "max_column_timestamp": str(max_column_timestamp) if max_column_timestamp else None,
            "max_column_timestamp_utc": str(max_column_timestamp_utc) if max_column_timestamp_utc else None,
            "now_variable_name": now_variable_name,
            "now_timestamp": now_variable_timestamp_text,
            "now_timestamp_utc": str(now_timestamp_utc) if now_timestamp_utc else None,
            "freshness": freshness,
        }

    def get_cloud_diagnostics_dict(self):
        return {
            "value": self.freshness_values["freshness"].total_seconds() * 1000,  # millisecond difference
            "maxColumnTimestamp": self.freshness_values["max_column_timestamp"],
            "maxColumnTimestampUtc": self.freshness_values["max_column_timestamp_utc"],
            "nowVariableName": self.freshness_values["now_variable_name"],
            "nowTimestamp": self.freshness_values["now_timestamp"],
            "nowTimestampUtc": self.freshness_values["now_timestamp_utc"],
            "freshness": self.freshness_values["freshness"],
        }

    def get_log_diagnostic_dict(self) -> dict:
        return self.freshness_values

    @staticmethod
    def _datetime_to_utc(input: datetime) -> datetime:
        if input.tzinfo is None:
            return input.replace(tzinfo=timezone.utc)
        return input.astimezone(timezone.utc)
