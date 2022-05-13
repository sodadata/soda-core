from numbers import Number
from typing import Dict, Optional

from soda.execution.check import Check
from soda.execution.check_outcome import CheckOutcome
from soda.execution.derived_metric import DERIVED_METRIC_NAMES
from soda.execution.metric import Metric
from soda.execution.user_defined_numeric_metric import UserDefinedNumericMetric
from soda.sampler.sample_ref import SampleRef
from soda.sodacl.metric_check_cfg import MetricCheckCfg

KEY_CHECK_VALUE = "check_value"


class MetricCheck(Check):
    def __init__(
        self,
        check_cfg: "MetricCheckCfg",
        data_source_scan: "DataSourceScan",
        partition: Optional["Partition"] = None,
        column: Optional["Column"] = None,
    ):
        metric_check_cfg: MetricCheckCfg = check_cfg
        metric_name = metric_check_cfg.metric_name

        super().__init__(
            check_cfg=check_cfg, data_source_scan=data_source_scan, partition=partition, column=column, name=metric_name
        )

        self.historic_descriptor = None
        self.metric_store_request = None

        self.check_value: Optional[float] = None
        self.formula_values: Optional[dict] = None
        self.historic_diff_values: Optional[dict] = None
        self.failed_rows_sample_ref: Optional[SampleRef] = None

        from soda.execution.derived_metric import DerivedMetric
        from soda.execution.numeric_query_metric import NumericQueryMetric

        is_user_defined_metric_query = metric_check_cfg.metric_query is not None
        if not is_user_defined_metric_query:
            is_derived = metric_name in DERIVED_METRIC_NAMES

            if not is_derived:
                metric = NumericQueryMetric(
                    data_source_scan=self.data_source_scan,
                    partition=self.partition,
                    column=self.column,
                    metric_name=metric_name,
                    metric_args=metric_check_cfg.metric_args,
                    filter=metric_check_cfg.filter,
                    aggregation=metric_check_cfg.metric_expression,
                    check_missing_and_valid_cfg=metric_check_cfg.missing_and_valid_cfg,
                    column_configurations_cfg=self.column.column_configurations_cfg if self.column else None,
                    check=self,
                )
            else:
                metric = DerivedMetric(
                    data_source_scan=self.data_source_scan,
                    partition=self.partition,
                    column=self.column,
                    metric_name=metric_name,
                    metric_args=metric_check_cfg.metric_args,
                    filter=metric_check_cfg.filter,
                    check_missing_and_valid_cfg=metric_check_cfg.missing_and_valid_cfg,
                    column_configurations_cfg=self.column.column_configurations_cfg if self.column else None,
                    check=self,
                )

            metric = data_source_scan.resolve_metric(metric)

        else:
            if data_source_scan.data_source.is_supported_metric_name(metric_name):
                self.logs.warning(
                    f"User defined metric {metric_name} is a data_source supported metric. Please, choose a different name for the metric.",
                    location=metric_check_cfg.location,
                )

            metric = UserDefinedNumericMetric(
                data_source_scan=self.data_source_scan,
                check_name=metric_check_cfg.source_line,
                sql=metric_check_cfg.metric_query,
                check=self,
            )

            metric = data_source_scan.resolve_metric(metric)

        self.metrics[metric_name] = metric

    def evaluate(self, metrics: Dict[str, Metric], historic_values: Dict[str, object]):
        self.check_value = self.get_metric_value()
        self.set_outcome_based_on_check_value()

    def get_metric_value(self) -> object:
        metric = self.get_metric()
        self.formula_values = metric.formula_values
        return metric.value

    def get_metric(self):
        metric_check_cfg: MetricCheckCfg = self.check_cfg
        metric_name = metric_check_cfg.metric_name
        metric = self.metrics.get(metric_name)
        return metric

    def set_outcome_based_on_check_value(self):
        metric_check_cfg: MetricCheckCfg = self.check_cfg
        if self.check_value is not None and metric_check_cfg.has_threshold():
            if isinstance(self.check_value, Number):
                if metric_check_cfg.fail_threshold_cfg and metric_check_cfg.fail_threshold_cfg.is_bad(self.check_value):
                    self.outcome = CheckOutcome.FAIL
                elif metric_check_cfg.warn_threshold_cfg and metric_check_cfg.warn_threshold_cfg.is_bad(
                    self.check_value
                ):
                    self.outcome = CheckOutcome.WARN
                else:
                    self.outcome = CheckOutcome.PASS
            else:
                hint = (
                    " Is your column text based? The valid format config only works on text columns."
                    if metric_check_cfg.missing_and_valid_cfg and metric_check_cfg.missing_and_valid_cfg.valid_format
                    else ""
                )
                self.logs.error(
                    f"Cannot evaluate check: Expected a numeric value, but was {self.check_value}.{hint}",
                    location=self.check_cfg.location,
                )

    def get_cloud_diagnostics_dict(self) -> dict:
        metric_check_cfg: MetricCheckCfg = self.check_cfg
        cloud_diagnostics = {"value": self.check_value}
        # TODO Disabled these until cloud can ingest them properly
        # if self.formula_values:
        #     cloud_diagnostics["formula_values"] = self.formula_values
        # if self.failed_rows_sample_ref:
        #     cloud_diagnostics["failed_rows_sample_ref"] = self.failed_rows_sample_ref.get_cloud_diagnostics_dict()
        if metric_check_cfg.fail_threshold_cfg is not None:
            cloud_diagnostics["fail"] = metric_check_cfg.fail_threshold_cfg.to_soda_cloud_diagnostics_json()
        if metric_check_cfg.warn_threshold_cfg is not None:
            cloud_diagnostics["warn"] = metric_check_cfg.warn_threshold_cfg.to_soda_cloud_diagnostics_json()

        metric = self.get_metric()
        if metric and metric.failed_rows_sample_ref:
            cloud_diagnostics["failedRowsFile"] = metric.failed_rows_sample_ref.get_cloud_diagnostics_dict()
        return cloud_diagnostics

    def get_log_diagnostic_dict(self) -> dict:
        log_diagnostics = {"check_value": self.check_value}
        if self.formula_values:
            log_diagnostics.update(self.formula_values)
        if self.failed_rows_sample_ref:
            log_diagnostics["failed_rows_sample_ref"] = str(self.failed_rows_sample_ref)
        return log_diagnostics
