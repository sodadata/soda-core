#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from typing import List, Optional, Set

from jinja2 import Template

from sodasql.scan.measurement import Measurement
from sodasql.scan.metric import Metric
from sodasql.scan.samples_yml import SamplesYml
from sodasql.scan.scan_yml_column import ScanYmlColumn
from sodasql.scan.sql_metric_yml import SqlMetricYml
from sodasql.scan.test import Test


class ScanYml:
    table_name: str = None
    metrics: Set[str] = None
    sql_metric_ymls: List[SqlMetricYml] = None
    tests: List[Test] = None
    # maps column_name.lower() to ScanYamlColumn's
    columns: dict = None
    excluded_columns: list = None
    filter: str = None
    filter_template: Template = None
    sample_percentage: float = None
    sample_method: str = None
    mins_maxs_limit: int = None
    frequent_values_limit: int = None
    # None means no samples to be taken
    samples_yml: SamplesYml = None

    def is_any_metric_enabled(self, metrics: List[str], column_name: Optional[str] = None):
        for metric in self.__get_metrics(column_name):
            if metric in metrics:
                return True
        return False

    def is_metric_enabled(self, metric: str, column_name: Optional[str] = None):
        return metric in self.__get_metrics(column_name)

    def __get_metrics(self, column_name: Optional[str] = None) -> Set[str]:
        metrics = self.metrics.copy()
        if column_name:
            column_configuration = self.columns.get(column_name.lower())
            if column_configuration is not None and column_configuration.metrics is not None:
                metrics.update(column_configuration.metrics)
        return metrics

    def get_missing(self, column_name: str):
        scan_yml_column = self.columns.get(column_name.lower())
        return scan_yml_column.missing if scan_yml_column else None

    def get_validity(self, column_name: str):
        scan_yml_column = self.columns.get(column_name.lower())
        return scan_yml_column.validity if scan_yml_column else None

    def get_validity_format(self, column):
        scan_yml_column = self.columns.get(column.name.lower())
        if scan_yml_column \
            and scan_yml_column.validity \
            and scan_yml_column.validity.format:
            return scan_yml_column.validity.format

    def get_mins_maxs_limit(self, column_name):
        return self.mins_maxs_limit

    def get_frequent_values_limit(self, column_name):
        return self.frequent_values_limit

    def get_scan_yaml_column(self, column_name: str):
        return self.columns.get(column_name.lower())

    def get_sample_yml(self, measurement: Measurement) -> Optional[SamplesYml]:
        if (measurement.metric == Metric.ROW_COUNT
            and self.samples_yml is not None
            and self.samples_yml.is_table_enabled()):
            return self.samples_yml
        else:
            column_samples_yml = self.get_column_samples_yml(measurement.column_name)
            if column_samples_yml:
                if (measurement.metric in [Metric.MISSING_COUNT, Metric.INVALID_COUNT]
                    and column_samples_yml.is_failed_enabled()):
                    return column_samples_yml
                elif (measurement.metric in [Metric.VALUES_COUNT, Metric.VALID_COUNT]
                      and column_samples_yml.is_passed_enabled()):
                    return column_samples_yml
        return None

    def get_column_samples_yml(self, column_name: str) -> Optional[SamplesYml]:
        if column_name and isinstance(column_name, str):
            scan_yml_column: ScanYmlColumn = self.columns.get(column_name.lower())
            if scan_yml_column and scan_yml_column.samples_yml:
                return scan_yml_column.samples_yml.with_defaults(self.samples_yml)
        return self.samples_yml

    def get_sql_metric_failed_rows_limit(self, sql_metric_yml: SqlMetricYml):
        if sql_metric_yml and isinstance(sql_metric_yml.failed_limit, int):
            return sql_metric_yml.failed_limit
        if self.samples_yml and isinstance(self.samples_yml.failed_limit, int):
            return self.samples_yml.failed_limit
        # default is 5
        return 5
