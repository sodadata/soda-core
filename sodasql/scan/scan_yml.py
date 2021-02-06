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

from sodasql.scan.sql_metric_yml import SqlMetricYml
from sodasql.scan.test import Test


class ScanYml:

    table_name: str = None
    metrics: Set[str] = None
    sql_metric_ymls: List[SqlMetricYml] = None
    tests: List[Test] = None
    # maps column_name.lower() to ScanYamlColumn's
    columns: dict = None
    sample_percentage: float = None
    sample_method: str = None
    filter: str = None
    filter_template: Template = None
    mins_maxs_limit: int = None
    frequent_values_limit: int = None

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
        column_configuration = self.columns.get(column_name.lower())
        return column_configuration.missing if column_configuration else None

    def get_validity(self, column_name: str):
        column_configuration = self.columns.get(column_name.lower())
        return column_configuration.validity if column_configuration else None

    def get_validity_format(self, column):
        column_configuration = self.columns.get(column.name.lower())
        if column_configuration \
                and column_configuration.validity \
                and column_configuration.validity.format:
            return column_configuration.validity.format

    def get_mins_maxs_limit(self, column_name):
        return self.mins_maxs_limit

    def get_frequent_values_limit(self, column_name):
        return self.mins_maxs_limit

    def get_scan_yaml_column(self, column_name: str):
        return self.columns.get(column_name.lower())
