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
import os
from typing import Optional

import yaml

from sodasql.scan.configuration_helper import parse_int
from sodasql.scan.metric import resolve_metrics, remove_metric, Metric, ensure_metric
from sodasql.scan.parse_logs import ParseLogs
from sodasql.scan.scan_configuration import ScanConfiguration

KEY_TABLE_NAME = 'table_name'
KEY_METRICS = 'metrics'
KEY_COLUMNS = 'columns'
KEY_MINS_MAXS_LIMIT = 'mins_maxs_limit'
KEY_FREQUENT_VALUES_LIMIT = 'frequent_values_limit'
KEY_SAMPLE_PERCENTAGE = 'sample_percentage'
KEY_SAMPLE_METHOD = 'sample_method'


class ScanParse:

    VALID_KEYS = [KEY_TABLE_NAME, KEY_METRICS, KEY_COLUMNS,
                  KEY_MINS_MAXS_LIMIT, KEY_FREQUENT_VALUES_LIMIT,
                  KEY_SAMPLE_PERCENTAGE, KEY_SAMPLE_METHOD]

    def __init__(self,
                 project_dir: Optional[str] = None,
                 profile: Optional[str] = None,
                 table: Optional[str] = None,
                 scan_yml_path: str = None,
                 scan_yaml_str: Optional[str] = None,
                 scan_dict: Optional[dict] = None):
        self.parse_logs: ParseLogs = ParseLogs('Scan')
        self.scan_configuration = ScanConfiguration()

        scan_dict = self._get_scan_dict(project_dir, profile, table, scan_yml_path, scan_yaml_str, scan_dict)

        self.scan_configuration.table_name = scan_dict.get(KEY_TABLE_NAME)
        if not self.scan_configuration.table_name:
            self.parse_logs.error('table_name is required')

        self.scan_configuration.metrics = resolve_metrics(scan_dict.get(KEY_METRICS, []), self.parse_logs)

        self.scan_configuration.columns = {}
        columns_dict = scan_dict.get(KEY_COLUMNS, {})
        for column_name in columns_dict:
            column_dict = columns_dict[column_name]
            column_name_lower = column_name.lower()
            from sodasql.scan.scan_column_configuration import ScanColumnConfiguration
            column_configuration = ScanColumnConfiguration(self.parse_logs, column_name, column_dict)
            self.scan_configuration.columns[column_name_lower] = column_configuration
            if remove_metric(column_configuration.metrics, Metric.ROW_COUNT):
                ensure_metric(self.scan_configuration.metrics, Metric.ROW_COUNT, f'{Metric.ROW_COUNT} {column_name}', self.parse_logs)

        self.scan_configuration.sample_percentage = \
            parse_int(scan_dict, KEY_SAMPLE_PERCENTAGE, self.parse_logs, 'scan configuration')
        self.scan_configuration.sample_method = scan_dict.get(KEY_SAMPLE_METHOD, 'SYSTEM').upper()
        self.scan_configuration.mins_maxs_limit = \
            parse_int(scan_dict, KEY_MINS_MAXS_LIMIT, self.parse_logs, 'scan configuration', 20)
        self.scan_configuration.frequent_values_limit = \
            parse_int(scan_dict, KEY_FREQUENT_VALUES_LIMIT, self.parse_logs, 'scan configuration', 20)

        self.parse_logs.warning_invalid_elements(
            scan_dict.keys(),
            ScanParse.VALID_KEYS,
            'Invalid scan configuration')

    def _get_scan_dict(self, project_dir, profile, table, scan_yml_path, scan_yaml_str, scan_dict):
        if scan_dict is None:
            if scan_yaml_str is None:
                if scan_yml_path is None:
                    if (isinstance(project_dir, str) and isinstance(profile, str) and isinstance(table, str)):
                        scan_yml_path = os.path.join(project_dir, profile, table, 'scan.yml')
                    else:
                        self.parse_logs.error('No scan configured')

                if isinstance(scan_yml_path, str):
                    try:
                        with open(scan_yml_path) as f:
                            scan_yml_str = f.read()
                    except Exception as e:
                        self.parse_logs.error(f"Couldn't read scan yaml {scan_yml_path}: {str(e)}")
                else:
                    self.parse_logs.error(f"scan_yml_path is not a str {str(scan_yml_path)}")

            if isinstance(scan_yml_str, str):
                try:
                    scan_dict = yaml.load(scan_yaml_str, Loader=yaml.FullLoader)
                except Exception as e:
                    self.parse_logs.error(f"Couldn't parse scan yaml string: {str(e)}")
            else:
                self.parse_logs.error(f"scan_yml_str is not a str {str(scan_yml_str)}")

        return scan_dict
