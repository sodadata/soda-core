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
import logging
from math import ceil

import yaml

from sodasql.cli.file_system import FileSystemSingleton
from sodasql.cli.indenting_yaml_dumper import IndentingDumper
from sodasql.scan.validity import Validity
from sodasql.scan.warehouse import Warehouse


class ScanInitializer:

    def __init__(self, warehouse: Warehouse, warehouse_dir: str):
        self.warehouse: Warehouse = warehouse
        self.warehouse_dir: str = warehouse_dir

    def initialize_scan_ymls(self, rows):
        file_system = FileSystemSingleton.INSTANCE

        for row in rows:
            table_name = row[0]
            table_dir = file_system.join(self.warehouse_dir, table_name)
            if not file_system.file_exists(table_dir):
                logging.info(f'Creating table directory {table_dir}')
                file_system.mkdirs(table_dir)
            else:
                logging.info(f'Directory {table_dir} already exists')

            table_scan_yaml_file = file_system.join(table_dir, 'scan.yml')

            if file_system.file_exists(table_scan_yaml_file):
                logging.info(
                    f"Scan file {table_scan_yaml_file} already exists")
            else:
                logging.info(f"Creating {table_scan_yaml_file} ...")
                from sodasql.scan.scan_yml_parser import (KEY_METRICS,
                                                          KEY_TABLE_NAME,
                                                          KEY_TESTS,
                                                          KEY_COLUMNS,
                                                          COLUMN_KEY_VALID_FORMAT,
                                                          COLUMN_KEY_TESTS)
                scan_yaml_dict = {
                    KEY_TABLE_NAME: table_name,
                    KEY_METRICS: [
                        # TODO replace with constants
                        'row_count',
                        'missing_count', 'missing_percentage', 'values_count', 'values_percentage',
                        'valid_count', 'valid_percentage', 'invalid_count', 'invalid_percentage',
                        'min', 'max', 'avg', 'sum', 'min_length', 'max_length', 'avg_length'
                    ],
                    KEY_TESTS: {
                        'row_count_min': 'row_count > 0'
                    }
                }

                dialect = self.warehouse.dialect
                sql = dialect.sql_columns_metadata_query(table_name)
                column_tuples = self.warehouse.sql_fetchall(sql)
                qualified_table_name = dialect.qualify_table_name(table_name)
                columns = {}
                for column_tuple in column_tuples:
                    column_name = column_tuple[0]
                    column_type = column_tuple[1]
                    qualified_column_name = dialect.qualify_column_name(column_name)
                    if dialect.is_text(column_type):
                        validity_format_count_fields = []
                        validity_counts = []
                        for validity_format in Validity.FORMATS:
                            format_regex = Validity.FORMATS[validity_format]
                            validity_counts.append({'format': validity_format})
                            qualified_regex = dialect.qualify_regex(format_regex)
                            regexp_like = dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex)
                            count_field = f'COUNT(CASE WHEN {regexp_like} THEN 1 END)'
                            validity_format_count_fields.append(count_field)

                        row = self.warehouse.sql_fetchone(
                            f'SELECT \n  ' +
                            (',\n  '.join(validity_format_count_fields)) + ',\n'
                            f'  COUNT({qualified_column_name}) \n'
                            f'FROM {qualified_table_name} \n'
                            f'LIMIT 1000'
                        )

                        values_count = row[len(validity_counts)]

                        if values_count > 0:
                            for i in range(len(validity_counts)):
                                validity_count = validity_counts[i]
                                validity_count['count'] = row[i]

                            sorted_validity_counts = sorted(validity_counts, key=lambda c: c['count'], reverse=True)
                            most_frequent_validity_format = sorted_validity_counts[0]
                            valid_count = most_frequent_validity_format['count']

                            if valid_count > (values_count / 2):
                                column_yml = {
                                    COLUMN_KEY_VALID_FORMAT: most_frequent_validity_format['format']
                                }
                                if valid_count > (values_count * .8):
                                    valid_percentage = valid_count * 100 / values_count
                                    invalid_threshold = (100 - valid_percentage) * 1.1
                                    invalid_threshold_rounded = ceil(invalid_threshold)
                                    column_yml[COLUMN_KEY_TESTS] = {
                                        'invalid_pct_max': f'invalid_percentage <= {str(invalid_threshold_rounded)}'
                                    }
                                columns[column_name] = column_yml

                if columns:
                    scan_yaml_dict[KEY_COLUMNS] = columns

                scan_yml_str = yaml.dump(scan_yaml_dict,
                                         sort_keys=False,
                                         Dumper=IndentingDumper,
                                         default_flow_style=False)
                file_system.file_write_from_str(table_scan_yaml_file, scan_yml_str)
