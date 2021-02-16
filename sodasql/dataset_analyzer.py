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
import re
from dataclasses import dataclass
from math import ceil
from typing import List

import yaml

from sodasql.scan.file_system import FileSystemSingleton
from sodasql.cli.indenting_yaml_dumper import IndentingDumper
from sodasql.scan.metric import Metric
from sodasql.scan.validity import Validity
from sodasql.scan.warehouse import Warehouse


@dataclass
class ColumnAnalysisResult:
    column_name: str
    source_type: str
    is_text: bool = False
    validity_format: str = None
    values_count: int = None
    valid_count: int = None

    def to_json(self):
        return {
            'columnName': self.column_name,
            'sourceType': self.source_type,
            'validityFormat': self.validity_format
        }

class DatasetAnalyzer:

    def analyze(self, warehouse: Warehouse, table_name: str):
        dialect = warehouse.dialect
        qualified_table_name = dialect.qualify_table_name(table_name)

        analyze_results: List[ColumnAnalysisResult] = []

        sql = dialect.sql_columns_metadata_query(table_name)
        column_tuples = warehouse.sql_fetchall(sql)
        for column_tuple in column_tuples:
            column_name = column_tuple[0]
            source_type = column_tuple[1]

            column_analysis_result = ColumnAnalysisResult(column_name=column_name, source_type=source_type)
            analyze_results.append(column_analysis_result)

            qualified_column_name = dialect.qualify_column_name(column_name)

            if dialect.is_text(source_type):
                column_analysis_result.is_text = True

                validity_format_count_fields = []
                validity_counts = []
                for validity_format in Validity.FORMATS:
                    format_regex = Validity.FORMATS[validity_format]
                    validity_counts.append({'format': validity_format})
                    qualified_regex = dialect.qualify_regex(format_regex)
                    regexp_like = dialect.sql_expr_regexp_like(qualified_column_name, qualified_regex)
                    count_field = f'COUNT(CASE WHEN {regexp_like} THEN 1 END)'
                    validity_format_count_fields.append(count_field)

                row = warehouse.sql_fetchone(
                    f'SELECT \n  ' +
                    (',\n  '.join(validity_format_count_fields)) + ',\n'
                    f'  COUNT({qualified_column_name}) \n'
                    f'FROM {qualified_table_name} \n'
                    f'LIMIT 1000'
                )

                values_count = row[len(validity_counts)]
                column_analysis_result.values_count = values_count

                if values_count > 0:
                    for i in range(len(validity_counts)):
                        validity_count = validity_counts[i]
                        validity_count['count'] = row[i]

                    sorted_validity_counts = sorted(validity_counts, key=lambda c: c['count'], reverse=True)
                    most_frequent_validity_format = sorted_validity_counts[0]
                    valid_count = most_frequent_validity_format['count']
                    column_analysis_result.valid_count = valid_count

                    if valid_count > (values_count / 2):
                        column_analysis_result.validity_format = most_frequent_validity_format['format']

        return analyze_results
