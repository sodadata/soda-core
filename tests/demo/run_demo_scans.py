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
from datetime import timedelta

from sodasql.scan.scan_parse import ScanParse
from sodasql.scan.warehouse import Warehouse
from tests.common.sql_test_case import SqlTestCase
from sodasql.scan.scan import Scan


scan_configuration_dict = {
    'table_name': 'demodata',
    'time_filter': "date = DATE '{{ date }}'",
    'metrics': [
        'missing',
        'validity',
        'min',
        'max',
        'avg',
        'sum',
        'min_length',
        'max_length',
        'avg_length'],
    'columns': {
        'ID': {
            'metrics': [
                'distinct',
                'uniqueness'],
            'tests': [
                'missing_percentage < 3.0',
                'invalid_count == 0']
        }
    }
}

scan_parse = ScanParse(scan_dict=scan_configuration_dict)
scan_parse.parse_logs.assert_no_warnings_or_errors()

profile_parse = SqlTestCase.parse_test_profile('postgres')
warehouse = Warehouse(profile_parse.warehouse_configuration)

row = warehouse.sql_fetchone(
    'SELECT MIN(date), MAX(date) FROM demodata'
)
min_date = row[0]
max_date = row[1]

date = min_date
while date != max_date:
    scan = Scan(warehouse, scan_parse.scan_configuration, variables={'date': date.strftime("%Y-%m-%d")})
    scan_result = scan.execute()
    for measurement in scan_result.measurements:
        print(measurement)
    date = date + timedelta(days=1)
