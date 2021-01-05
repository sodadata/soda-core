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
from datetime import timedelta, datetime

from sodasql.scan.scan import Scan
from sodasql.scan.scan_configuration_parser import ScanConfigurationParser
from sodasql.scan.warehouse import Warehouse
from tests.common.sql_test_case import SqlTestCase

scan_configuration_dict = {
    'table_name': 'demodata',
    'timeslice_filter': "date = DATE '{{ date }}'",
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

scan_parse = ScanConfigurationParser(scan_dict=scan_configuration_dict)
scan_parse.parse_logs.assert_no_warnings_or_errors()

profile_parse = SqlTestCase.create_dialect('postgres')
warehouse = Warehouse(profile_parse.warehouse_configuration)

row = warehouse.sql_fetchone(
    'SELECT MIN(date), MAX(date) FROM demodata'
)
min_date = row[0]
max_date = row[1]

scan_results = []

date = min_date
while date != max_date:
    timeslice = datetime(year=date.year, month=date.month, day=date.day).isoformat()
    timeslice_variables = {'date': date.strftime("%Y-%m-%d")}
    scan = Scan(warehouse=warehouse,
                scan_configuration=scan_parse.scan_configuration,
                timeslice_variables=timeslice_variables,
                timeslice=timeslice)
    scan_results.append(scan.execute())
    date = date + timedelta(days=1)

print()
print('Summary:')
for scan_result in scan_results:
    print(f'Results for scan {scan_result.timeslice}:')
    print(f'  Measurements: {len(scan_result.measurements)}')
    print(f'  Test results: {len(scan_result.test_results)} of which {scan_result.failures_count()} failed')
