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
from datetime import datetime

from sodasql.scan.scan_yml_parser import ScanYmlParser
from sodasql.soda_server_client.soda_server_client import SodaServerClient
from tests.common.sql_test_case import SqlTestCase


class TestSodaServerInteraction(SqlTestCase):

    def test_soda_server_client(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_integer}"],
            ["(1)",
             "(2)",
             "(3)",
             "(4)",
             "(null)"])

        scan_yml_dict = {
            'table_name': self.default_test_table_name,
            # 'samples': {
            #     'table_limit': 10,
            #     'failed_limit': 5
            # },
            'metric_groups': [
                'missing',
                'validity'
            ],
            'tests': [
                'row_count > 0'
            ],
            'columns': {
                'name': {
                    'valid_max': 2,
                    'tests': [
                        'missing_count < 1',
                    ]
                }
            },
            'sql_metrics': [{
                'name': 'big_names',
                'sql': f'SELECT * from {self.default_test_table_name} WHERE name > 3',
                'type': 'failed_rows'
            }]
        }

        scan_configuration_parser = ScanYmlParser(scan_yml_dict, 'test-scan')
        scan_configuration_parser.assert_no_warnings_or_errors()

        soda_server_client = SodaServerClient(
            host='localhost',
            port='5000',
            protocol='http',
            api_key_id='testapikeyid',
            api_key_secret='testapikeysecret'
        )

        scan = self.warehouse.create_scan(scan_yml=scan_configuration_parser.scan_yml,
                                          soda_server_client=soda_server_client,
                                          time=datetime.now().isoformat(timespec='seconds'))
        scan.close_warehouse = False
        scan_result = scan.execute()

        # if scan_result.has_failures():
        #     raise RuntimeError(f'Scan failed: {scan_result.get_failures_message()}')
