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
from unittest import TestCase

from sodasql.scan.scan_builder import ScanBuilder
from sodasql.scan.scan_yml_parser import KEY_TABLE_NAME
from sodasql.scan.warehouse_yml_parser import KEY_NAME, KEY_SODA_ACCOUNT, \
    SODA_KEY_HOST, SODA_KEY_API_KEY_ID, SODA_KEY_API_KEY_SECRET, KEY_CONNECTION


class TestSodaServerClientConfiguration(TestCase):

    def test_soda_server_client_configuration(self):
        scan_builder = ScanBuilder()
        scan_builder.warehouse_yml_dict = {
            KEY_NAME: 'Test warehouse',
            KEY_CONNECTION: {
            },
            KEY_SODA_ACCOUNT: {
                SODA_KEY_HOST: 'mycloud.soda.io',
                SODA_KEY_API_KEY_ID: 'mykeyid',
                SODA_KEY_API_KEY_SECRET: 'mykeysecret'
            }
        }
        scan_builder.scan_yml_dict = {
            KEY_TABLE_NAME: 't'
        }

        scan_builder._build_warehouse_yml()
        scan_builder._create_soda_server_client()

        self.assertIsNotNone(scan_builder.soda_server_client)
        self.assertEqual(scan_builder.soda_server_client.host, 'mycloud.soda.io')
        self.assertEqual(scan_builder.soda_server_client.api_key_id, 'mykeyid')
        self.assertEqual(scan_builder.soda_server_client.api_key_secret, 'mykeysecret')
