#  Copyright 2021 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from sodasql.scan.metric import Metric
from sodasql.scan.scan_yml_parser import COLUMN_KEY_VALID_FORMAT, KEY_COLUMNS, KEY_METRICS
from tests.common.sql_test_case import SqlTestCase


class TestNumberValidityFormats(SqlTestCase):

    def test_number_whole(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('1')",
             "('-2')",
             "('6.62607004')",
             "('Turn around, look at what you see... e-e-e, e-e-e, e-e-e...')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    COLUMN_KEY_VALID_FORMAT: 'number_whole'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 40.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 40.0)

    def test_number_decimal_point(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('3.14159265359')",
             "('1.61803398875')",
             "('If my calculations are correct, when this baby hits...')",
             "('88')",
             "('... miles per hour...')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_decimal_point'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 5)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 3)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 60.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 40.0)

    def test_number_decimal_comma(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('2,718')",
             "('6,0221515')",
             "('It must be admitted that very simple relations exist between...')",
             "('... the volumes of gaseous substances...')",
             "(null)"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_decimal_comma'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 40.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 40.0)

    def test_number_percentage(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('1%')",
             "('2.0%')",
             "('3,0%')",
             "('I am...')",
             "('10000000000 %')",
             "('... sure.')",
             "('... It is a slow but steady effort.')",
             "('... I am going to beat fantasy with science!')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_percentage'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 50.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 4)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 50.0)

    def test_number_money_usd(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            [f"('10,000,000,000 USD')",
             f"('10,000,000,000USD')",
             f"('10,000,000,000 usd')",
             f"('$10,000,000,000')",
             f"('$ 10,000,000,000')",
             f"('52.5 USD')",
             f"('7,717.5 USD')",
             f"('-9.99 USD')",
             "('3 sacks of cow dung')",
             "('2 goats and my daughter')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_money_usd'
                }
            }
        })

        print([m.metric for m in scan_result.measurements])
        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 10)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 80.0)

    def test_number_money_eur(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            [f"('10 000 000 000 EUR')",
             f"('10 000 000 000EUR')",
             f"('10 000 000 000 eur')",
             f"('€10 000 000 000')",
             f"('€ 10 000 000 000')",
             f"('52,5 EUR')",
             f"('7 717,5 EUR')",
             f"('-9,99 EUR')",
             "('1000 Nis')",
             "('10 shrubberies')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_money_eur'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 10)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 80.0)

    def test_number_money_gbp(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            [f"('10,000,000,000 GBP')",
             f"('10,000,000,000GBP')",
             f"('10,000,000,000 gbp')",
             f"('£10,000,000,000')",
             f"('£ 10,000,000,000')",
             f"('52.5 GBP')",
             f"('7,717.5 GBP')",
             f"('-9.99 GBP')",
             "('1 hamster and 1 basket of elderberries')",
             "('12 million Twitter followers')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_money_gbp'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 10)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 80.0)

    def test_number_money_rmb(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            [f"('10,000,000,000 RMB')",
             f"('10,000,000,000RMB')",
             f"('10,000,000,000 rmb')",
             f"('¥10,000,000,000')",
             f"('¥ 10,000,000,000')",
             f"('52.5 RMB')",
             f"('7,717.5 RMB')",
             f"('-9.99 RMB')",
             "('10 hugs')",
             "('1.67 x 10^21 water molecules')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_money_rmb'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 10)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 80.0)

    def test_number_money_chf(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            [self.qualify_string(f"('10''000''000''000 CHF')"),
             self.qualify_string(f"('10''000''000''000CHF')"),
             self.qualify_string(f"('10''000''000''000 chf')"),
             self.qualify_string(f"('CHf10''000''000''000')"),
             self.qualify_string(f"('CHf 10''000''000''000')"),
             self.qualify_string(f"('52.5 CHF')"),
             self.qualify_string(f"('7''717.5 CHF')"),
             self.qualify_string(f"('-9.99 CHF')"),
             "('1 Chuck Norris roundhouse kick')",
             "('16 candles')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_money_chf'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 10)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 20.0)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 8)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 80.0)

    def test_number_money(self):
        self.sql_recreate_table(
            [f"name {self.dialect.data_type_varchar_255}"],
            ["('10,000,000,000 $')",
             f"('52,5 €')",
             f"('7,717.5 £')",
             f"('-9.99 ¥')",
             "('$ 10,000,000,000')",
             f"('€ 52,5')",
             f"('£ 7,717.5')",
             f"('¥ -9.99')",
             "('10,000,000,000 USD')",
             f"('52,5 eur')",
             f"('7,717.5 gbp')",
             f"('-9.99 rmb')",
             self.qualify_string(f"('99''99.99 CHF')"),
             f"('-99.99 CHF')",
             "('2 garbage bags full of old hair and nails')",
             "('7 samurais')"])

        scan_result = self.scan({
            KEY_METRICS: [
                Metric.INVALID_COUNT,
                Metric.INVALID_PERCENTAGE,
                Metric.VALID_COUNT,
                Metric.VALID_PERCENTAGE,
            ],
            KEY_COLUMNS: {
                'name': {
                    'valid_format': 'number_money'
                }
            }
        })

        self.assertEqual(scan_result.get(Metric.VALUES_COUNT, 'name'), 16)
        self.assertEqual(scan_result.get(Metric.INVALID_COUNT, 'name'), 2)
        self.assertEqual(scan_result.get(Metric.INVALID_PERCENTAGE, 'name'), 12.5)
        self.assertEqual(scan_result.get(Metric.VALID_COUNT, 'name'), 14)
        self.assertEqual(scan_result.get(Metric.VALID_PERCENTAGE, 'name'), 87.5)
