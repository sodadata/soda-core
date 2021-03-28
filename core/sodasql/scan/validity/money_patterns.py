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
from typing import List


class MoneyPatternHelper:

    @staticmethod
    def currency_number_pattern(thousands_separator, decimal_separator):
        return rf"(\d+[{thousands_separator}])*(\d+)({decimal_separator}\d+)?"

    @staticmethod
    def currency_prefix_pattern(currency_symbol):
        return rf"({currency_symbol})? ?(\-)?"

    @staticmethod
    def currency_suffix_pattern(currency_symbol, currency_name):
        return rf" ?({currency_symbol}|{currency_name.lower()}|{currency_name.upper()})?"

    @classmethod
    def money_pattern(cls, thousands_separator, decimal_separator, currency_symbol, currency_name):
        return cls.currency_prefix_pattern(currency_symbol) + \
               cls.currency_number_pattern(thousands_separator, decimal_separator, ) + \
               cls.currency_suffix_pattern(currency_symbol, currency_name)

    @staticmethod
    def or_patterns(patterns: List[str]):
        return '(' + '|'.join([f"({pattern})" for pattern in patterns]) + ')'

    @staticmethod
    def enclose_pattern(pattern):
        return r'^' + pattern + r'$'


MONEY_USD_PATTERN = MoneyPatternHelper.money_pattern(r',', r'\.', r'\$', r'usd')
MONEY_EUR_PATTERN = MoneyPatternHelper.money_pattern(r' ', r',', r'€', r'eur')
MONEY_GBP_PATTERN = MoneyPatternHelper.money_pattern(r',', r'\.', r'£', r'gbp')
MONEY_RMB_PATTERN = MoneyPatternHelper.money_pattern(r',', r'\.', r'¥', r'rmb')
MONEY_CHF_PATTERN = MoneyPatternHelper.money_pattern(r"''", r'\.', r'CHf', r'chf')
