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
from sodasql.scan.validity.money_patterns import MoneyPatternHelper
from sodasql.scan.validity.money_patterns import MONEY_USD_PATTERN, MONEY_EUR_PATTERN, MONEY_GBP_PATTERN, \
    MONEY_RMB_PATTERN, MONEY_CHF_PATTERN


class Validity:
    """
    For the sake of compatibility with all warehouses, we only support POSIX regexes. POSIX does not support ERE.
    This source makes the distinction between RE and ERE (Extended Regular Expression):
        http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html#tag_09_05
    More regexes:
        https://digitalfortress.tech/tricks/top-15-commonly-used-regex/
        http://regexlib.com/
    """

    FORMATS = {
        'number_whole': r'^\-?[0-9]+$',
        'number_decimal_point': r'^\-?[0-9]+\.[0-9]+$',
        'number_decimal_comma': r'^\-?[0-9]+,[0-9]+$',
        'number_percentage': r'^\-?\d+([\.,]\d+)? ?%$',
        'number_percentage_point': r'^\-?[0-9]+([\.][0-9]+)? ?%$',
        'number_percentage_comma': r'^\-?[0-9]+([,][0-9]+)? ?%$',
        'number_money_usd': MoneyPatternHelper.enclose_pattern(MONEY_USD_PATTERN),
        'number_money_eur': MoneyPatternHelper.enclose_pattern(MONEY_EUR_PATTERN),
        'number_money_gbp': MoneyPatternHelper.enclose_pattern(MONEY_GBP_PATTERN),
        'number_money_rmb': MoneyPatternHelper.enclose_pattern(MONEY_RMB_PATTERN),
        'number_money_chf': MoneyPatternHelper.enclose_pattern(MONEY_CHF_PATTERN),
        'number_money': MoneyPatternHelper.enclose_pattern(
            MoneyPatternHelper.or_patterns([MONEY_USD_PATTERN,
                                            MONEY_EUR_PATTERN,
                                            MONEY_GBP_PATTERN,
                                            MONEY_RMB_PATTERN,
                                            MONEY_CHF_PATTERN])
        ),

        'date_eu': r'^([1-9]|0[1-9]|[12][0-9]|3[01])[-\./]([1-9]|0[1-9]|1[012])[-\./](19|20)?[0-9][0-9]',
        'date_us': r'^([1-9]|0[1-9]|1[012])[-\./]([1-9]|0[1-9]|[12][0-9]|3[01])[-\./](19|20)?[0-9][0-9]',
        'date_inverse': r'^(19|20)[0-9][0-9][-\./]?([1-9]|0[1-9]|1[012])[-\./]?([1-9]|0[1-9]|[12][0-9]|3[01])',
        'time_24h': r'^([01][0-9]|2[0-3]):([0-5][0-9])$',
        'time_12h': r'^(1[0-2]|0?[1-9]):[0-5][0-9]$',
        'time': r'([0-9]|1[0-9]|2[0-4])[:-]([0-9]|[0-5][0-9])([:-]([0-9]|[0-5][0-9])(,[0-9]+)?)?$',
        'date_iso_8601':
            r'^'
            r'([1-9][0-9]{3}-((0[1-9]|1[0-2])-(0[1-9]|1[0-9]|2[0-8])|(0[13-9]|1[0-2])-(29|30)|(0[13578]|1[02])-31)|'
            r'([1-9][0-9](0[48]|[2468][048]|[13579][26])|([2468][048]|[13579][26])00)-02-29)'

            r'T([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\.[0-9]+)?'

            r'(Z|[+-][01][0-9]:[0-5][0-9])?'
            r'$',

        'uuid': r'^[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}$',
        'ip_address': r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$',
        'ipv4_address': r'^[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}$',
        # scary - but covers all cases
        'ipv6_address': r'^(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))$',

        'email': r'^[A-Za-z0-9.-_%]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,4}$',
        'phone_number': r'^((\+[0-9]{1,2}\s)?\(?[0-9]{3}\)?[\s.-])?[0-9]{3}[\s.-][0-9]{4}$',
        'credit_card_number': r'^[0-9]{14}|[0-9]{15}|[0-9]{16}|[0-9]{17}|[0-9]{18}|[0-9]{19}|([0-9]{4}-){3}[0-9]{4}|([0-9]{4} ){3}[0-9]{4}$',
    }

    def __init__(self):
        self.format = None
        self.regex = None
        self.values = None
        self.min_length = None
        self.max_length = None
        self.min = None
        self.max = None
