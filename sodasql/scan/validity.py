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

class Validity:

    FORMATS = {
        # more regexes: https://digitalfortress.tech/tricks/top-15-commonly-used-regex/ & http://regexlib.com/

        # http://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html#tag_09_05
        # This source makes the distinction between RE and ERE (Extended Regular Expression). POSIX does not support ERE.

        'number_whole': r'^\-?\d+$',
        'number_decimal_point': r'^\-?\d+\.\d+$',
        'number_decimal_comma': r'^\-?\d+,\d+$',
        'number_percentage': r'^\-?\d+([\.,]\d+)? ?%$',
        'date_eu': r'^([1-9]|0[1-9]|[12][0-9]|3[01])[-\./]([1-9]|0[1-9]|1[012])[-\./](19|20)?\d\d',
        'date_us': r'^([1-9]|0[1-9]|1[012])[-\./]([1-9]|0[1-9]|[12][0-9]|3[01])[-\./](19|20)?\d\d',
        'date_inverse': r'^(19|20)\d\d[-\./]?([1-9]|0[1-9]|1[012])[-\./]?([1-9]|0[1-9]|[12][0-9]|3[01])',
        'time': r'([0-9]|1[0-9]|2[0-4])[:-]([0-9]|[0-5][0-9])([:-]([0-9]|[0-5][0-9])(,\d+)?)?$',
        'uuid': r'^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$',
        'email': r'^[A-Z0-9._%-]+@[A-Z0-9.-]+\.[A-Z]{2,4}$',
        'phone': r'^(\+\d{1,2}\s)?\(?\d{3}\)?[\s.-]\d{3}[\s.-]\d{4}$',
        'credit_card_number': r'^\d{16}|(\d{4}-){3}\d{4}|(\d{4} ){3}\d{4}$',
        'ip_address': r'^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$'

        # Regular expressions containing non-capturing groups are all non POSIX, that is, all the ones containing "(?:"
        #
        # 'number_money_usd': r'^((?:\$) ?\-?(?:\d+([\.\,]\d+)?))|\-?((?:\d+([\.\,]\d+)? ?(?:\$|usd|USD)))$',
        # 'number_money_eur': r'^((?:\€) ?\-?(?:\d+([\.\,]\d+)?))|\-?((?:\d+([\.\,]\d+)? ?(?:\€|eur|EUR)))$',
        # 'number_money_gbp': r'^((?:\£) ?\-?(?:\d+([\.\,]\d+)?))|\-?((?:\d+([\.\,]\d+)? ?(?:\£|gbp|GBP)))$',
        # 'number_money_rmb': r'^((?:\¥) ?\-?(?:\d+([\.\,]\d+)?))|\-?((?:\d+([\.\,]\d+)? ?(?:\¥|rmb|RMB)))$',
        # 'number_money': r'^((?:\$|\€|\£|\¥) ?\-?(?:\d+([\.\,]\d+)?))|\-?((?:\d+([\.\,]\d+)? ?(?:\$|\€|\£|\¥|[a-zA-Z]{3})))$',
        # 'date_iso_8601':
        #     r'^(?:[1-9]\d{3}-(?:(?:0[1-9]|1[0-2])-(?:0[1-9]|1\d|2[0-8])'
        #     r'|(?:0[13-9]|1[0-2])-(?:29|30)|(?:0[13578]|1[02])-31)'
        #     r'|(?:[1-9]\d(?:0[48]|[2468][048]|[13579][26])|'
        #     r'(?:[2468][048]|[13579][26])00)-02-29)T(?:[01]\d|2[0-3]):[0-5]\d:[0-5]\d(\.\d+)?(?:Z|[+-][01]\d:[0-5]\d)$',
    }

    def __str__(self):
        self.format = None
        self.regex = None
        self.values = None
        self.min_length = None
        self.max_length = None
        self.min = None
        self.max = None


