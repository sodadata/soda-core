#   Copyright 2021 Soda
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#    http://www.apache.org/licenses/LICENSE-2.0
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.
from datetime import datetime, timezone
from unittest import TestCase


class TestDateParser(TestCase):

    def test_default_date(self):
        default_date = datetime.now(tz=timezone.utc).isoformat(timespec='seconds')
        self.assertTrue(self.datetime_valid(default_date))

    def test_is_valid_iso_8601_date(self):
        compliant_date = "2021-04-15T09:00:00+02:00"
        self.assertTrue(self.datetime_valid(compliant_date))

        compliant_date_2 = "2021-04-15T09:00:00+00:00"
        self.assertTrue(self.datetime_valid(compliant_date_2))

    def test_is_not_valid_iso_8601_date(self):
        self.datetime_valid("2021-04-15T09:00:00+0200")
        self.assertRaises(ValueError)

    @staticmethod
    def datetime_valid(date: str):
        try:
            datetime.fromisoformat(date)
        except Exception as e:
            return False
        return True
