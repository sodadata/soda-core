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

from tests.common.sql_test_case import SqlTestCase


class TestNumericDataOverflow(SqlTestCase):

    def test_sum_overflow(self):
        self.create_test_table(
            [self.sql_declare_big_integer_column("numeric_value")],
            ["(9223372036854775807)",
             "(9223372036854775807)"])

        expression = self.warehouse.dialect.sql_expr_sum(self.warehouse.dialect.qualify_column_name("numeric_value"))
        sql = f"SELECT {expression} from {self.warehouse.dialect.qualify_table_name(self.default_test_table_name)}"
        self.warehouse.sql_fetchone(sql)
