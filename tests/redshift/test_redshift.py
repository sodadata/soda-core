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
from unittest import skip

from tests.all_warehouse_tests import AllWarehouseTests


@skip('Not yet supported')
class TestRedshift(AllWarehouseTests):

    def get_warehouse_configuration(self):
        return {
            'name': 'test-redshift-warehouse',
            'type': 'redshift',
            'host': 'TODO',
            'port': 'TODO',
            'username': 'sodalite',
            'database': 'sodalite',
            'schema': 'public'}

