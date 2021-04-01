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

from google.cloud import bigquery

from tests.common.warehouse_fixture import WarehouseFixture


class BigQueryFixture(WarehouseFixture):

    def __init__(self, *args, **kwargs):
        self.project_id = None
        super().__init__(*args, **kwargs)

    def create_database(self):
        self.database = self.create_unique_database_name()
        self.warehouse.dialect.database = self.database
        self.project_id = self.warehouse.dialect.account_info_dict['project_id']
        dataset_id = f"{self.project_id}.{self.database}"
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "EU"
        self.warehouse.dialect.client.create_dataset(dataset, timeout=30)

    def drop_database(self):
        dataset_id = f"{self.project_id}.{self.database}"
        self.warehouse.dialect.client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)

    def tear_down(self):
        pass
