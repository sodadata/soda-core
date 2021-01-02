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

import pyathena

from sodasql.scan.dialect import Dialect, ATHENA
from sodasql.scan.parse_logs import ParseConfiguration


class AthenaDialect(Dialect):

    def __init__(self, warehouse_cfg: ParseConfiguration):
        super().__init__()
        self.aws_credentials = warehouse_cfg.get_aws_credentials_optional()
        self.athena_staging_dir = warehouse_cfg.get_str_required('staging_dir')
        self.database = warehouse_cfg.get_str_required('database')
        self.schema = warehouse_cfg.get_str_required('schema')

    @classmethod
    def create_default_configuration_dict(cls, warehouse_type: str):
        return {
            'type': ATHENA,
            'access_key_id': '--- ENTER AWS ACCESS KEY ID HERE ---',
            'secret_access_key': '--- ENTER AWS SECRET ACCESS KEY HERE ---',
            'role_arn': '--- ENTER AWS ROLE ARN TO ASSUME HERE (optional) ---',
            'region': '--- ENTER AWS REGION HERE (optional, default is eu-west-1) ---',
            'staging_dir': '--- ENTER STAGING DIR HERE ---',
            'database': '--- ENTER DATABASE HERE ---'
        }

    def create_connection(self):
        # pyathena.connect will do the role resolving
        # aws_credentials = self.aws_credentials.resolve_role('soda_scan')
        return pyathena.connect(
            aws_access_key_id=self.aws_credentials.access_key_id,
            aws_secret_access_key=self.aws_credentials.secret_access_key,
            s3_staging_dir=self.athena_staging_dir,
            region_name=self.aws_credentials.region_name,
            role_arn=self.aws_credentials.role_arn)

    def sql_columns_metadata_query(self, scan_configuration):
        return (f"SELECT column_name, data_type, is_nullable \n"
                f"FROM information_schema.columns \n"
                f"WHERE table_name = '{scan_configuration.table_name.lower()}' \n"
                f"  AND table_schema = '{self.database.lower()}';")
