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
import logging
import random
import string

import boto3

from sodasql.credentials.aws_credentials import AwsCredentials
from tests.common.all_warehouse_tests import AllWarehouseTests
from tests.common.boto3_helper import Boto3Helper


class TestAthena(AllWarehouseTests):

    def __init__(self, method_name: str = ...) -> None:
        super().__init__(method_name)
        self.test_bucket = self.create_unique_bucket_name('soda')
        self.test_bucket_path = 'sodalite-athena-test'
        self.warehouse_configuration['work_dir'] = f's3://{self.test_bucket}/{self.test_bucket_path}'
        self.delete_s3_files()

    def setup_get_test_profile_target(self):
        return 'athena'

    def create_unique_bucket_name(self, prefix: str):
        random_suffix = ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(10))
        return f"{prefix}_{random_suffix}"

    def delete_s3_files(self):
        logging.debug(f"Deleting all files under s3://%s/%s", self.test_bucket, self.test_bucket_path)
        Boto3Helper.filter_false_positive_boto3_warning()
        aws_credentials = AwsCredentials.from_configuration(self.warehouse_configuration)
        aws_credentials = aws_credentials.resolve_role("soda_sql_test_cleanup")
        s3_client = boto3.client(
            's3',
            region_name=aws_credentials.region_name,
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            aws_session_token=aws_credentials.session_token
        )
        object_keys = []
        response = s3_client.list_objects_v2(Bucket=self.test_bucket, Prefix=self.test_bucket_path)
        if 'Contents' in response:
            for object_summary in response['Contents']:
                object_key = object_summary['Key']
                object_keys.append({'Key': object_key})
            max_objects = 200
            assert len(object_keys) < max_objects, \
                f'This method is intended for tests and hence limited to max {max_objects} keys: {len(object_keys)}'
            s3_client.delete_objects(Bucket=self.test_bucket, Delete={'Objects': object_keys})
