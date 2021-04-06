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
import re
import string
from os import path
from typing import List

import boto3

from sodasql.scan.db import sql_updates
from tests.common.boto3_helper import Boto3Helper
from tests.common.warehouse_fixture import WarehouseFixture


class AthenaFixture(WarehouseFixture):
    S3_URI_PATTERN = r"(^s3://)([^/]*)/(.*$)"

    def __init__(self, target: str) -> None:
        super().__init__(target)
        self.suite_id = 'suite_' + (''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(5)))

    def drop_database(self):
        super().drop_database()
        self.delete_staging_files()

    def create_database(self):
        self.database = self.create_unique_database_name()
        self.warehouse.dialect.database = self.database
        quoted_database_name = self.dialect.quote_identifier_declaration(self.database)
        sql_updates(self.warehouse.connection, [
            f'CREATE DATABASE IF NOT EXISTS {quoted_database_name}'])

    def sql_create_table(self, columns: List[str], table_name: str):
        columns_sql = ", ".join(columns)
        table_postfix = (''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(5)))
        table_location = path.join(self.warehouse.dialect.athena_staging_dir, self.suite_id, table_name, table_postfix)
        quoted_table_name = self.warehouse.dialect.quote_identifier_declaration(table_name)
        return f"CREATE EXTERNAL TABLE " \
               f"{quoted_table_name} ( \n " \
               f"{columns_sql} ) \n " \
               f"LOCATION '{table_location}';"

    def tear_down(self):
        pass

    def delete_staging_files(self):
        database_full_location = path.join(self.warehouse.dialect.athena_staging_dir, self.suite_id)
        logging.debug(f"Deleting all files under %s...", database_full_location)
        bucket = self._extract_s3_bucket(database_full_location)
        folder = self._extract_s3_folder(database_full_location)
        s3_client = self._create_s3_client()
        AthenaFixture.delete_s3_files(s3_client, bucket, folder)

    def _create_s3_client(self):
        Boto3Helper.filter_false_positive_boto3_warning()
        aws_credentials = self.warehouse.dialect.aws_credentials
        aws_credentials = aws_credentials.resolve_role("soda_sql_test_cleanup")
        return boto3.client(
            's3',
            region_name=aws_credentials.region_name,
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            aws_session_token=aws_credentials.session_token
        )

    @staticmethod
    def delete_s3_files(s3_client, bucket, folder, max_objects=200):
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
        object_keys = AthenaFixture._extract_object_keys(response)
        assert len(object_keys) < max_objects, \
            f"This method is intended for tests and hence limited to a maximum of {max_objects} objects, " \
            f"{len(object_keys)} objects exceeds the limit."
        if object_keys:
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': object_keys})


    @staticmethod
    def _extract_object_keys(response):
        object_keys = []
        if 'Contents' in response:
            objects = response['Contents']
            for summary in objects:
                key = summary['Key']
                object_keys.append({'Key': key})
        return object_keys

    @classmethod
    def _extract_s3_folder(cls, uri):
        return re.search(cls.S3_URI_PATTERN, uri).group(3)

    @classmethod
    def _extract_s3_bucket(cls, uri):
        return re.search(cls.S3_URI_PATTERN, uri).group(2)
