import logging
import re
from os import path

import boto3
from pyathena import OperationalError
from soda.data_sources.spark_df_data_source import DataSourceImpl
from tests.helpers.test_table import TestTable
from tests.helpers.data_source_fixture import DataSourceFixture

logger = logging.getLogger(__name__)


class AthenaDataSourceFixture(DataSourceFixture):
    def __init__(self, data_source: DataSourceImpl):
        super().__init__(data_source=data_source)

    def _drop_schema_if_exists_sql(self) -> str:
        return f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE"

    def _create_schema_if_not_exists_sql(self) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"

    def _create_test_table_sql_compose(self, qualified_table_name, columns_sql) -> str:
        table_part = qualified_table_name.replace('"', "")
        table_part = re.sub("[^0-9a-zA-Z]+", "_", table_part)
        location = f"{self.data_source.athena_staging_dir}{self.schema_name}_{table_part}"
        return f"CREATE EXTERNAL TABLE {qualified_table_name} ( \n{columns_sql} \n)LOCATION '{location}/'"

    def delete_staging_files(self):
        database_full_location = path.join(self.data_source.athena_staging_dir, self.suite_id)
        logging.debug(f"Deleting all files under %s...", database_full_location)
        bucket = self._extract_s3_bucket(database_full_location)
        folder = self._extract_s3_folder(database_full_location)
        s3_client = self._create_s3_client()
        self.delete_s3_files(s3_client, bucket, folder)

    def _create_s3_client(self):
        self.filter_false_positive_boto3_warning()
        aws_credentials = self.data_source.aws_credentials
        aws_credentials = aws_credentials.resolve_role("soda_sql_test_cleanup")
        return boto3.client(
            's3',
            region_name=aws_credentials.region_name,
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            aws_session_token=aws_credentials.session_token
        )

    def delete_s3_files(self, s3_client, bucket, folder, max_objects=200):
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
        object_keys = self._extract_object_keys(response)
        assert len(object_keys) < max_objects, \
            f"This method is intended for tests and hence limited to a maximum of {max_objects} objects, " \
            f"{len(object_keys)} objects exceeds the limit."
        if object_keys:
            s3_client.delete_objects(Bucket=bucket, Delete={'Objects': object_keys})

    def _extract_object_keys(self, response):
        object_keys = []
        if 'Contents' in response:
            objects = response['Contents']
            for summary in objects:
                key = summary['Key']
                object_keys.append({'Key': key})
        return object_keys

    S3_URI_PATTERN = r"(^s3://)([^/]*)/(.*$)"

    @classmethod
    def _extract_s3_folder(cls, uri):
        return re.search(cls.S3_URI_PATTERN, uri).group(3)

    @classmethod
    def _extract_s3_bucket(cls, uri):
        return re.search(cls.S3_URI_PATTERN, uri).group(2)

    def filter_false_positive_boto3_warning(self):
        # see
        # https://github.com/boto/boto3/issues/454#issuecomment-380900404
        import warnings
        warnings.filterwarnings("ignore", category=ResourceWarning, message='unclosed <ssl.SSLSocket')
        warnings.filterwarnings("ignore", category=DeprecationWarning, message='the imp module is deprecated')
        warnings.filterwarnings("ignore", category=DeprecationWarning, message='Using or importing the ABCs')
