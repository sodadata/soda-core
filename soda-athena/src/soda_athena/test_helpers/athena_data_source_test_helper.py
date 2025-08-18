from __future__ import annotations

import logging
import os
import re

import boto3
from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTable
from soda_core.common.logging_constants import soda_logger

logger: logging.Logger = soda_logger


ATHENA_ACCESS_KEY_ID = os.getenv("ATHENA_ACCESS_KEY_ID", None)
ATHENA_SECRET_ACCESS_KEY = os.getenv("ATHENA_SECRET_ACCESS_KEY", None)
ATHENA_S3_TEST_DIR = os.getenv("ATHENA_S3_TEST_DIR")
ATHENA_REGION_NAME = os.getenv("ATHENA_REGION_NAME", "eu-west-1")
ATHENA_CATALOG = os.getenv("ATHENA_CATALOG", "awsdatacatalog")


class AthenaDataSourceTestHelper(DataSourceTestHelper):
    def __init__(self):
        super().__init__()
        self.s3_test_dir = ATHENA_S3_TEST_DIR
        if self.s3_test_dir.endswith("/"):
            self.s3_test_dir = self.s3_test_dir[:-1]

    def _create_database_name(self) -> str | None:
        return ATHENA_CATALOG

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return f"""
                type: athena
                name: ATHENA_TEST_DS
                connection:
                    access_key_id: {ATHENA_ACCESS_KEY_ID}
                    secret_access_key: {ATHENA_SECRET_ACCESS_KEY}
                    staging_dir: {ATHENA_S3_TEST_DIR}/staging-dir
                    region_name: {ATHENA_REGION_NAME}
                    catalog: {ATHENA_CATALOG}
            """

    def _create_test_table_sql_statement(self, table_name_qualified_quoted: str, columns_sql: str) -> str:
        # Table name will not be quoted as we removed the quoting with `quote_default`, we just keep the original name
        table_part = re.sub("[^0-9a-zA-Z]+", "_", table_name_qualified_quoted)
        location = f"{self._get_3_schema_dir()}/{table_part}"
        return f"CREATE EXTERNAL TABLE {table_name_qualified_quoted} ( \n{columns_sql} \n) \n LOCATION '{location}/';"

    def _create_columns_sql(self, test_table: TestTable) -> str:
        columns_sql: str = ",\n".join(
            [
                # Athena requires a different quoting for the create table statement.
                f"  `{column.name}` {column.create_table_data_type}"
                for column in test_table.columns.values()
            ]
        )
        return columns_sql

    def _get_3_schema_dir(self):
        schema_name = self.dataset_prefix[self.data_source_impl.sql_dialect.get_schema_prefix_index()]
        return f"{self.s3_test_dir}/{schema_name}"

    def drop_test_schema_if_exists(self) -> str:
        super().drop_test_schema_if_exists()
        self._delete_s3_schema_files()

    def _delete_s3_schema_files(self):
        s3_schema_dir = self._get_3_schema_dir()
        logger.debug(f"Deleting all s3 files under {s3_schema_dir}")
        bucket = self._extract_s3_bucket(s3_schema_dir)
        folder = self._extract_s3_folder(s3_schema_dir)
        s3_client = self._create_s3_client()
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=folder)
        object_keys = self._extract_object_keys(response)
        logger.debug(f"Found {len(object_keys)} to be deleted")
        max_objects = 200
        assert len(object_keys) < max_objects, (
            f"This method is intended for tests and hence limited to a maximum of {max_objects} objects, "
            f"{len(object_keys)} objects exceeds the limit."
        )
        if len(object_keys) > 0:
            response: dict = s3_client.delete_objects(Bucket=bucket, Delete={"Objects": object_keys})
            deleted_list = response.get("Deleted") if isinstance(response, dict) else None
            deleted_count = (
                len(deleted_list) if isinstance(deleted_list, list) else "Unknown aws delete objects response"
            )
            logger.debug(f"Deleted {deleted_count}")

    def _create_s3_client(self):
        self.filter_false_positive_boto3_warning()
        aws_credentials = self.data_source_impl.data_source_connection.aws_credentials
        aws_credentials = aws_credentials.resolve_role("soda_sql_test_cleanup")
        return boto3.client(
            "s3",
            region_name=aws_credentials.region_name,
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            aws_session_token=aws_credentials.session_token,
        )

    def _extract_object_keys(self, response):
        object_keys = []
        if "Contents" in response:
            objects = response["Contents"]
            for summary in objects:
                key = summary["Key"]
                object_keys.append({"Key": key})
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

        warnings.filterwarnings("ignore", category=ResourceWarning, message="unclosed <ssl.SSLSocket")
        warnings.filterwarnings("ignore", category=DeprecationWarning, message="the imp module is deprecated")
        warnings.filterwarnings("ignore", category=DeprecationWarning, message="Using or importing the ABCs")
