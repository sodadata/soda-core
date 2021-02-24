import json
import os
from unittest import TestCase

import boto3
import psycopg2
import pyathena
from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2.service_account import Credentials
from snowflake import connector

from sodasql.common.logging_helper import LoggingHelper
from sodasql.scan.aws_credentials import AwsCredentials
from sodasql.scan.db import sql_update, sql_fetchone
from sodasql.scan.env_vars import EnvVars
from tests.common.boto3_helper import Boto3Helper
from tests.warehouses.athena_fixture import AthenaFixture

LoggingHelper.configure_for_test()
EnvVars.load_env_vars('test')


class TestSqlQuotes(TestCase):

    connection = None

    table_name = 'quotestest'
    cte_table_name = 'ctetablename'

    def test_postgres(self):
        database = 'sodasql'
        schema = 'public'

        connection = psycopg2.connect(
            user='sodasql',
            host='localhost',
            database=database,
            options=f'-c search_path={schema}')

        try:
            sql_update(connection, (
                f'DROP TABLE IF EXISTS "{database}"."{schema}"."{self.table_name}"'
            ))

            sql_update(connection, (
                f'CREATE TABLE "{database}"."{schema}"."{self.table_name}" (\n'
                f'  "id" VARCHAR(255), \n'
                f'  "size" INTEGER );'
            ))

            sql_fetchone(connection, (
                f'WITH "{self.cte_table_name}" as ( \n'
                f'  SELECT "id" as "v", "size" as "s", LENGTH("id") as "l" \n'
                f'  FROM "{database}"."{schema}"."{self.table_name}" \n'
                f'  WHERE "size" = 1 \n'
                f'  ORDER BY "size" ASC ) \n'
                f'SELECT COUNT(DISTINCT("v")), COUNT("s") \n'
                f'FROM "{self.cte_table_name}" \n'
                f'WHERE "l" > 0'
            ))

        finally:
            connection.close()

    def test_redshift(self):
        database = 'soda_test_quotes_db'
        schema = 'public'

        connection = psycopg2.connect(
            user=os.getenv('SODA_REDSHIFT_USERNAME'),
            password=os.getenv('SODA_REDSHIFT_PASSWORD'),
            host='soda-agent-test.c0l8nhpcaknw.eu-west-1.redshift.amazonaws.com',
            port='5439',
            database=database)

        try:

            sql_update(connection, (
                f'DROP TABLE IF EXISTS "{database}"."{schema}"."{self.table_name}"'
            ))

            sql_update(connection, (
                f'CREATE TABLE "{database}"."{schema}"."{self.table_name}" (\n'
                f'  "id" VARCHAR(255), \n'
                f'  "size" INTEGER );'
            ))

            sql_fetchone(connection, (
                f'WITH "{self.cte_table_name}" as ( \n'
                f'  SELECT "id" as "v", "size" as "s", LENGTH("id") as "l" \n'
                f'  FROM "{database}"."{schema}"."{self.table_name}" \n'
                f'  WHERE "size" = 1 \n'
                f'  ORDER BY "size" ASC ) \n'
                f'SELECT COUNT(DISTINCT("v")), COUNT("s") \n'
                f'FROM "{self.cte_table_name}" \n'
                f'WHERE "l" > 0'
            ))

        finally:
            connection.close()

    def test_snowflake(self):
        schema = 'PUBLIC'

        connection = connector.connect(
            user=os.getenv('SODA_SNOWFLAKE_USERNAME'),
            password=os.getenv('SODA_SNOWFLAKE_PASSWORD'),
            account='SODADATAPARTNER.eu-central-1',
            warehouse='DEMO_WH',
            schema=schema
        )

        try:
            database = 'soda_test_quotes_db'

            sql_update(connection, (
                f'CREATE DATABASE IF NOT EXISTS "{database}"'
            ))

            sql_update(connection, (
                f'DROP TABLE IF EXISTS "{database}"."{schema}"."{self.table_name}"'
            ))

            sql_update(connection, (
                f'CREATE TABLE "{database}"."{schema}"."{self.table_name}" (\n'
                f'  "id" VARCHAR(255), \n'
                f'  "size" INTEGER );'
            ))

            sql_fetchone(connection, (
                f'WITH "{self.cte_table_name}" as ( \n'
                f'  SELECT "id" as "v", "size" as "s", LENGTH("id") as "l" \n'
                f'  FROM "{database}"."{schema}"."{self.table_name}" \n'
                f'  WHERE "size" = 1 \n'
                f'  ORDER BY "size" ASC ) \n'
                f'SELECT COUNT(DISTINCT("v")), COUNT("s") \n'
                f'FROM "{self.cte_table_name}" \n'
                f'WHERE "l" > 0'
            ))

        finally:
            connection.close()

    def test_bigquery(self):
        database = 'sodalite'

        account_info_json_str = os.getenv('BIGQUERY_ACCOUNT_INFO_JSON')
        account_info_json_dict = json.loads(account_info_json_str)
        credentials = Credentials.from_service_account_info(account_info_json_dict)
        project_id = account_info_json_dict['project_id']
        client = bigquery.Client(project=project_id, credentials=credentials)
        connection = dbapi.Connection(client)

        try:
            sql_update(connection, (
                f'DROP TABLE IF EXISTS `{database}`.`{self.table_name}`'
            ))

            sql_update(connection, (
                f'CREATE TABLE `{database}`.`{self.table_name}` (\n'
                f'  `id` STRING, \n'
                f'  `size` INT64 );'
            ))

            sql_fetchone(connection, (
                f'WITH `{self.cte_table_name}` as ( \n'
                f'  SELECT "id" as `v`, "size" as `s`, LENGTH("id") as `l` \n'
                f'  FROM `{database}`.`{self.table_name}` \n'
                f'  WHERE `size` = 1 \n'
                f'  ORDER BY `size` ASC ) \n'
                f'SELECT COUNT(DISTINCT("v")), COUNT("s") \n'
                f'FROM `{self.cte_table_name}`;'
            ))

        finally:
            connection.close()

    def test_athena(self):
        Boto3Helper.filter_false_positive_boto3_warning()

        aws_access_key_id = os.getenv('SODA_ATHENA_ACCESS_KEY_ID')
        aws_secret_access_key = os.getenv('SODA_ATHENA_SECRET_ACCESS_KEY')

        s3_staging_bucket = 'sodalite-test'
        s3_staging_folder = 'soda_quotes_test'

        database = 'sodalite_test'
        schema = 'PUBLIC'

        connection = pyathena.connect(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            s3_staging_dir=f's3://{s3_staging_bucket}/',
            region_name='eu-west-1')

        try:

            sql_update(connection, (
                f'CREATE DATABASE IF NOT EXISTS `{database}`'
            ))

            sql_update(connection, (
                f'DROP TABLE IF EXISTS `{database}`.`{self.table_name}`'
            ))

            sql_update(connection, (
                f'CREATE EXTERNAL TABLE `{database}`.`{self.table_name}` (\n'
                f'  `id` VARCHAR(255), \n'
                f'  `size` INTEGER ) \n'
                f"LOCATION 's3://{s3_staging_bucket}/{s3_staging_folder}';"
            ))

            sql_fetchone(connection, (
                f'WITH "{self.cte_table_name}" as ( \n'
                f'  SELECT "id" as "v", "size" as "s", LENGTH("id") as "l" \n'
                f'  FROM "{database}"."{self.table_name}" \n'
                f'  WHERE "size" = 1 \n'
                f'  ORDER BY "size" ASC ) \n'
                f'SELECT COUNT(DISTINCT("v")), COUNT("s") \n'
                f'FROM "{self.cte_table_name}" \n'
                f'WHERE "l" > 0'
            ))

        finally:
            TestSqlQuotes.delete_athena_s3_staging_files(
                aws_access_key_id,
                aws_secret_access_key,
                s3_staging_bucket,
                s3_staging_folder)

            connection.close()

    @staticmethod
    def delete_athena_s3_staging_files(aws_access_key_id, aws_secret_access_key, s3_staging_bucket, s3_staging_folder):
        aws_credentials = AwsCredentials(access_key_id=aws_access_key_id,
                                         secret_access_key=aws_secret_access_key)
        aws_credentials = aws_credentials.resolve_role("soda_sql_test_cleanup")
        s3_client = boto3.client(
            's3',
            region_name=aws_credentials.region_name,
            aws_access_key_id=aws_credentials.access_key_id,
            aws_secret_access_key=aws_credentials.secret_access_key,
            aws_session_token=aws_credentials.session_token
        )
        AthenaFixture.delete_s3_files(s3_client, s3_staging_bucket, s3_staging_folder)