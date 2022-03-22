import json
import logging
import re
from json.decoder import JSONDecodeError
from typing import Dict, List, Optional

from google.cloud import bigquery
from google.cloud.bigquery import dbapi
from google.oauth2.service_account import Credentials
from soda.common.exceptions import DataSourceConnectionError
from soda.execution.data_source import DataSource
from soda.execution.data_type import DataType

logger = logging.getLogger(__name__)


class DataSourceImpl(DataSource):
    TYPE = "bigquery"

    SCHEMA_CHECK_TYPES_MAPPING: Dict = {
        "STRING": ["character varying", "varchar"],
        "INT64": ["integer", "int"],
    }
    SQL_TYPE_FOR_CREATE_TABLE_MAP: Dict = {
        DataType.TEXT: "STRING",
        DataType.INTEGER: "INT64",
        DataType.DECIMAL: "NUMERIC",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMP_TZ: "TIMESTAMP",
        DataType.BOOLEAN: "BOOL",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP = {
        DataType.TEXT: "STRING",
        DataType.INTEGER: "INT64",
        DataType.DECIMAL: "NUMERIC",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMP_TZ: "TIMESTAMP",
        DataType.BOOLEAN: "BOOL",
    }

    def connect(self, connection_properties):
        self.connection_properties = connection_properties

        try:
            self.dataset_name = connection_properties.get("dataset")
            # self.table_prefix = self.dataset_name
            default_auth_scopes = [
                "https://www.googleapis.com/auth/bigquery",
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive",
            ]
            self.auth_scopes = connection_properties.get("auth_scopes", default_auth_scopes)
            # self.auth_scopes = parser.get_list_optional("auth_scopes", default_auth_scopes)
            # self.__context_auth = parser.get_bool_optional("use_context_auth", None)
            # if self.__context_auth:
            #     self.account_info_dict = None
            #     self.project_id = parser.get_str_required("project_id")
            #     logger.info("Using context auth, account_info_json will be ignored.")
            # else:
            self.account_info_dict = self.__parse_json_credential()
            if self.account_info_dict:
                self.project_id = self.account_info_dict.get("project_id")
            # self.client = None

            # if self.__context_auth:
            #     credentials = None
            # elif self.account_info_dict:
            credentials = Credentials.from_service_account_info(self.account_info_dict, scopes=self.auth_scopes)
            # else:
            # raise Exception("Account_info_json or account_info_json_path or use_context_auth are not provided")

            self.client = bigquery.Client(
                project=self.project_id,
                credentials=credentials,
                default_query_job_config=bigquery.QueryJobConfig(
                    default_dataset=f"{self.project_id}.{self.dataset_name}",
                ),
            )
            self.connection = dbapi.Connection(self.client)

            return self.connection
        except Exception as e:
            raise DataSourceConnectionError(self.TYPE, e)

    def __parse_json_credential(self):
        account_info_path = self.connection_properties.get("account_info_json_path")
        if account_info_path:
            try:
                account_info = self._read_file_as_string(account_info_path)
                if account_info is not None:
                    return json.loads(account_info)
            except JSONDecodeError as e:
                logger.error(f"Error parsing credentials from {account_info_path}: {e}")
        else:
            try:
                cred = self.connection_properties.get("account_info_json")
                # Prevent json load when the Dialect is init from create command
                if cred is not None:
                    return json.loads(json.dumps(cred))
                else:
                    logger.warning("Dialect initiated from the create command, cred is None.")
            except JSONDecodeError as e:
                logger.error(f"Error parsing credential 'account_info_json': {e}")

    @staticmethod
    def _read_file_as_string(file_path: str):
        try:
            with open(file_path, encoding="utf-8") as f:
                return f.read()
        except Exception as e:
            logger.error(f"Could not read file {file_path}: {str(e)}")

    def sql_to_get_column_metadata_for_table(self, table_name: str):
        return (
            f"SELECT column_name, data_type, is_nullable "
            f"FROM `{self.dataset_name}.INFORMATION_SCHEMA.COLUMNS` "
            f"WHERE table_name = '{table_name}';"
        )

    def sql_get_column(
        self, include_tables: Optional[List[str]] = None, exclude_tables: Optional[List[str]] = None
    ) -> str:
        table_filter_expression = self.sql_table_filter_based_on_includes_excludes(
            "table_name", "table_schema", include_tables, exclude_tables
        )
        where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
        return (
            f"SELECT table_name, column_name, data_type, is_nullable \n"
            f"FROM {self.dataset_name}.INFORMATION_SCHEMA.COLUMNS"
            f"{where_clause}"
        )

    def sql_find_table_names(self, filter: Optional[str] = None) -> str:
        sql = f"SELECT table_name \n" f"FROM `{self.dataset_name}.INFORMATION_SCHEMA.COLUMNS`"
        where_clauses = []
        # if self.schema:
        # where_clauses.append(f"lower(table_schema) = '{self.schema.lower()}'")
        if filter:
            where_clauses.append(f"lower(table_name) like '{filter.lower()}'")
        if where_clauses:
            where_clauses_sql = "\n  AND ".join(where_clauses)
            sql += f"\nWHERE {where_clauses_sql}"
        return sql

    def sql_get_table_names_with_count(
        self, include_tables: Optional[List[str]] = None, exclude_tables: Optional[List[str]] = None
    ) -> str:
        table_filter_expression = self.sql_table_filter_based_on_includes_excludes(
            "table_id", "dataset_id", include_tables, exclude_tables
        )
        where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
        return f"SELECT table_id, row_count \n" f"FROM {self.dataset_name}.__TABLES__" f"{where_clause}"

    def quote_table(self, table_name) -> str:
        return f"`{table_name}`"

    def quote_column(self, column_name: str) -> str:
        return f"`{column_name}`"

    def escape_regex(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"REGEXP_CONTAINS({expr}, '{regex_pattern}')"

    def regex_replace_flags(self) -> str:
        return ""

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: Optional[List[object]], expr: str):
        # TODO add all of these bigquery specific statistical aggregate functions: https://cloud.google.com/bigquery/docs/reference/standard-sql/aggregate_analytic_functions
        if metric_name in [
            "stddev",
            "stddev_pop",
            "stddev_samp",
            "variance",
            "var_pop",
            "var_samp",
        ]:
            return f"{metric_name.upper()}({expr})"
        return super().get_metric_sql_aggregation_expression(metric_name, metric_args, expr)

    def sql_information_schema_identifier(self) -> str:
        return f"{self.project_id}.{self.dataset_name}.INFORMATION_SCHEMA.TABLES"

    @staticmethod
    def format_column_default(identifier: str) -> str:
        return identifier

    @staticmethod
    def format_type_default(identifier: str) -> str:
        return identifier.upper()

    def safe_connection_data(self):
        return [
            self.type,
            self.connection_properties.get("project_id"),
        ]
