from __future__ import annotations

import logging
import os
import random
import re
import string
from textwrap import dedent
from typing import Optional

from soda_core.common.logs import Logs
from soda_core.common.soda_cloud import SodaCloud
from soda_core.common.statements.metadata_tables_query import (
    FullyQualifiedTableName,
    MetadataTablesQuery,
)
from soda_core.common.yaml import YamlSource
from soda_core.contracts.contract_verification import (
    ContractVerificationResult,
    ContractVerificationSession,
    ContractVerificationSessionResult,
)
from soda_core.tests.helpers.mock_soda_cloud import MockResponse, MockSodaCloud
from soda_core.tests.helpers.test_table import (
    TestColumn,
    TestDataType,
    TestTable,
    TestTableSpecification,
)

logger = logging.getLogger(__name__)


class DataSourceTestHelper:
    @classmethod
    def create(cls) -> DataSourceTestHelper:
        from soda_postgres.contracts.impl.data_sources.postgres_data_source_test_helper import (
            PostgresDataSourceTestHelper,
        )

        return PostgresDataSourceTestHelper()

    def __init__(self):
        self.dataset_prefix: list[str] = self._create_dataset_prefix()
        logs: Logs = Logs()
        self.data_source_impl: "DataSourceImpl" = self._create_data_source_impl()
        logs.remove_from_root_logger()
        if logs.has_errors():
            raise RuntimeError(f"Couldn't create DataSource: {self.data_source_impl.logs}")
        self.is_cicd = os.getenv("GITHUB_ACTIONS") is not None

        self.create_table_sql_type_dict: dict[str, str] = self._get_create_table_sql_type_dict()
        self.contract_data_type_dict: dict[str, str] = self._get_contract_data_type_dict()

        # Test table names that are present in the data source.
        # None means the data source is not queried
        self.existing_test_table_names: Optional[list[str]] = None

        # Maps TestTable to their unique_name property
        # (that is the the full table name composed of "SODATEST_" prefix, table purpose & test table hash)
        self.test_tables: dict[str, TestTable] = {}

        self.soda_cloud: Optional[SodaCloud] = None
        self.use_agent: bool = False

        if os.environ.get("SEND_RESULTS_TO_SODA_CLOUD") == "on":
            self.enable_soda_cloud()

    def enable_soda_cloud(self):
        logs: Logs = Logs()
        soda_cloud_yaml_str: str = """
            soda_cloud:
              host: ${env.SODA_CLOUD_HOST}
              api_key_id: ${env.SODA_CLOUD_API_KEY_ID}
              api_key_secret: ${env.SODA_CLOUD_API_KEY_SECRET}
        """
        soda_cloud_yaml_source: YamlSource = YamlSource.from_str(yaml_str=soda_cloud_yaml_str)
        self.soda_cloud = SodaCloud.from_yaml_source(soda_cloud_yaml_source=soda_cloud_yaml_source, variables={})
        if logs.has_errors():
            raise AssertionError(str(logs))

    def enable_soda_cloud_mock(self, responses: list[MockResponse]):
        self.soda_cloud = MockSodaCloud(responses)

    def _create_data_source_impl(self) -> "DataSourceImpl":
        """
        Called in constructor to initialized self.data_source
        """
        logs: Logs = Logs()
        data_source_yaml_source: YamlSource = self._create_data_source_yaml_source()
        from soda_core.common.data_source_impl import DataSourceImpl

        data_source_impl: DataSourceImpl = DataSourceImpl.from_yaml_source(data_source_yaml_source)
        assert not logs.has_errors()
        return data_source_impl

    def _create_data_source_yaml_str(self) -> str:
        """
        Called in _create_data_source_impl to initialized self.data_source_impl
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return ""

    def _create_data_source_yaml_source(self) -> YamlSource:
        test_data_source_yaml_str: str = self._create_data_source_yaml_str()
        test_data_source_yaml_str = dedent(test_data_source_yaml_str).strip()
        return YamlSource.from_str(yaml_str=test_data_source_yaml_str)

    def _create_dataset_prefix(self) -> list[str]:
        database_name: str = self._create_database_name()
        schema_name: str = self._create_schema_name()
        return [database_name, schema_name]

    def _create_database_name(self) -> Optional[str]:
        """
        Called in constructor to initialized self.database_name
        """
        return "soda_test"

    def _create_schema_name(self) -> Optional[str]:
        """
        Called in constructor to initialized self.schema_name
        """

        schema_name_parts = []

        github_ref_name = os.getenv("GITHUB_REF_NAME")
        github_head_ref = os.getenv("GITHUB_HEAD_REF")

        if not github_ref_name and not github_head_ref:
            user = os.getenv("USER", "anonymous")
            schema_name_parts.append("dev")
            schema_name_parts.append(user)

        else:
            python_version = os.getenv("PYTHON_VERSION")
            python_version_short = f'P{python_version.replace(".", "")}' if python_version else ""

            def generate_random_alpha_num_str(length: int) -> str:
                return "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))

            if github_head_ref:
                github_head_ref_short = (
                    github_head_ref[:15] if github_head_ref and len(github_head_ref) > 15 else github_head_ref
                )
                schema_name_parts.append("ci")
                schema_name_parts.append(github_head_ref_short)
                schema_name_parts.append(python_version_short)
                schema_name_parts.append(generate_random_alpha_num_str(5))

            else:
                schema_name_parts.append("ci_main")
                schema_name_parts.append(python_version_short)
                schema_name_parts.append(generate_random_alpha_num_str(5))

        schema_name_raw = "_".join(schema_name_parts)
        schema_name = re.sub("[^0-9a-zA-Z]+", "_", schema_name_raw).lower()
        return schema_name

    def _get_create_table_sql_type_dict(self) -> dict[str, str]:
        """
        DataSourceTestHelpers can override this method as an easy way
        to customize the get_create_table_sql_type behavior
        """
        return {
            TestDataType.TEXT: "VARCHAR(255)",
            TestDataType.INTEGER: "INT",
            TestDataType.DECIMAL: "FLOAT",
            TestDataType.DATE: "DATE",
            TestDataType.TIME: "TIME",
            TestDataType.TIMESTAMP: "TIMESTAMP",
            TestDataType.TIMESTAMP_TZ: "TIMESTAMPTZ",
            TestDataType.BOOLEAN: "BOOLEAN",
        }

    def _get_contract_data_type_dict(self) -> dict[str, str]:
        """
        DataSourceTestHelpers can override this method as an easy way
        to customize the get_schema_check_sql_type behavior
        """
        return {
            TestDataType.TEXT: "varchar",
            TestDataType.INTEGER: "integer",
            TestDataType.DECIMAL: "double precision",
            TestDataType.DATE: "date",
            TestDataType.TIME: "time",
            TestDataType.TIMESTAMP: "timestamp without time zone",
            TestDataType.TIMESTAMP_TZ: "timestamp with time zone",
            TestDataType.BOOLEAN: "boolean",
        }

    def get_create_table_sql_type(self, test_data_type: str) -> str:
        """
        Resolves DataType.XXX constants to the data type sql string used in the create table statement.
        Raises AssertionError if the data type is not found
        Behavior can be overridden by customizing the dict in _get_create_table_sql_type_dict
        or by overriding this method.
        """
        create_table_sql_type: str = self.create_table_sql_type_dict.get(test_data_type)
        assert create_table_sql_type is not None, f"Invalid create table data type {test_data_type}"
        return create_table_sql_type

    def get_contract_data_type(self, data_type: str) -> str:
        """
        Resolves DataType.XXX constants to the data type sql string used in the create table statement.
        Raises AssertionError if the data type is not found
        Behavior can be overridden by customizing the dict in _get_create_table_sql_type_dict
        or by overriding this method.
        """
        contract_data_type: str = self.contract_data_type_dict.get(data_type)
        assert contract_data_type is not None, f"No contract data type for {data_type}: "
        return contract_data_type

    def start_test_session(self) -> None:
        self.start_test_session_open_connection()
        self.start_test_session_ensure_schema()

    def start_test_session_open_connection(self) -> None:
        logs: Logs = Logs()
        self.data_source_impl.open_connection()
        logs.remove_from_root_logger()
        if logs.has_errors():
            raise AssertionError(f"Connection creation has errors. See logs.")

    def start_test_session_ensure_schema(self) -> None:
        if self.is_cicd:
            self.drop_test_schema_if_exists()
        self.create_test_schema_if_not_exists()

    def end_test_session(self, exception: Optional[Exception]) -> None:
        self.end_test_session_drop_schema()
        self.end_test_session_close_connection()

    def end_test_session_close_connection(self) -> None:
        self.data_source_impl.close_connection()

    def end_test_session_drop_schema(self) -> None:
        if self.is_cicd:
            self.drop_test_schema_if_exists()

    def query_existing_test_table_names(self):
        metadata_tables_query: MetadataTablesQuery = self.data_source_impl.create_metadata_tables_query()
        fully_qualified_table_names: list[FullyQualifiedTableName] = metadata_tables_query.execute(
            database_name=self.dataset_prefix[0],
            schema_name=self.dataset_prefix[1],
            include_table_name_like_filters=["SODATEST_%"],
        )
        return [
            fully_qualified_test_table_name.table_name
            for fully_qualified_test_table_name in fully_qualified_table_names
        ]

    def create_test_schema_if_not_exists(self) -> None:
        sql: str = self.create_test_schema_if_not_exists_sql()
        self.data_source_impl.execute_update(sql)

    def create_test_schema_if_not_exists_sql(self) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {self.dataset_prefix[1]} AUTHORIZATION CURRENT_USER;"

    def drop_test_schema_if_exists(self) -> None:
        sql: str = self.drop_test_schema_if_exists_sql()
        self.data_source_impl.execute_update(sql)

    def drop_test_schema_if_exists_sql(self) -> str:
        return f"DROP SCHEMA IF EXISTS {self.dataset_prefix[1]} CASCADE;"

    def ensure_test_table(self, test_table_specification: TestTableSpecification) -> TestTable:
        """
        Returns a test table with the given table data
        """
        if self.existing_test_table_names is None:
            self.existing_test_table_names = self.query_existing_test_table_names()

        test_table: TestTable = self.test_tables.get(test_table_specification.unique_name)
        if not test_table:
            test_table = self._create_test_table_python_object(test_table_specification)

            existing_test_table_names_lower: list[str] = [
                existing_test_table_name.lower() for existing_test_table_name in self.existing_test_table_names
            ]
            if test_table_specification.unique_name.lower() not in existing_test_table_names_lower:
                obsolete_table_names = [
                    existing_test_table
                    for existing_test_table in self.existing_test_table_names
                    if existing_test_table.lower().startswith(f"sodatest_{test_table_specification.name.lower()}_")
                ]
                if obsolete_table_names:
                    for obsolete_table_name in obsolete_table_names:
                        logger.debug(f"Test table {obsolete_table_name} has changed and will be recreated")
                        self._drop_test_table(table_name=obsolete_table_name)
                        self.existing_test_table_names.remove(obsolete_table_name)

                logger.debug(f"Test table {test_table_specification.unique_name} will be created")
                self._create_and_insert_test_table(test_table=test_table)
                self.existing_test_table_names.append(test_table.unique_name)

                self.data_source_impl.data_source_connection.commit()
        else:
            logger.debug(f"Test table {test_table.unique_name} already exists")

        return test_table

    def _create_test_table_python_object(self, test_table_specification: TestTableSpecification) -> TestTable:
        columns: list[TestColumn] = []
        for test_column_specification in test_table_specification.columns:
            contract_data_type = self.get_contract_data_type(test_column_specification.test_data_type)
            test_column: TestColumn = TestColumn(
                name=test_column_specification.name,
                test_data_type=contract_data_type,
                create_table_data_type=self.get_create_table_sql_type(test_column_specification.test_data_type),
                contract_data_type=self.get_contract_data_type(test_column_specification.test_data_type),
            )
            columns.append(test_column)

        sql_dialect = self.data_source_impl.sql_dialect

        return TestTable(
            data_source_name=self.data_source_impl.name,
            dataset_prefix=self.dataset_prefix,
            code_name=test_table_specification.unique_name,
            unique_name=test_table_specification.unique_name,
            qualified_name=sql_dialect.qualify_dataset_name(
                dataset_prefix=self.dataset_prefix,
                dataset_name=test_table_specification.unique_name,
            ),
            columns=columns,
            row_values=test_table_specification.row_values,
        )

    def _create_and_insert_test_table(self, test_table: TestTable) -> None:
        self._create_test_table(test_table)
        self._insert_test_table_rows(test_table)

    def _create_test_table(self, test_table: TestTable) -> None:
        sql: str = self._create_test_table_sql(test_table)
        self.data_source_impl.execute_update(sql)

    def _create_test_table_sql(self, test_table: TestTable) -> str:
        columns_sql: str = ",\n".join(
            [f"  {column.name} {column.create_table_data_type}" for column in test_table.columns.values()]
        )
        return self._create_test_table_sql_statement(test_table.qualified_name, columns_sql)

    def _create_test_table_sql_statement(self, table_name_qualified_quoted: str, columns_sql: str) -> str:
        return f"CREATE TABLE {table_name_qualified_quoted} ( \n{columns_sql} \n);"

    def _insert_test_table_rows(self, test_table: TestTable) -> None:
        sql: str = self._insert_test_table_rows_sql(test_table)
        if sql:
            self.data_source_impl.execute_update(sql)

    def _insert_test_table_rows_sql(self, test_table: TestTable) -> str:
        if test_table.row_values:

            def literalize_row(row: tuple) -> list[str]:
                return [self.data_source_impl.sql_dialect.literal(value) for value in row]

            literal_row_values = [literalize_row(row_values) for row_values in test_table.row_values]

            def format_literal_row_values(row: list[str]) -> str:
                return ",".join(row)

            rows_sql = ",\n".join([f"  ({format_literal_row_values(row)})" for row in literal_row_values])

            return self._insert_test_table_rows_sql_statement(test_table.qualified_name, rows_sql)

    def _insert_test_table_rows_sql_statement(self, table_name_qualified_quoted, rows_sql):
        return f"INSERT INTO {table_name_qualified_quoted} VALUES \n" f"{rows_sql};"

    def _drop_test_table(self, table_name: str) -> None:
        sql: str = self._drop_test_table_sql(table_name)
        self.data_source_impl.execute_update(sql)

    def _drop_test_table_sql(self, table_name) -> str:
        table_name_qualified_quoted: str = self.data_source_impl.sql_dialect.qualify_dataset_name(
            dataset_prefix=self.dataset_prefix, dataset_name=table_name
        )
        return self._drop_test_table_sql_statement(table_name_qualified_quoted)

    def _drop_test_table_sql_statement(self, table_name_qualified_quoted: str) -> str:
        return f"DROP TABLE {table_name_qualified_quoted};"

    def get_parse_errors_str(self, contract_yaml_str: str) -> str:
        contract_yaml_str: str = dedent(contract_yaml_str).strip()
        contract_verification_session_result = ContractVerificationSession.execute(
            contract_yaml_sources=[YamlSource.from_str(contract_yaml_str)], only_validate_without_execute=True
        )
        return contract_verification_session_result.get_errors_str()

    def assert_contract_error(self, contract_yaml_str: str, variables: Optional[dict[str, str]] = None) -> str:
        contract_yaml_str: str = dedent(contract_yaml_str).strip()

        contract_verification_session_result: ContractVerificationSessionResult = ContractVerificationSession.execute(
            contract_yaml_sources=[YamlSource.from_str(yaml_str=contract_yaml_str, file_path="yaml_string.yml")],
            only_validate_without_execute=True,
            variables=variables,
            data_source_impls=[self.data_source_impl],
            soda_cloud_impl=self.soda_cloud,
            soda_cloud_use_agent=self.use_agent,
        )

        errors_str: str = contract_verification_session_result.get_errors_str()
        if not errors_str:
            raise AssertionError(f"Expected contract execution errors, but got none")
        return errors_str

    def assert_contract_pass(
        self, test_table: TestTable, contract_yaml_str: str, variables: Optional[dict[str, str]] = None
    ) -> ContractVerificationResult:
        contract_verification_session_result: ContractVerificationSessionResult = self.verify_contract(
            contract_yaml_str=contract_yaml_str, test_table=test_table, variables=variables
        )
        if not isinstance(contract_verification_session_result, ContractVerificationSessionResult):
            raise AssertionError(f"No contract verification result session")
        if not contract_verification_session_result.is_ok():
            raise AssertionError(f"Expected contract verification passed")
        if len(contract_verification_session_result.contract_verification_results) == 0:
            raise AssertionError(f"No contract verification results")
        return contract_verification_session_result.contract_verification_results[0]

    def assert_contract_fail(
        self, test_table: TestTable, contract_yaml_str: str, variables: Optional[dict[str, str]] = None
    ) -> ContractVerificationResult:
        contract_verification_session_result: ContractVerificationSessionResult = self.verify_contract(
            contract_yaml_str=contract_yaml_str, test_table=test_table, variables=variables
        )
        if contract_verification_session_result.is_ok():
            raise AssertionError(f"Expected contract verification failed")
        return contract_verification_session_result.contract_verification_results[0]

    def verify_contract(
        self, contract_yaml_str: str, test_table: Optional[TestTable] = None, variables: Optional[dict] = None
    ) -> ContractVerificationSessionResult:
        contract_yaml_str = self._dedent_strip_and_prepend_dataset(contract_yaml_str, test_table)
        logger.debug(f"Contract:\n{contract_yaml_str}")
        return ContractVerificationSession.execute(
            contract_yaml_sources=[YamlSource.from_str(contract_yaml_str)],
            variables=variables,
            data_source_impls=[self.data_source_impl],
            soda_cloud_impl=self.soda_cloud,
            soda_cloud_use_agent=self.use_agent,
        )

    def _dedent_strip_and_prepend_dataset(self, contract_yaml_str: str, test_table: Optional[TestTable]):
        checks_contract_yaml_str = dedent(contract_yaml_str).strip()
        if test_table:
            header_contract_yaml_str: str = (
                f"dataset: {self.build_dqn(test_table)}\n"
            )
            checks_contract_yaml_str = header_contract_yaml_str + checks_contract_yaml_str
        return checks_contract_yaml_str

    def build_dqn(self, test_table: TestTable) -> str:
        dqn_parts: list[str] = [self.data_source_impl.name] + self.dataset_prefix + [test_table.unique_name]
        dqn: str = "/".join(dqn_parts)
        return dqn

    def test_method_ended(self) -> None:
        self.data_source_impl.data_source_connection.rollback()
        self.soda_cloud = None
        self.use_agent = False
