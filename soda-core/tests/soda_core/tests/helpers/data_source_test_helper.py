from __future__ import annotations

import logging
import os
import random
import re
import string
from textwrap import dedent

from soda_core.common.data_source import DataSource
from soda_core.common.data_source_results import QueryResult
from soda_core.common.data_source_parser import DataSourceParser
from soda_core.common.logs import Logs, Log
from soda_core.common.statements.metadata_tables_query import FullyQualifiedTableName, MetadataTablesQuery
from soda_core.common.yaml import YamlFile
from soda_core.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationBuilder,
    ContractVerificationResult, ContractResult,
)
from soda_core.tests.helpers.test_table import TestDataType, TestTable, TestTableSpecification, TestColumn

logger = logging.getLogger(__name__)


class TestContractVerificationBuilder(ContractVerificationBuilder):
    __test__ = False

    def __init__(self):
        super().__init__()
        self.data_source = None

    def with_contract_yaml_str(self, contract_yaml_str: str) -> ContractVerificationBuilder:
        logger.debug(f"Contract:\n{contract_yaml_str}")
        super().with_contract_yaml_str(contract_yaml_str)
        return self

    def build(self) -> TestContractVerification:
        return TestContractVerification(self)


class TestContractVerification(ContractVerification):
    __test__ = False

    @classmethod
    def builder(cls) -> TestContractVerificationBuilder:
        return TestContractVerificationBuilder()

    def __init__(self, test_contract_verification_builder: TestContractVerificationBuilder):
        super().__init__(contract_verification_builder=test_contract_verification_builder)

    def _initialize_data_source(self, contract_verification_builder: ContractVerificationBuilder) -> None:
        self.data_source = contract_verification_builder.data_source


class DataSourceTestHelper:

    @classmethod
    def create(cls) -> DataSourceTestHelper:
        from soda_postgres.contracts.impl.data_sources.postgres_data_source_test_helper import PostgresDataSourceTestHelper
        return PostgresDataSourceTestHelper()

    def __init__(self):
        super().__init__()
        self.database_name: str = self._create_database_name()
        self.schema_name: str = self._create_schema_name()
        self.data_source: DataSource = self._create_data_source()
        if self.data_source.logs.has_errors():
            raise RuntimeError(f"Couldn't create DataSource: {self.data_source.logs}")
        self.is_cicd = os.getenv("GITHUB_ACTIONS") is not None

        self.create_table_sql_type_dict: dict[str, str] = self._get_create_table_sql_type_dict()
        self.contract_data_type_dict: dict[str, str] = self._get_contract_data_type_dict()

        # Test table names that are present in the data source.
        # None means the data source is not queried
        self.existing_test_table_names: list[str] | None = None

        # Maps TestTable to their unique_name property
        # (that is the the full table name composed of "SODATEST_" prefix, table purpose & test table hash)
        self.test_tables: dict[str, TestTable] = {}

    def _create_data_source(self) -> DataSource:
        """
        Called in constructor to initialized self.data_source
        """
        logs: Logs = Logs()
        test_data_source_yaml_dict: dict = self._create_data_source_yaml_dict()
        data_source_yaml_file = YamlFile(yaml_dict=test_data_source_yaml_dict, logs=logs)
        data_source_parser = DataSourceParser(
            data_source_yaml_file=data_source_yaml_file,
            spark_session=None
        )
        data_source: DataSource = data_source_parser.parse()
        assert not logs.has_errors()
        return data_source

    def _create_data_source_yaml_dict(self) -> dict:
        """
        Called in _create_data_source to initialized self.data_source
        self.database_name and self.schema_name are available if appropriate for the data source type
        """
        return {}

    def _create_database_name(self) -> str | None:
        """
        Called in constructor to initialized self.database_name
        """
        return "soda_test"

    def _create_schema_name(self) -> str | None:
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
            TestDataType.TEXT: "character varying",
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
        self.data_source.open_connection()
        if self.data_source.logs.has_errors():
            e = next((l.exception for l in reversed(self.data_source.logs.logs) if l.exception), None)
            raise AssertionError(f"Connection creation has errors: {self.data_source.logs}") from e

    def start_test_session_ensure_schema(self) -> None:
        if self.schema_name:
            if self.is_cicd:
                self.drop_test_schema_if_exists()
            self.create_test_schema_if_not_exists()

    def end_test_session(self, exception: Exception | None) -> None:
        self.end_test_session_drop_schema()
        self.end_test_session_close_connection()

    def end_test_session_close_connection(self) -> None:
        self.data_source.close_connection()

    def end_test_session_drop_schema(self) -> None:
        if self.schema_name:
            if self.is_cicd:
                self.drop_test_schema_if_exists()

    def query_existing_test_table_names(self):
        metadata_tables_query: MetadataTablesQuery = self.data_source.create_metadata_tables_query()
        fully_qualified_table_names: list[FullyQualifiedTableName] = metadata_tables_query.execute(
            database_name=self.database_name,
            schema_name=self.schema_name,
            include_table_name_like_filters=["SODATEST_%"],
        )
        return [
            fully_qualified_test_table_name.table_name
            for fully_qualified_test_table_name in fully_qualified_table_names
        ]

    def create_test_schema_if_not_exists(self) -> None:
        sql: str = self.create_test_schema_if_not_exists_sql()
        self.data_source.execute_update(sql)

    def create_test_schema_if_not_exists_sql(self) -> str:
        return f"CREATE SCHEMA IF NOT EXISTS {self.schema_name} AUTHORIZATION CURRENT_USER;"

    def drop_test_schema_if_exists(self) -> None:
        sql: str = self.drop_test_schema_if_exists_sql()
        self.data_source.execute_update(sql)

    def drop_test_schema_if_exists_sql(self) -> str:
        return f"DROP SCHEMA IF EXISTS {self.schema_name} CASCADE;"

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

                self.data_source.data_source_connection.commit()
        else:
            logger.debug(f"Test table {test_table.unique_name} already exists")

        return test_table

    def _create_test_table_python_object(self, test_table_specification: TestTableSpecification) -> TestTable:
        columns: list[TestColumn] = []
        for test_column_specification in test_table_specification.columns:
            contract_data_type = self.get_contract_data_type(test_column_specification.test_data_type)
            test_column: TestColumn = TestColumn(
                name=test_column_specification.name,
                test_data_type=self.get_create_table_sql_type(test_column_specification.test_data_type),
                data_type=contract_data_type
            )
            columns.append(test_column)

        sql_dialect = self.data_source.sql_dialect

        return TestTable(
            data_source_name=self.data_source.name,
            database_name=self.database_name,
            schema_name=self.schema_name,
            name=test_table_specification.unique_name,
            unique_name=test_table_specification.unique_name,
            qualified_name=sql_dialect.qualify_table(
                database_name=self.database_name,
                schema_name=self.schema_name,
                table_name=test_table_specification.unique_name
            ),
            columns=columns,
            row_values=test_table_specification.row_values
        )

    def _create_and_insert_test_table(self, test_table: TestTable) -> None:
        self._create_test_table(test_table)
        self._insert_test_table_rows(test_table)

    def _create_test_table(self, test_table: TestTable) -> None:
        sql: str = self._create_test_table_sql(test_table)
        self.data_source.execute_update(sql)

    def _create_test_table_sql(self, test_table: TestTable) -> str:
        test_table_name_qualified_quoted: str = self.data_source.sql_dialect.qualify_table(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=test_table.unique_name
        )
        columns_sql: str = ",\n".join(
            [
                f"  {column.name} {column.data_type}"
                for column in test_table.columns.values()
            ]
        )
        return self._create_test_table_sql_statement(test_table_name_qualified_quoted, columns_sql)

    def _create_test_table_sql_statement(self, table_name_qualified_quoted: str, columns_sql: str) -> str:
        return f"CREATE TABLE {table_name_qualified_quoted} ( \n{columns_sql} \n);"

    def _insert_test_table_rows(self, test_table: TestTable) -> None:
        sql: str = self._insert_test_table_rows_sql(test_table)
        if sql:
            self.data_source.execute_update(sql)

    def _insert_test_table_rows_sql(self, test_table: TestTable) -> str:
        if test_table.row_values:
            def literalize_row(row: tuple) -> list[str]:
                return [
                    self.data_source.sql_dialect.literal(value)
                    for value in row
                ]

            literal_row_values = [
                literalize_row(row_values)
                for row_values in test_table.row_values
            ]

            table_name_qualified_quoted = self.data_source.sql_dialect.qualify_table(
                database_name=self.database_name,
                schema_name=self.schema_name,
                table_name=test_table.unique_name
            )

            def format_literal_row_values(row: list[str]) -> str:
                return ",".join(row)

            rows_sql = ",\n".join(
                [
                    f"  ({format_literal_row_values(row)})" for row in literal_row_values
                ]
            )

            return self._insert_test_table_rows_sql_statement(table_name_qualified_quoted, rows_sql)

    def _insert_test_table_rows_sql_statement(self, table_name_qualified_quoted, rows_sql):
        return f"INSERT INTO {table_name_qualified_quoted} VALUES \n" f"{rows_sql};"

    def _drop_test_table(self, table_name: str) -> None:
        sql: str = self._drop_test_table_sql(table_name)
        self.data_source.execute_update(sql)

    def _drop_test_table_sql(self, table_name) -> str:
        table_name_qualified_quoted: str = self.data_source.sql_dialect.qualify_table(
            database_name=self.database_name,
            schema_name=self.schema_name,
            table_name=table_name
        )

        return self._drop_test_table_sql_statement(table_name_qualified_quoted)

    def _drop_test_table_sql_statement(self, table_name_qualified_quoted: str) -> str:
        return f"DROP TABLE {table_name_qualified_quoted};"

    def get_parse_errors_str(self, contract_yaml_str: str) -> str:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        contract_verification_builder = ContractVerification.builder().with_contract_yaml_str(
            contract_yaml_str=contract_yaml_str
        )
        contract_verification = contract_verification_builder.build()
        errors: list[Log] = contract_verification.logs.get_errors()
        return "\n".join([str(e) for e in errors])

    def assert_contract_pass(
        self, test_table: TestTable, contract_yaml_str: str, variables: dict[str, str] | None = None
    ) -> ContractResult:
        full_contract_yaml_str: str = self._build_full_contract_yaml_str(
            test_table=test_table, unique_table_name=test_table.unique_name, contract_yaml_str=contract_yaml_str
        )
        contract_verification_result: ContractVerificationResult = (
            self.create_test_verification_builder()
            .with_contract_yaml_str(full_contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        if contract_verification_result.failed():
            raise AssertionError(f"Expected contract verification passed, but was: {contract_verification_result}")
        logging.debug(f"Contract result: {contract_verification_result}")
        return contract_verification_result.contract_results[0]

    def assert_contract_fail(
        self, test_table: TestTable, contract_yaml_str: str, variables: dict[str, str] | None = None
    ) -> ContractResult:
        unique_table_name: str = self.ensure_test_table(test_table)
        full_contract_yaml_str: str = self._build_full_contract_yaml_str(
            test_table=test_table, unique_table_name=unique_table_name, contract_yaml_str=contract_yaml_str
        )
        logging.debug(full_contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            self.create_test_verification_builder()
            .with_contract_yaml_str(full_contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        if not contract_verification_result.failed():
            raise AssertionError(
                f"Expected contract verification failed, but got contract result: {contract_verification_result}"
            )
        logging.debug(f"Contract result: {contract_verification_result}")
        return contract_verification_result.contract_results[0]

    def assert_contract_error(
        self, contract_yaml_str: str, variables: dict[str, str] | None = None
    ) -> ContractVerificationResult:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            self.create_test_verification_builder()
            .with_contract_yaml_str(contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        logs_text = "\n".join([str(l) for l in contract_verification_result.logs.logs])
        if not contract_verification_result.has_errors():
            raise AssertionError(f"Expected contract execution errors, but got none. Logs:\n{logs_text}")
        contract_result_str = str(contract_verification_result)
        logging.debug(f"Contract result: {contract_result_str}")
        return contract_verification_result

    def create_test_verification_builder(self):
        return TestContractVerification.builder().with_data_source(self.data_source)

    def _build_full_contract_yaml_str(self, test_table: TestTable, unique_table_name: str, contract_yaml_str: str):
        header_lines: list[str] = [
            f"dataset: {unique_table_name}",
            f"data_source: {self.data_source.name}"
        ]
        if self.database_name:
            header_lines.append(f"database: {self.database_name}")
        if self.schema_name:
            header_lines.append(f"schema: {self.schema_name}")
        header_contract_yaml_str = "\n".join(header_lines)
        checks_contract_yaml_str = dedent(contract_yaml_str).strip()
        return f"{header_contract_yaml_str}\n{checks_contract_yaml_str}"
