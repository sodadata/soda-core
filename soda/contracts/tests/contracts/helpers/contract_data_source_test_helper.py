from __future__ import annotations

import logging
import os
import random
import re
import string
from importlib import import_module
from io import StringIO
from textwrap import dedent

from helpers.test_column import TestColumn
from helpers.test_table import TestTable
from ruamel.yaml import YAML, round_trip_dump

from soda.contracts.contract import ContractResult
from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationBuilder,
    ContractVerificationResult,
)
from soda.contracts.impl.contract_data_source import ContractDataSource
from soda.contracts.impl.logs import Log, Logs
from soda.contracts.impl.sql_dialect import SqlDialect
from soda.contracts.impl.yaml_helper import YamlFile

logger = logging.getLogger(__name__)


class TestContractVerificationBuilder(ContractVerificationBuilder):
    __test__ = False

    def __init__(self):
        super().__init__()
        self.data_source = None

    def with_data_source(self, data_source: ContractDataSource) -> TestContractVerificationBuilder:
        self.data_source = data_source
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


class ContractDataSourceTestHelper:

    @classmethod
    def create(cls) -> ContractDataSourceTestHelper:
        test_data_source_type = os.getenv("test_data_source", "postgres")
        camel_case_data_source_type = ContractDataSource._camel_case_data_source_type(test_data_source_type)
        class_name = f"{camel_case_data_source_type}ContractDataSourceTestHelper"
        try:
            module_name = import_module(f"{test_data_source_type}_contract_data_source_test_helper")
            class_ = getattr(module_name, class_name)
            return class_()
        except Exception as e:
            raise RuntimeError(f"Couldn't instantiate {class_name}: {e}") from e

    def __init__(self):
        super().__init__()
        self.database_name: str = self._create_database_name()
        self.schema_name: str = self._create_schema_name()
        self.contract_data_source: ContractDataSource = self._create_contract_data_source(
            database_name=self.database_name,
            schema_name=self.schema_name,
        )
        if self.contract_data_source.logs.has_errors():
            raise RuntimeError(f"Couldn't create ContractDataSource: {self.contract_data_source.logs}")
        self.existing_test_table_names: list[str] | None = None
        self.is_cicd = os.getenv("GITHUB_ACTIONS") is not None

    def _create_contract_data_source(self, database_name: str | None, schema_name: str | None) -> ContractDataSource:
        """
        Called in constructor to initialized self.contract_data_source
        """
        logs: Logs = Logs()
        test_data_source_yaml_dict = self._create_contract_data_source_yaml_dict(
            database_name=database_name,
            schema_name=schema_name,
        )
        data_source_yaml_file = YamlFile(yaml_dict=test_data_source_yaml_dict, logs=logs)
        data_source_yaml_file.parse({})
        data_source = ContractDataSource.from_yaml_file(data_source_yaml_file=data_source_yaml_file)
        assert not logs.has_errors()
        return data_source

    def _create_contract_data_source_yaml_dict(self, database_name: str | None, schema_name: str | None) -> dict:
        """
        Called in _create_contract_data_source to initialized self.contract_data_source
        """
        return {}

    def _create_database_name(self) -> str | None:
        """
        Called in constructor to initialized self.database_name
        """
        return "sodasql"

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

    def start_test_session(self) -> None:
        self.start_test_session_open_connection()
        self.start_test_session_ensure_schema()

    def start_test_session_open_connection(self) -> None:
        self.contract_data_source.open_connection()
        self.contract_data_source.disable_close_connection()
        if self.contract_data_source.logs.has_errors():
            e = next((l.exception for l in reversed(self.contract_data_source.logs.logs) if l.exception), None)
            raise AssertionError(f"Connection creation has errors: {self.contract_data_source.logs}") from e

    def start_test_session_ensure_schema(self) -> None:
        if self.schema_name:
            if self.is_cicd:
                self.drop_test_schema_if_exists(database_name=self.database_name, schema_name=self.schema_name)
            self.create_test_schema_if_not_exists(database_name=self.database_name, schema_name=self.schema_name)

    def end_test_session(self, exception: Exception | None) -> None:
        self.end_test_session_drop_schema()
        self.end_test_session_close_connection()

    def end_test_session_close_connection(self) -> None:
        self.contract_data_source.enable_close_connection()
        self.contract_data_source.close_connection()

    def end_test_session_drop_schema(self) -> None:
        if self.schema_name:
            if self.is_cicd:
                self.drop_test_schema_if_exists(database_name=self.database_name, schema_name=self.schema_name)

    def ensure_test_table(self, test_table: TestTable) -> str:
        """
        Returns a unique test table name with the given table data
        """
        if self.existing_test_table_names is None:
            self.existing_test_table_names = self.contract_data_source.select_existing_test_table_names(
                database_name=self.database_name, schema_name=self.schema_name
            )
        existing_test_table_names_lower = [table_name.lower() for table_name in self.existing_test_table_names]
        if test_table.unique_table_name.lower() not in existing_test_table_names_lower:
            obsolete_table_names = [
                existing_test_table
                for existing_test_table in self.existing_test_table_names
                if existing_test_table.lower().startswith(f"sodatest_{test_table.name.lower()}_")
            ]
            if obsolete_table_names:
                for obsolete_table_name in obsolete_table_names:
                    logger.debug(f"Test table {obsolete_table_name} has changed and will be recreated")
                    self.drop_test_table(self.database_name, self.schema_name, obsolete_table_name)
                    self.existing_test_table_names.remove(obsolete_table_name)
            logger.debug(f"Test table {test_table.unique_table_name} will be created")
            self.create_and_insert_test_table(
                database_name=self.database_name, schema_name=self.schema_name, test_table=test_table
            )
            self.existing_test_table_names.append(test_table.unique_table_name)
            self.contract_data_source.commit()
        else:
            logger.debug(f"Test table {test_table.unique_table_name} already exists")
        return test_table.unique_view_name if test_table.create_view else test_table.unique_table_name

    def drop_test_schema_if_exists(self, database_name: str, schema_name: str) -> None:
        ds = self.contract_data_source
        drop_schema_if_exists_sql = ds.sql_dialect.stmt_drop_schema_if_exists(
            database_name=database_name, schema_name=schema_name
        )
        ds._execute_sql_update(drop_schema_if_exists_sql)

    def create_test_schema_if_not_exists(self, database_name: str, schema_name: str) -> None:
        ds = self.contract_data_source
        create_schema_if_not_exists_sql = ds.sql_dialect.stmt_create_schema_if_not_exists(database_name, schema_name)
        ds._execute_sql_update(create_schema_if_not_exists_sql)

    def drop_test_table(self, database_name: str, schema_name: str, table_name: str) -> None:
        ds = self.contract_data_source
        sql = ds.sql_dialect.stmt_drop_test_table(
            database_name=database_name, schema_name=schema_name, table_name=table_name
        )
        ds._execute_sql_update(sql)

    def create_and_insert_test_table(
        self, database_name: str | None, schema_name: str | None, test_table: TestTable
    ) -> None:
        ds = self.contract_data_source
        create_table_sql = self._stmt_create_test_table(
            database_name=database_name, schema_name=schema_name, test_table=test_table
        )
        ds._execute_sql_update(create_table_sql)
        insert_table_sql = self._insert_test_table_sql(
            database_name=database_name, schema_name=schema_name, test_table=test_table
        )
        if insert_table_sql:
            ds._execute_sql_update(insert_table_sql)

    def get_parse_errors_str(self, contract_yaml_str: str) -> str:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        contract_verification_builder = (ContractVerification.builder()
            .with_contract_yaml_str(
                contract_yaml_str=contract_yaml_str
            )
        )
        contract_verification = contract_verification_builder.build()
        errors: list[Log] = contract_verification.logs.get_errors()
        return "\n".join([str(e) for e in errors])

    def assert_contract_pass(
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
        return TestContractVerification.builder().with_data_source(self.contract_data_source)

    def _build_full_contract_yaml_str(self, test_table: TestTable, unique_table_name: str, contract_yaml_str: str):
        header_lines: list[str] = [f"dataset: {unique_table_name}", f"data_source: {self.contract_data_source.name}"]
        if self.database_name:
            header_lines.append(f"database: {self.database_name}")
        if self.schema_name:
            header_lines.append(f"schema: {self.schema_name}")
        header_contract_yaml_str = "\n".join(header_lines)
        checks_contract_yaml_str = dedent(contract_yaml_str).strip()

        checks_contract_yaml_str = self.casify_contract_yaml_str(
            test_table=test_table, contract_yaml_str=checks_contract_yaml_str
        )

        return f"{header_contract_yaml_str}\n{checks_contract_yaml_str}"

    def casify_contract_yaml_str(self, test_table, contract_yaml_str):
        if not test_table.quote_names:
            sql_dialect: SqlDialect = self.contract_data_source.sql_dialect
            ruamel_yaml: YAML = YAML()
            ruamel_yaml.preserve_quotes = True
            contract_content_dict: dict = ruamel_yaml.load(contract_yaml_str)
            for column_yaml_dict in contract_content_dict["columns"]:
                original_column_name: str = column_yaml_dict["name"]
                actual_column_name: str = sql_dialect.default_casify(original_column_name)
                column_yaml_dict["name"] = actual_column_name
                original_column_data_type: str | None = column_yaml_dict.get("data_type")
                if isinstance(original_column_data_type, str):
                    actual_column_data_type: str | None = sql_dialect.get_schema_check_sql_type(
                        original_column_data_type
                    )
                    column_yaml_dict["data_type"] = (
                        actual_column_data_type if actual_column_data_type else original_column_data_type
                    )
            stream = StringIO()
            round_trip_dump(contract_content_dict, stream=stream)
            contract_yaml_str = stream.getvalue()
        return contract_yaml_str

    def _stmt_create_test_table(self, database_name: str | None, schema_name: str | None, test_table: TestTable) -> str:
        sql_dialect: SqlDialect = self.contract_data_source.sql_dialect
        table_name_qualified_quoted = sql_dialect.qualify_table(
            database_name=database_name,
            schema_name=schema_name,
            table_name=test_table.unique_table_name,
            quote_table_name=test_table.quote_names,
        )

        test_columns = test_table.test_columns
        if test_table.quote_names:
            test_columns = [
                TestColumn(name=sql_dialect.quote_default(test_column.name), data_type=test_column.data_type)
                for test_column in test_columns
            ]

        columns_sql = ",\n".join(
            [
                f"  {test_column.name} {self.get_create_table_sql_type(test_column.data_type)}"
                for test_column in test_columns
            ]
        )
        return self.compose_create_table_statement(table_name_qualified_quoted, columns_sql)

    def compose_create_table_statement(self, qualified_table_name, columns_sql) -> str:
        return f"CREATE TABLE {qualified_table_name} ( \n{columns_sql} \n)"

    def get_create_table_sql_type(self, data_type: str) -> str:
        sql_dialect: SqlDialect = self.contract_data_source.sql_dialect
        column_declaration_data_type: str = sql_dialect.create_table_sql_type_dict.get(data_type)
        assert isinstance(column_declaration_data_type, str) and len(column_declaration_data_type) > 0
        return column_declaration_data_type

    def _insert_test_table_sql(self, database_name: str | None, schema_name: str | None, test_table: TestTable) -> str:
        if test_table.values:
            sql_dialect = self.contract_data_source.sql_dialect
            table_name_qualified_quoted = sql_dialect.qualify_table(
                database_name=database_name,
                schema_name=schema_name,
                table_name=test_table.unique_table_name,
                quote_table_name=test_table.quote_names,
            )

            def sql_test_table_row(row):
                return ",".join([sql_dialect.literal(value) for value in row])

            rows_sql = ",\n".join([f"  ({sql_test_table_row(row)})" for row in test_table.values])
            return f"INSERT INTO {table_name_qualified_quoted} VALUES \n" f"{rows_sql};"
