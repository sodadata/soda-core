from __future__ import annotations

import logging
import os
import random
import re
import string
import textwrap
from importlib import import_module
from textwrap import dedent

from helpers.test_column import TestColumn
from helpers.test_table import TestTable
from soda.contracts.contract import ContractResult
from soda.contracts.contract_verification import (
    ContractVerification,
    ContractVerificationBuilder,
    ContractVerificationResult,
)
from soda.contracts.impl.contract_data_source import ContractDataSource
from soda.contracts.impl.logs import Logs
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
        module_name = import_module(f"{test_data_source_type}_contract_data_source_test_helper")
        class_ = getattr(module_name, class_name)
        return class_()

    def __init__(self):
        super().__init__()
        self.data_source: ContractDataSource = self._create_data_source()
        self.database_name: str = self._get_database_name()
        self.schema_name: str = self._get_schema_name()
        self.__existing_table_names: list[str] | None = None
        self.is_cicd = os.getenv("GITHUB_ACTIONS") is not None

    def _create_data_source(self) -> ContractDataSource:
        logs: Logs = Logs()
        test_data_source_yaml_dict = self._create_contract_data_source_yaml_dict()
        data_source_yaml_file = YamlFile(yaml_dict=test_data_source_yaml_dict, logs=logs)
        data_source = ContractDataSource.from_yaml_file(data_source_yaml_file=data_source_yaml_file)
        assert not logs.has_errors()
        return data_source

    def _create_contract_data_source_yaml_dict(self) -> dict:
        return  {}

    def _get_database_name(self) -> str:
        return "soda_test_db"

    def _get_schema_name(self):
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

    def __enter__(self) -> ContractDataSourceTestHelper:
        self.data_source.__enter__()
        self._ensure_schema()
        return self

    def _ensure_schema(self):
        self._drop_schema_if_exists()
        self._create_schema_if_not_exists()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.data_source.__exit__(exc_type, exc_val, exc_tb)
        if self.is_cicd:
            self._drop_schema_if_exists()

    def _drop_schema_if_exists(self):
        drop_schema_if_exists_sql = self._drop_schema_if_exists_sql()
        self._update(drop_schema_if_exists_sql, self.data_source.connection)

    def _drop_schema_if_exists_sql(self) -> str:
        return f"DROP DATABASE {self.schema_name}"

    def _create_schema_if_not_exists(self):
        create_schema_if_not_exists_sql = self._create_schema_if_not_exists_sql()
        self._update(create_schema_if_not_exists_sql, self.schema_data_source.connection)

    def _create_schema_if_not_exists_sql(self) -> str:
        return f"CREATE DATABASE {self.schema_name}"

    def ensure_test_table(self, test_table: TestTable) -> str:
        """
        Returns a unique test table name with the given table data
        """
        existing_test_table_names = self._get_existing_test_table_names()
        existing_test_table_names_lower = [table_name.lower() for table_name in existing_test_table_names]
        if test_table.unique_table_name.lower() not in existing_test_table_names_lower:
            obsolete_table_names = [
                existing_test_table
                for existing_test_table in existing_test_table_names
                if existing_test_table.lower().startswith(f"sodatest_{test_table.name.lower()}_")
            ]
            if obsolete_table_names:
                for obsolete_table_name in obsolete_table_names:
                    self._drop_test_table(obsolete_table_name)
            self._create_and_insert_test_table(test_table)
            self.data_source.commit()

        return test_table.unique_view_name if test_table.create_view else test_table.unique_table_name

    def _get_existing_test_table_names(self):
        if self.__existing_table_names is None:
            sql = self.data_source.sql_find_table_names(filter="sodatest_%")
            rows = self._fetch_all(sql)
            table_names = [row[0] for row in rows]
            self.__existing_table_names.set(table_names)
        return self.__existing_table_names.get()

    def _create_and_insert_test_table(self, test_table: TestTable):
        create_table_sql = self._create_test_table_sql(test_table)
        self._update(create_table_sql)
        self._get_existing_test_table_names().append(test_table.unique_table_name)
        insert_table_sql = self._insert_test_table_sql(test_table)
        if insert_table_sql:
            self._update(insert_table_sql)
        if test_table.create_view:
            self._update(self._create_view_from_table_sql(test_table))

    def _create_test_table_sql(self, test_table: TestTable) -> str:
        table_name = test_table.unique_table_name
        if test_table.quote_names:
            table_name = self.data_source.quote_table_declaration(table_name)
        qualified_table_name = self.data_source.qualified_table_name(table_name)
        test_columns = test_table.test_columns
        if test_table.quote_names:
            test_columns = [
                TestColumn(
                    name=self.data_source.quote_column_declaration(test_column.name),
                    data_type=test_column.data_type
                )
                for test_column in test_columns
            ]
        columns_sql = ",\n".join(
            [
                f"  {test_column.name} {self.data_source.get_sql_type_for_create_table(test_column.data_type)}"
                for test_column in test_columns
            ]
        )
        return self._create_test_table_sql_compose(qualified_table_name, columns_sql)

    def _create_view_from_table_sql(self, test_table: TestTable):
        return f"CREATE VIEW {test_table.unique_view_name} AS SELECT * FROM {test_table.unique_table_name}"

    def _create_test_table_sql_compose(self, qualified_table_name, columns_sql) -> str:
        return f"CREATE TABLE {qualified_table_name} ( \n{columns_sql} \n)"

    def _insert_test_table_sql(self, test_table: TestTable) -> str:
        if test_table.values:
            quoted_table_name = (
                self.data_source.quote_table(test_table.unique_table_name)
                if test_table.quote_names
                else test_table.unique_table_name
            )
            qualified_table_name = self.data_source.qualified_table_name(quoted_table_name)

            def sql_test_table_row(row):
                return ",".join(self.data_source.literal(value) for value in row)

            rows_sql = ",\n".join([f"  ({sql_test_table_row(row)})" for row in test_table.values])
            return f"INSERT INTO {qualified_table_name} VALUES \n" f"{rows_sql};"

    def _drop_test_table(self, table_name):
        drop_test_table_sql = self._drop_test_table_sql(table_name)
        self._update(drop_test_table_sql)
        self._get_existing_test_table_names().remove(table_name)

    def _drop_test_table_sql(self, table_name):
        qualified_table_name = self.data_source.qualified_table_name(table_name)
        return f"DROP TABLE IF EXISTS {qualified_table_name}"

    def _fetch_all(self, sql: str, connection=None) -> list[tuple]:
        if connection is None:
            connection = self.data_source.connection
        cursor = connection.cursor()
        try:
            sql_indented = textwrap.indent(text=sql, prefix="  #   ")
            logger.debug(f"  # Test data handler fetchall: \n{sql_indented}")
            cursor.execute(sql)
            return cursor.fetchall()
        finally:
            cursor.close()

    def _update(self, sql: str, connection=None) -> object:
        if connection is None:
            connection = self.data_source.connection
        cursor = connection.cursor()
        try:
            sql_indented = textwrap.indent(text=sql, prefix="  #   ")
            logger.debug(f"  # Test data handler update: \n{sql_indented}")
            updates = cursor.execute(sql)
            connection.commit()
            return updates
        finally:
            cursor.close()

    def assert_contract_pass(self, contract_yaml_str: str, variables: dict[str, str] | None = None) -> ContractResult:
        contract_yaml_str = dedent(contract_yaml_str)
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            TestContractVerification.builder()
            .with_data_source(self.data_source)
            .with_contract_yaml_str(contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        if contract_verification_result.failed():
            raise AssertionError(f"Expected contract verification passed, but was: {contract_verification_result}")
        logging.debug(f"Contract result: {contract_verification_result}")
        return contract_verification_result.contract_results[0]

    def assert_contract_fail(self, contract_yaml_str: str, variables: dict[str, str] | None = None) -> ContractResult:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            TestContractVerification.builder()
            .with_data_source(self.data_source)
            .with_contract_yaml_str(contract_yaml_str)
            .with_variables(variables)
            .execute()
        )
        if not contract_verification_result.failed():
            raise AssertionError(
                f"Expected contract verification failed, but got contract result: {contract_verification_result}"
            )
        logging.debug(f"Contract result: {contract_verification_result}")
        return contract_verification_result.contract_results[0]

        # except SodaException as e:
        #     assert e.contract_result
        #     if e.contract_result.has_execution_errors():
        #         raise AssertionError(str(e.contract_result))
        #     contract_result = e.contract_result

    def assert_contract_error(
        self, contract_yaml_str: str, variables: dict[str, str] | None = None
    ) -> ContractVerificationResult:
        contract_yaml_str = dedent(contract_yaml_str).strip()
        logging.debug(contract_yaml_str)
        contract_verification_result: ContractVerificationResult = (
            TestContractVerification.builder()
            .with_data_source(self.data_source)
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
