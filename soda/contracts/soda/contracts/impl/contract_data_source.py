from __future__ import annotations

import importlib
import logging
import re
import textwrap
from abc import ABC, abstractmethod

from soda.contracts.impl.logs import Logs
from soda.contracts.impl.sodacl_log_converter import SodaClLogConverter
from soda.contracts.impl.sql_dialect import SqlDialect
from soda.contracts.impl.yaml_helper import YamlFile, YamlHelper
from soda.execution.data_source import DataSource as SodaCLDataSource

logger = logging.getLogger(__name__)


class ContractDataSource(ABC):
    """
    Takes configuration as input that is usually loaded from a data source YAML file.
    Is responsible for creating the connection in a with-block.  (see __enter__ and __exit__)
    Exposes methods to the rest of the engine that encapsulate interaction with the SQL connection.
    This class uses self.sql_dialect to build the SQL statement strings.
    """

    _KEY_TYPE = "type"
    _KEY_NAME = "name"
    _KEY_CONNECTION = "connection"
    _KEY_SPARK_SESSION = "spark_session"

    @classmethod
    def from_yaml_file(cls, data_source_yaml_file: YamlFile) -> ContractDataSource:
        data_source_type: str = data_source_yaml_file.get_dict().get("type")
        data_source_module_name: str = cls._create_data_source_module_name(data_source_type)
        data_source_class_name: str = cls._create_data_source_class_name(data_source_type)
        # dynamically load contracts data source
        try:
            module = importlib.import_module(f"soda.data_sources.{data_source_module_name}_contract_data_source")
            class_ = getattr(module, data_source_class_name)
            return class_(data_source_yaml_file)
        except ModuleNotFoundError as e:
            data_source_yaml_file.logs.error(
                f"Did you install the right package dependency for data source type {data_source_type}? \n"
                f"Could not instantiate contract data source {data_source_class_name} \n"
                f"{e}"
            )
            return None

    @classmethod
    def _create_data_source_module_name(cls, data_source_type: str) -> str:
        return data_source_type

    @classmethod
    def _create_data_source_class_name(cls, data_source_type: str) -> str:
        return f"{cls._camel_case_data_source_type(data_source_type)}ContractDataSource"

    @classmethod
    def _camel_case_data_source_type(cls, test_data_source_type) -> str:
        data_source_camel_case: dict[str, str] = {
            "bigquery": "BigQuery",
            "spark_df": "SparkDf",
            "sqlserver": "SQLServer",
            "mysql": "MySQL",
            "duckdb": "DuckDB"
        }
        if test_data_source_type in data_source_camel_case:
            return data_source_camel_case[test_data_source_type]
        else:
            return f"{test_data_source_type[0:1].upper()}{test_data_source_type[1:]}"

    def __init__(self, data_source_yaml_file: YamlFile):
        self.logs: Logs = data_source_yaml_file.logs

        # only initialized after the .open() method is called
        self.name: str | None = None
        # only initialized after the .open() method is called
        self.type: str | None = None

        self.data_source_yaml_file: YamlFile = data_source_yaml_file
        self.data_source_yaml_dict: dict = data_source_yaml_file.get_dict()

        yaml_helper: yaml_helper = YamlHelper(yaml_file=data_source_yaml_file, logs=self.logs)
        self.type = yaml_helper.read_string(self.data_source_yaml_dict, self._KEY_TYPE)
        self.name = yaml_helper.read_string(self.data_source_yaml_dict, self._KEY_NAME)
        if isinstance(self.name, str) and not re.match("[_a-z0-9]+", self.name):
            self.logs.error(f"Data source name must contain only lower case letters, numbers and underscores.  Was {self.name}")

        self.sql_dialect: SqlDialect = self._create_sql_dialect()

        self.connection_yaml_dict: dict | None = yaml_helper.read_dict(self.data_source_yaml_dict, self._KEY_CONNECTION)

        # only initialized after the .open() method is called
        self.connection: object | None = None

        # Only if the open method has opened a connection should the close method close the connection
        # In the test suite this is used and later this may be used when passing in a user defined DBAPI connection
        self.close_connection_enabled: bool = False

        # The database name in the current connection context & current contract if applicable for this data source type
        # Updated in the ensure_connection method
        self.database_name: str | None = None
        # The schema name in the current connection context & current contract if applicable for this data source type
        # Updated in the ensure_connection method
        self.schema_name: str | None = None


    @abstractmethod
    def _create_sql_dialect(self) -> SqlDialect:
        raise NotImplementedError(f"{type(self).__name__} must override ContractDataSource._create_sql_dialect")

    def __str__(self) -> str:
        return self.name

    def open_connection(self) -> None:
        """
        Ensures that an open connection is available.
        After this method ends, the open connection must have the database_name and optionally
        the schema_name as context as far as these are applicable for the specific data source type.
        Potentially closes and re-opens the self.connection if the existing connection has a different database
        or schema context compared to the given database_name and schema_name for the next contract.
        """
        if self.connection is None:
            try:
                self.connection = self._create_connection(self.connection_yaml_dict)
                self.close_connection_enabled = True
            except Exception as e:
                self.logs.error(f"Could not connect to '{self.name}': {e}", exception=e)

    def set_contract_context(self, database_name: str | None, schema_name: str | None) -> None:
        """
        Ensures that an open connection is available.
        After this method ends, the open connection must have the database_name and optionally
        the schema_name as context as far as these are applicable for the specific data source type.
        Potentially closes and re-opens the self.connection if the existing connection has a different database
        or schema context compared to the given database_name and schema_name for the next contract.
        """
        self.database_name = database_name
        self.schema_name = schema_name

    @abstractmethod
    def _create_connection(self, connection_yaml_dict: dict) -> object:
        """
        self.connection_yaml_dict is provided as a parameter for convenience.
        Returns a new DBAPI connection based on the provided connection properties.
        The returned value will be saved in self.connection. See open() for details.
        Exceptions do not need to be handled.  They will be handled by the calling method.
        """
        pass

    def _log_connection_properties_excl_pwd(self, data_source_type: str, connection_yaml_dict: dict):
        dict_without_pwd = {
            k:v for k,v in connection_yaml_dict.items()
            if not "password" in k
               and not "pwd" in k
               and not "key" in k
               and not "secret" in k
        }
        logger.debug(
            f"{data_source_type} connection properties: {dict_without_pwd}"
        )

    def _create_sodacl_data_source(self,
                                   sodacl_data_source_name: str,
                                   ) -> SodaCLDataSource:
        """
        Create SodaCL DataSource using the self.connection and sodacl_data_source_name
        """
        try:
            data_source_properties = self.connection_yaml_dict.copy()
            data_source_properties["database"] = self.database_name
            data_source_properties["schema"] = self.schema_name
            sodacl_data_source: SodaCLDataSource = SodaCLDataSource.create(
                logs=SodaClLogConverter(self.logs),
                data_source_name=sodacl_data_source_name,
                data_source_type=self.type,
                data_source_properties=data_source_properties,
            )
            sodacl_data_source.connection = self.connection
            return sodacl_data_source

        except Exception as e:
            self.logs.error(message=f"Could not create the data source: {e}", exception=e)

    def close_connection(self) -> None:
        """
        Closes te connection. This method will not throw any exceptions.
        Check errors with has_errors or assert_no_errors.
        """
        if self.close_connection_enabled and self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Could not close the DBAPI connection: {e}")
            finally:
                self.connection = None

    def select_existing_test_table_names(self, database_name: str | None, schema_name: str | None) -> list[str]:
        sql = self.sql_dialect.stmt_select_table_names(
            database_name=database_name,
            schema_name=schema_name,
            table_name_like_filter="sodatest_%"
        )
        rows = self._execute_sql_fetch_all(sql)
        return [row[0] for row in rows]


    def _execute_sql_fetch_all(self, sql: str) -> list[tuple]:
        cursor = self.connection.cursor()
        try:
            sql_indented = textwrap.indent(text=sql, prefix="  #   ")
            logger.debug(f"SQL query fetchall: \n{sql_indented}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            logger.debug(" | ".join([c[0] for c in cursor.description]))
            for row in rows:
                logger.debug(" | ".join([e for e in row]))
            return rows
        finally:
            cursor.close()

    def _execute_sql_update(self, sql: str) -> object:
        cursor = self.connection.cursor()
        try:
            sql_indented = textwrap.indent(text=sql, prefix="  # ")
            logger.debug(f"SQL update: \n{sql_indented}")
            updates = cursor.execute(sql)
            self.connection.commit()
            return updates
        finally:
            cursor.close()

    def commit(self):
        self.connection.commit()

    def disable_close_connection(self) -> None:
        self.close_connection_enabled = False

    def enable_close_connection(self) -> None:
        self.close_connection_enabled = True


class ClContractDataSource(ContractDataSource, ABC):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file=data_source_yaml_file)
        self.sodacl_data_source: SodaCLDataSource | None = None


class FileClContractDataSource(ClContractDataSource):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file=data_source_yaml_file)
