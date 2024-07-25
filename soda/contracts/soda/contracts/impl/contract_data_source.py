from __future__ import annotations

import importlib
import logging
import re
from abc import ABC, abstractmethod


import soda.common.logs as soda_common_logs
from soda.contracts.impl.sodacl_log_converter import SodaClLogConverter
from soda.execution.data_source import DataSource as SodaCLDataSource

from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlFile, YamlHelper

logger = logging.getLogger(__name__)


class ContractDataSource(ABC):
    """
    Represents the configurations to create a connection. Usually it's loaded from a YAML file.
    """

    __KEY_TYPE = "type"
    __KEY_NAME = "name"
    __KEY_CONNECTION = "connection"
    __KEY_SPARK_SESSION = "spark_session"

    @classmethod
    def from_yaml_file(cls, data_source_yaml_file: YamlFile) -> ContractDataSource:
        data_source_type: str = data_source_yaml_file.dict.get("type")
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
        return f"{cls._camel_case_data_source_type(data_source_type)}DataSource"

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

    @classmethod
    def from_spark_session(cls, data_source_yaml_file: YamlFile, spark_session: object) -> ContractDataSource:
        data_source_yaml_dict: dict = data_source_yaml_file.dict
        data_source_yaml_dict[cls.__KEY_TYPE] = "spark_df"
        data_source_yaml_dict.setdefault(cls.__KEY_NAME, "spark")
        data_source_yaml_dict[cls.__KEY_SPARK_SESSION] = spark_session
        data_source_yaml_dict[cls.__KEY_CONNECTION] = None
        return SparkSessionClContractDataSource(data_source_yaml_file=data_source_yaml_file)

    def __init__(self, data_source_yaml_file: YamlFile):
        self.logs: Logs = data_source_yaml_file.logs

        # only initialized after the .open() method is called
        self.name: str | None = None
        # only initialized after the .open() method is called
        self.type: str | None = None

        self.data_source_yaml_file: YamlFile = data_source_yaml_file
        self.data_source_yaml_dict: dict = data_source_yaml_file.dict

        yaml_helper: yaml_helper = YamlHelper(yaml_file=data_source_yaml_file, logs=self.logs)
        self.type = yaml_helper.read_string(self.data_source_yaml_dict, self.__KEY_TYPE)
        self.name = yaml_helper.read_string(self.data_source_yaml_dict, self.__KEY_NAME)

        if isinstance(self.name, str) and not re.match("[_a-z0-9]+", self.name):
            self.logs.error(f"Data source name must contain only lower case letters, numbers and underscores.  Was {self.name}")

        self.connection_yaml_dict: dict | None = yaml_helper.read_dict(self.data_source_yaml_dict, self.__KEY_CONNECTION)

        # only initialized after the .open() method is called
        self.connection: object | None = None

        # Only if the open method has opened a connection should the close method close the connection
        # In the test suite this is used and later this may be used when passing in a user defined DBAPI connection
        self.close_connection: bool = False

    def __enter__(self) -> ContractDataSource:
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Could not close connection: {e}")

    def __str__(self) -> str:
        return self.name

    def open(self) -> None:
        if self.connection is None:
            try:
                self.connection = self._create_connection(self.connection_yaml_dict)
                self.close_connection = True
            except Exception as e:
                self.logs.error(f"Could not connect to '{self.name}': {e}")

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

    def close(self) -> None:
        """
        Closes te connection. This method will not throw any exceptions.
        Check errors with has_errors or assert_no_errors.
        """
        if self.close_connection and self.connection:
            try:
                self.connection.close()
            except Exception as e:
                logger.warning(f"Could not close the DBAPI connection: {e}")
            finally:
                self.connection = None


class ClContractDataSource(ContractDataSource, ABC):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file=data_source_yaml_file)
        self.sodacl_data_source: SodaCLDataSource | None = None

    @abstractmethod
    def _create_sodacl_data_source(self,
                                   database_name: str | None,
                                   schema_name: str | None,
                                   sodacl_data_source_name: str,
                                   sodacl_logs: SodaClLogConverter,
                                   ) -> SodaCLDataSource:
        """
        Create SodaCL DataSource using the self.connection and sodacl_data_source_name
        """
        pass


class FileClContractDataSource(ClContractDataSource):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file=data_source_yaml_file)

    def _create_sodacl_data_source(self, sodacl_data_source_name: str) -> SodaCLDataSource:
        # consider translating postgres schema search_path option
        # options = f"-c search_path={schema}" if schema else None
        try:
            return SodaCLDataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=sodacl_data_source_name,
                data_source_type=self.type,
                data_source_properties=self.connection_yaml_dict,
            )
        except Exception as e:
            self.logs.error(message=f"Could not create the data source: {e}", exception=e)


class SparkSessionClContractDataSource(ClContractDataSource):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file)
        self.spark_session: object = self.data_source_yaml_dict[self.__KEY_SPARK_SESSION]

    def _create_sodacl_data_source(self, sodacl_data_source_name: str) -> SodaCLDataSource:
        try:
            sodacl_data_source_properties = self.connection_yaml_dict.copy()
            sodacl_data_source_properties["spark_session"] = self.spark_session
            return SodaCLDataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=sodacl_data_source_name,
                data_source_type=self.type,
                data_source_properties=sodacl_data_source_properties
            )
        except Exception as e:
            self.logs.error(message=f"Could not create the spark session data source: {e}", exception=e)
