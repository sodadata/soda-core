from __future__ import annotations

import logging
from abc import ABC, abstractmethod

import soda.common.logs as soda_common_logs
from soda.execution.data_source import DataSource as SodaCLDataSource

from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlFile, YamlHelper

logger = logging.getLogger(__name__)


class DataSource(ABC):
    """
    Represents the configurations to create a connection. Usually it's loaded from a YAML file.
    """

    def __init__(self, logs: Logs | None = None):
        self.logs: Logs = logs if logs else Logs()

        # only initialized after the .open() method is called
        self.dbapi_connection: object | None = None

        # Only if the open method has opened a connection should the close method close the connection
        # In the test suite this is used and later this may be used when passing in a user defined DBAPI connection
        self.close_connection: bool = False

        # only initialized after the .open() method is called
        self.data_source_name: str | None = None
        # only initialized after the .open() method is called
        self.data_source_type: str | None = None
        # only initialized after the .open() method is called
        self.database_name: str | None = None
        # only initialized after the .open() method is called
        self.schema_name: str | None = None

        self.data_source_yaml_dict: dict = {}

    @classmethod
    def from_yaml_file(cls, data_source_file: YamlFile) -> DataSource:
        return FileClDataSource(data_source_yaml_file=data_source_file)

    @classmethod
    def from_spark_session(cls, spark_configuration: SparkConfiguration) -> DataSource:
        return SparkSessionClDataSource(
            spark_session=spark_configuration.spark_session,
            data_source_yaml_dict=spark_configuration.data_source_yaml_dict,
            logs=spark_configuration.logs,
        )

    def __enter__(self) -> DataSource:
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Could not close connection: {e}")

    def __str__(self) -> str:
        return self.data_source_name

    def open(self) -> None:
        if self.dbapi_connection is None:
            self.dbapi_connection = self._create_dbapi_connection()
            self.close_connection = True

    @abstractmethod
    def _create_dbapi_connection(self) -> object:
        pass

    def close(self) -> None:
        """
        Closes te connection. This method will not throw any exceptions.
        Check errors with has_errors or assert_no_errors.
        """
        if self.close_connection and self.dbapi_connection:
            try:
                self.dbapi_connection.close()
            except Exception as e:
                logger.warning(f"Could not close the dbapi connection: {e}")

    def get_database_name(self) -> str:
        return self.database_name

    def get_schema_name(self) -> str:
        return self.schema_name


class ClDataSource(DataSource, ABC):

    def __init__(self, logs: Logs):
        super().__init__(logs)
        self.sodacl_data_source: SodaCLDataSource | None = None

    def _create_dbapi_connection(self) -> object:
        self.sodacl_data_source: SodaCLDataSource = self._create_sodacl_data_source()
        try:
            self.sodacl_data_source.connect()
        except Exception as e:
            self.logs.error(f"Could not connect to '{self.data_source_name}': {e}")
        return self.sodacl_data_source.connection

    @abstractmethod
    def _create_sodacl_data_source(self) -> SodaCLDataSource:
        pass


class FileClDataSource(ClDataSource):

    def __init__(self, data_source_yaml_file: YamlFile):
        super().__init__(data_source_yaml_file.logs)
        self.data_source_file: YamlFile = data_source_yaml_file
        self.data_source_yaml_dict: dict = data_source_yaml_file.dict
        self.connection_dict: dict | None = None

        if self.data_source_file.is_ok():
            yaml_helper: yaml_helper = YamlHelper(yaml_file=self.data_source_file, logs=self.logs)
            self.data_source_type = yaml_helper.read_string(self.data_source_yaml_dict, "type")
            self.data_source_name = yaml_helper.read_string(self.data_source_yaml_dict, "name")
            self.connection_dict: dict = yaml_helper.read_dict(self.data_source_yaml_dict, "connection")

    def _create_sodacl_data_source(self) -> DataSource:
        # consider translating postgres schema search_path option
        # options = f"-c search_path={schema}" if schema else None
        try:
            return SodaCLDataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=self.data_source_name,
                data_source_type=self.data_source_type,
                data_source_properties=self.connection_dict,
            )
        except Exception as e:
            self.logs.error(message=f"Could not create the data source: {e}", exception=e)


class SparkConfiguration:
    def __init__(self, spark_session: object, data_source_yaml_dict: dict, logs: Logs):
        self.spark_session: object = spark_session
        self.data_source_yaml_dict: dict = data_source_yaml_dict
        self.logs: Logs = logs


class SparkSessionClDataSource(ClDataSource):

    def __init__(self, spark_session: object, data_source_yaml_dict: dict, logs: Logs):
        super().__init__(logs)
        self.spark_session: object = spark_session
        self.data_source_yaml_dict: dict = data_source_yaml_dict
        self.data_source_name = data_source_yaml_dict.get("name", "spark_ds")
        self.data_source_type = "spark_df"

    def _create_sodacl_data_source(self) -> DataSource:
        try:
            return SodaCLDataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=self.data_source_name,
                data_source_type=self.data_source_type,
                data_source_properties={"spark_session": self.spark_session},
            )
        except Exception as e:
            self.logs.error(message=f"Could not create the spark session data source: {e}", exception=e)
