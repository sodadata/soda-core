from __future__ import annotations

import logging
from abc import abstractmethod, ABC

import soda.common.logs as soda_common_logs
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlHelper, YamlFile
from soda.execution.data_source import DataSource as SodaCLDataSource


logger = logging.getLogger(__name__)


class DataSource:

    """
    Represents the configurations to create a connection. Usually it's loaded from a YAML file.
    """

    def __init__(self,
                 logs: Logs | None = None,
                 data_source_yaml_file: YamlFile | None = None,
                 spark_session: object | None = None,
                 ):
        self.logs: Logs = logs if logs else Logs()
        self.variables: dict[str, str] = {}
        self.data_source_file: YamlFile = data_source_yaml_file
        self.spark_session: object | None = spark_session

        # only initialized after the .open() method is called
        self.dbapi_connection: object | None = None
        # only initialized after the .open() method is called
        self.data_source_name: str | None = None
        # only initialized after the .open() method is called
        self.data_source_type: str | None = None

    @classmethod
    def from_yaml_file(cls, data_source_yaml_file_path: str, logs: Logs | None = None) -> DataSource:
        assert isinstance(data_source_yaml_file_path, str) and len(data_source_yaml_file_path) > 1
        return FileClDataSource(
            data_source_yaml_file=YamlFile(yaml_file_path=data_source_yaml_file_path, logs=logs),
            logs=logs
        )

    @classmethod
    def from_yaml_str(cls, data_source_yaml_str: str, logs: Logs | None = None) -> DataSource:
        assert isinstance(data_source_yaml_str, str) and len(data_source_yaml_str) > 1
        return FileClDataSource(
            data_source_yaml_file=YamlFile(yaml_str=data_source_yaml_str, logs=logs),
            logs=logs
        )

    @classmethod
    def from_yaml_dict(cls, data_source_yaml_dict: dict, logs: Logs | None = None) -> DataSource:
        return FileClDataSource(
            data_source_yaml_file=YamlFile(yaml_dict=data_source_yaml_dict, logs=logs),
            logs=logs
        )

    @classmethod
    def from_spark_session(cls, spark_session, logs: Logs | None = None) -> DataSource:
        return SparkSessionClDataSource(spark_session=spark_session, logs=logs)

    def with_variable(self, key: str, value: str) -> DataSource:
        self.variables[key] = value
        return self

    def with_variables(self, variables: dict[str, str]) -> DataSource:
        if isinstance(variables, dict):
            self.variables.update(variables)
        return self

    def __enter__(self) -> DataSource:
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Could not close connection: {e}")

    def open(self) -> None:
        self.dbapi_connection = self._create_dbapi_connection()

    @abstractmethod
    def _create_dbapi_connection(self) -> object:
        pass

    def close(self) -> None:
        """
        Closes te connection. This method will not throw any exceptions.
        Check errors with has_errors or assert_no_errors.
        """
        if self.dbapi_connection:
            try:
                self.dbapi_connection.close()
            except Exception as e:
                logger.warning(f"Could not close the dbapi connection: {e}")


class ClDataSource(DataSource, ABC):

    def __init__(self, logs: Logs):
        super().__init__(logs)
        self.sodacl_data_source: SodaCLDataSource | None = None

    def _create_dbapi_connection(self) -> object:
        self.sodacl_data_source: SodaCLDataSource= self._create_sodacl_data_source()
        self.sodacl_data_source.connect()
        return self.sodacl_data_source.connection

    @abstractmethod
    def _create_sodacl_data_source(self) -> SodaCLDataSource:
        pass


class FileClDataSource(ClDataSource):

    def __init__(self, data_source_yaml_file: YamlFile, logs: Logs):
        super().__init__(logs)
        self.data_source_file: YamlFile = data_source_yaml_file
        self.connection_dict: dict | None = None

    def _create_sodacl_data_source(self) -> SodaCLDataSource:
        self.data_source_file.parse(self.variables)

        yaml_helper: yaml_helper = YamlHelper(yaml_file=self.data_source_file, logs=self.logs)
        data_source_yaml_dict: dict = self.data_source_file.dict
        self.data_source_type = yaml_helper.read_string_opt(data_source_yaml_dict, "type")
        self.data_source_name = yaml_helper.read_string(data_source_yaml_dict, "name")
        self.connection_dict = yaml_helper.read_dict(data_source_yaml_dict, "connection")

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


class SparkSessionClDataSource(ClDataSource):

    def __init__(self, spark_session: object, logs: Logs):
        super().__init__(logs)
        self.spark_session: object = spark_session

    def _create_sodacl_data_source(self) -> SodaCLDataSource:
        self.data_source_name = "spark_ds"
        self.data_source_type = "spark_df"
        try:
            return SodaCLDataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=self.data_source_name,
                data_source_type=self.data_source_type,
                data_source_properties={
                    "spark_session": self.spark_session
                },
            )
        except Exception as e:
            self.logs.error(message=f"Could not create the spark session data source: {e}", exception=e)
