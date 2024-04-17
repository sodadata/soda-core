from __future__ import annotations

import logging
from abc import abstractmethod, ABC

import soda.common.logs as soda_common_logs
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlHelper, YamlFile
from soda.execution.data_source import DataSource

logger = logging.getLogger(__name__)


class Warehouse:

    """
    Represents the configurations to create a connection. Usually it's loaded from a YAML file.
    """

    def __init__(self, logs: Logs | None = None):
        self.logs: Logs = logs if logs else Logs()

        # only initialized after the .open() method is called
        self.dbapi_connection: object | None = None
        # only initialized after the .open() method is called
        self.warehouse_name: str | None = None
        # only initialized after the .open() method is called
        self.warehouse_type: str | None = None

    @classmethod
    def from_yaml_file(cls, warehouse_file: YamlFile) -> Warehouse:
        return FileClWarehouse(warehouse_yaml_file=warehouse_file)

    @classmethod
    def from_spark_session(cls, spark_session, logs: Logs | None = None) -> Warehouse:
        return SparkSessionClWarehouse(spark_session=spark_session, logs=logs)

    def __enter__(self) -> Warehouse:
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Could not close connection: {e}")

    def __str__(self) -> str:
        return self.warehouse_name

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


class ClWarehouse(Warehouse, ABC):

    def __init__(self, logs: Logs):
        super().__init__(logs)
        self.sodacl_data_source: DataSource | None = None

    def _create_dbapi_connection(self) -> object:
        self.sodacl_data_source: DataSource= self._create_sodacl_data_source()
        try:
            self.sodacl_data_source.connect()
        except Exception as e:
            self.logs.error(f"Could not connect to '{self.warehouse_name}': {e}")
        return self.sodacl_data_source.connection

    @abstractmethod
    def _create_sodacl_data_source(self) -> DataSource:
        pass


class FileClWarehouse(ClWarehouse):

    def __init__(self, warehouse_yaml_file: YamlFile):
        super().__init__(warehouse_yaml_file.logs)
        self.warehouse_file: YamlFile = warehouse_yaml_file
        self.connection_dict: dict | None = None

        if self.warehouse_file.is_ok():
            yaml_helper: yaml_helper = YamlHelper(yaml_file=self.warehouse_file, logs=self.logs)
            warehouse_yaml_dict: dict = self.warehouse_file.dict
            self.warehouse_type = yaml_helper.read_string(warehouse_yaml_dict, "type")
            self.warehouse_name = yaml_helper.read_string(warehouse_yaml_dict, "name")
            self.connection_dict: dict = yaml_helper.read_dict(warehouse_yaml_dict, "connection")

    def _create_sodacl_data_source(self) -> DataSource:
        # consider translating postgres schema search_path option
        # options = f"-c search_path={schema}" if schema else None
        try:
            return DataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=self.warehouse_name,
                data_source_type=self.warehouse_type,
                data_source_properties=self.connection_dict,
            )
        except Exception as e:
            self.logs.error(message=f"Could not create the data source: {e}", exception=e)


class SparkSessionClWarehouse(ClWarehouse):

    def __init__(self, spark_session: object, logs: Logs):
        super().__init__(logs)
        self.spark_session: object = spark_session
        self.warehouse_name = "spark_ds"
        self.warehouse_type = "spark_df"

    def _create_sodacl_data_source(self) -> DataSource:
        try:
            return DataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=self.warehouse_name,
                data_source_type=self.warehouse_type,
                data_source_properties={
                    "spark_session": self.spark_session
                },
            )
        except Exception as e:
            self.logs.error(message=f"Could not create the spark session data source: {e}", exception=e)
