from __future__ import annotations

import logging

import soda.common.logs as soda_common_logs
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.yaml_helper import YamlHelper, YamlFile

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
        self.name: str | None = None

    @classmethod
    def from_yaml_file(cls, data_source_yaml_file_path: str, logs: Logs | None = None) -> DataSource:
        assert isinstance(data_source_yaml_file_path, str) and len(data_source_yaml_file_path) > 1
        return DataSource(
            data_source_yaml_file=YamlFile(yaml_file_path=data_source_yaml_file_path, logs=logs)
        )

    @classmethod
    def from_yaml_str(cls, data_source_yaml_str: str, logs: Logs | None = None) -> DataSource:
        assert isinstance(data_source_yaml_str, str) and len(data_source_yaml_str) > 1
        return DataSource(
            data_source_yaml_file=YamlFile(yaml_str=data_source_yaml_str, logs=logs)
        )

    @classmethod
    def from_yaml_dict(cls, data_source_yaml_dict: str, logs: Logs | None = None) -> DataSource:
        return DataSource(
            data_source_yaml_file=YamlFile(yaml_dict=data_source_yaml_dict, logs=logs)
        )

    @classmethod
    def from_spark_session(cls, spark_session, logs: Logs | None = None) -> DataSource:
        return DataSource(spark_session=spark_session, logs=logs)

    def with_variable(self, key: str, value: str) -> DataSource:
        self.variables[key] = value
        return self

    def with_variables(self, variables: dict[str, str]) -> DataSource:
        if isinstance(variables, dict):
            self.variables.update(variables)
        return self

    def build(self) -> DataSource:
        if self.data_source_file and self.data_source_file.is_ok():
            yaml_helper: YamlHelper = YamlHelper(yaml_file=self.data_source_file, logs=self.logs)
            self.name: str = yaml_helper.read_string(self.data_source_file.dict, "name")
            self.data_source_file.parse(self.variables)
        elif self.spark_session:
            self.name = "spark_ds"
        return self

    def create_connection(self) -> Connection:
        if self.spark_session:
            # build the connection
            return Connection.from_spark_session(
                spark_session=self.spark_session,
                logs=self.logs
            )
        elif self.data_source_file and self.data_source_file.is_ok():
            return Connection.from_yaml_dict(
                data_source_yaml_dict=self.data_source_file.dict,
                logs=self.logs
            )
        else:
            self.logs.error("Data source not properly configured")
            return Connection(logs=self.logs)


class Connection:

    @classmethod
    def from_yaml_dict(cls, data_source_yaml_dict: dict) -> Connection:
        logs: Logs = Logs()
        return Connection.from_yaml_file(
            data_source_yaml_file=YamlFile(logs=logs, yaml_dict=data_source_yaml_dict),
            logs=logs
        )

    @classmethod
    def from_yaml_file(cls, data_source_yaml_file: YamlFile, logs: Logs | None = None) -> Connection:
        TODO continue
        logs: Logs = Logs()
        yaml_helper: yaml_helper = YamlHelper(logs=logs)
        # For now, the type is not yet used, but it will when we start building native contract connection types
        data_source_type: str = yaml_helper.read_string(data_source_yaml_dict, "type")
        return DataSourceConnection(
            data_source_yaml_dict=data_source_yaml_dict,
            logs=logs if logs else Logs()
        )

    @classmethod
    def from_spark_session(cls, spark_session: object, data_source_name: str = "spark_ds", logs: Logs | None = None) -> Connection:
        return DataSourceConnection(data_source_yaml_dict={
                "name": data_source_name,
                "type": "spark_df",
                "connection": {
                    "spark_session": spark_session
                }
            },
            logs=logs if logs else Logs()
        )

    def __enter__(self) -> Connection:
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Could not close connection: {e}")

    def __init__(self, logs: Logs):
        self.logs: Logs = logs
        self.dbapi_connection: object | None = None

    def open(self) -> None:
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


class DataSourceConnection(Connection):

    def __init__(self, data_source_yaml_dict: dict, logs: Logs):
        yaml_helper: yaml_helper = YamlHelper(logs)
        self.data_source_type: str = yaml_helper.read_string_opt(data_source_yaml_dict, "type")
        self.data_source_name: str = yaml_helper.read_string(data_source_yaml_dict, "name")
        self.connection_dict: dict = yaml_helper.read_dict(data_source_yaml_dict, "connection")
        self.logs: Logs = logs

        # consider translating postgres schema search_path option
        # options = f"-c search_path={schema}" if schema else None
        try:
            self.data_source = DataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=self.data_source_name,
                data_source_type=self.data_source_type,
                data_source_properties=self.connection_dict,
            )
        except Exception as e:
            logs.error(message=f"Could not create the connection: {e}", exception=e)
        super().__init__(logs=logs)

    def open(self) -> None:
        self.data_source.connect()
        self.dbapi_connection = self.data_source.connection
