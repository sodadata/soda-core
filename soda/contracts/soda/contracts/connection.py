from __future__ import annotations

import logging
from typing import Dict

from ruamel.yaml import YAML
from ruamel.yaml.error import MarkedYAMLError

import soda.common.logs as soda_common_logs
from soda.contracts.impl.logs import Logs
from soda.contracts.impl.variable_resolver import VariableResolver
from soda.execution.data_source import DataSource

logger = logging.getLogger(__name__)


class SodaException(Exception):
    """
    See also adr/03_exceptions_vs_error_logs.md
    """

    def __init__(self,
                 message: str | None = None,
                 contract_result: "ContractResult | None" = None
                 ):
        from soda.contracts.contract import ContractResult
        self.contract_result: ContractResult = contract_result
        message_parts: list[str] = []
        if message:
            message_parts.append(message)
        if self.contract_result:
            message_parts.append(str(self.contract_result))
        exception_message: str ="\n".join(message_parts)
        super().__init__(exception_message)


class Connection:
    """
    A wrapper for DBAPI a connection to handle all database differences. Usage:

    with Connection.from_dict({
      "type": "postgres",
      "host": "localhost",
      "database": "soda",
      "user": "postgres",
      "password": "<PASSWORD>"
    }) as connection:
        # Do stuff with connection

    When creating a connection from YAML, you can use ${VAR} to resolve environment variables.
    Recommended for credentials.

    The 'type' property is used to determine the type of connection and driver.
    All the other properties are passed in the DBAPI connect method to create the DBAPI connection.
    """

    def __init__(self, dbapi_connection: object | None = None, logs: Logs | None = None):
        self.dbapi_connection = dbapi_connection
        # See also adr/03_exceptions_vs_error_logs.md
        self.logs: Logs = logs if logs else Logs()

    @classmethod
    def from_yaml_file(cls, connection_yaml_file_path: str) -> Connection:
        """
        with Connection.from_yaml_file(file_path) as connection:
            # Do stuff with connection

        Use ${YOUR_PASSWORD} to resolve environment variables. Recommended for credentials.

        :param connection_yaml_file_path: A file path to a YAML file containing the connection configuration properties.
        :return: an open connection, if no exception is raised
        :raises SodaConnectionException: if the connection cannot be established for any reason
        """

        logs: Logs = Logs()
        try:
            if not isinstance(connection_yaml_file_path, str):
                logs.error(
                    f"Couldn't create connection from yaml file. Expected str in parameter "
                    f"connection_yaml_file_path={connection_yaml_file_path}, but was '{type(connection_yaml_file_path)}"
                )
            elif not len(connection_yaml_file_path) > 1:
                logs.error(
                    f"Couldn't create connection from yaml file. connection_yaml_file_path is an empty string"
                )
            else:
                with open(file=connection_yaml_file_path) as f:
                    connection_yaml_str = f.read()
                    return cls.from_yaml_str(connection_yaml_str=connection_yaml_str, logs=logs)
        except Exception as e:
            logs.error(
                f"Couldn't create connection from yaml file '{connection_yaml_file_path}': {e}",
                exception=e
            )
        return Connection(logs=logs)

    @classmethod
    def from_yaml_str(cls,
                      connection_yaml_str: str,
                      variables: Dict[str, str] | None = None,
                      logs: Logs | None = None
                      ) -> Connection:
        """
        # TODO specify where the connection configuration properties are being documented
        connection_yaml_str: str = "...YAML string for connection configuration properties..."
        with Connection.from_yaml_str(connection_yaml_str) as connection:
           # do stuff with connection

        Use ${YOUR_PASSWORD} to resolve environment variables. Recommended for credentials. Resolving will
        first use the variables parameter and otherwise fall back to environment variables.  A SodaConnectionException
        will be raised if a variable is used and cannot be resolved.

        :param connection_yaml_str: A YAML string containing the connection configuration properties.
        :param variables: Optional dictionary of variables that will be used to resolve variables before checking
        environment variables.
        :return: an open connection, if no exception is raised
        :raises SodaConnectionException: if the connection cannot be established for any reason
        """

        if not logs:
            logs = Logs()

        if not isinstance(connection_yaml_str, str):
            logs.error(
                f"Expected a string for parameter connection_yaml_str, "
                f"but was '{type(connection_yaml_str)}'"
            )

        if connection_yaml_str == "":
            logs.error(
                f"connection_yaml_str must be non-emtpy, but was ''"
            )

        try:
            variable_resolver = VariableResolver(variables=variables, logs=logs)
            resolved_connection_yaml_str = variable_resolver.resolve(connection_yaml_str)
        except BaseException as e:
            logs.error(f"Could not resolve variables in connection YAML: {e}", exception=e)

        try:
            yaml = YAML()
            yaml.preserve_quotes = True
            connection_dict = yaml.load(resolved_connection_yaml_str)
            if not isinstance(connection_dict, dict):
                logs.error(
                    f"Content of the connection YAML file must be a YAML object, "
                    f"but was {type(connection_dict)}"
                )
            return cls.from_dict(connection_dict)
        except MarkedYAMLError as e:
            mark = e.context_mark if e.context_mark else e.problem_mark
            line = mark.line + 1,
            column = mark.column + 1,
            logs.error(f"YAML syntax error: {e} | line={line} | column={column}")

    @classmethod
    def from_dict(cls, connection_dict: dict, logs: Logs | None = None) -> Connection:
        """
        with Connection.from_dict({
          "type": "postgres",
          "host": "localhost",
          "database": "soda",
          "user": "postgres",
          "password": "<PASSWORD>"
        }) as connection:
            # Do stuff with connection

        :return: an open connection, if no exception is raised
        :raises SodaConnectionException: if the connection cannot be established for any reason
        """
        if not logs:
            logs = Logs()

        connection_type: str | None = None

        if not isinstance(connection_dict, dict):
            logs.error(
                f"connect_properties must be a object, but was {type(connection_dict)}"
            )
        elif "type" not in connection_dict:
            logs.error(
                f"'type' is required, but was not provided"
            )
        else:
            connection_type = connection_dict.get("type")
            if not isinstance(connection_type, str):
                logs.error(
                    f"'type' must be a string, but was  {type(connection_type)}"
                )
        return DataSourceConnection(connection_type=connection_type, connection_dict=connection_dict, logs=logs)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            logger.warning(f"Could not close connection: {e}")

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

    def _create_contract_parser(self, logs: Logs) -> "ContractParser":
        """
        Enables connection subclasses to create database specific errors during translation.
        This is for better static analysis of the contract taking the connection type into account.
        """
        from soda.contracts.impl.contract_parser import ContractParser
        return ContractParser(logs)


class DataSourceConnection(Connection):

    def __init__(self, connection_type: str, connection_dict: dict, logs: Logs | None = None):
        # consider translating postgres schema search_path option
        # options = f"-c search_path={schema}" if schema else None
        try:
            self.data_source = DataSource.create(
                logs=soda_common_logs.Logs(logger=logger),
                data_source_name=f"{connection_type}_ds",
                data_source_type=connection_type,
                data_source_properties=connection_dict,
            )
            self.data_source.connect()
        except Exception as e:
            logs.error(message=f"Could not create the connection: {e}", exception=e)
        super().__init__(dbapi_connection=self.data_source.connection, logs=logs)
