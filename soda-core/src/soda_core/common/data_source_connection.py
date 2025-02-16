from __future__ import annotations

from abc import abstractmethod, ABC
from importlib.util import find_spec

from soda_core.common.data_source_results import QueryResult, UpdateResult
from soda_core.common.logs import Logs


class DataSourceConnection(ABC):

    def __init__(
            self,
            name: str,
            connection_properties: dict,
            logs: Logs
    ):
        self.name: str = name
        self.connection_properties: dict = connection_properties
        self.logs: Logs = logs
        self.is_tabulate_available: bool = bool(find_spec(name="tabulate"))
        self.connection: object | None = None
        # Auto-open on creation.  See DataSource.open_connection()
        self.open_connection()

    def __enter__(self):
        pass

    @abstractmethod
    def _create_connection(self, connection_yaml_dict: dict) -> object:
        """
        self.connection_yaml_dict is provided as a parameter for convenience.
        Returns a new DBAPI connection based on the provided connection properties.
        The returned value will be saved in self.connection. See open() for details.
        Exceptions do not need to be handled.  They will be handled by the calling method.
        """

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
                self._log_connection_properties_excl_pwd(self.connection_properties)
                self.connection = self._create_connection(self.connection_properties)
            except Exception as e:
                self.logs.error(f"Could not connect to '{self.name}': {e}", exception=e)

    def _log_connection_properties_excl_pwd(self, connection_yaml_dict: dict):
        dict_without_pwd = {
            k: v
            for k, v in connection_yaml_dict.items()
            if not "password" in k and not "pwd" in k and not "key" in k and not "secret" in k
        }
        self.logs.debug(f"{self.name} connection properties: {dict_without_pwd}")

    def close_connection(self) -> None:
        """
        Closes te connection. This method will not throw any exceptions.
        Check errors with has_errors or assert_no_errors.
        """
        if self.connection:
            try:
                # noinspection PyUnresolvedReferences
                self.connection.close()
            except Exception as e:
                self.logs.warning(
                    message=f"Could not close the DBAPI connection",
                    exception=e
                )
            finally:
                self.connection = None

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_connection()

    def execute_query(self, sql: str) -> QueryResult:
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            self.logs.debug(f"SQL query fetchall: \n{sql}")
            cursor.execute(sql)
            rows = cursor.fetchall()
            if self.is_tabulate_available:
                from tabulate import tabulate
                table_text: str = tabulate(rows, headers=[c.name for c in cursor.description])
                self.logs.debug(f"SQL query result:\n{table_text}")
            else:
                self.logs.debug(f"SQL query result: {len(rows)} rows")
            return QueryResult(rows=rows, columns=cursor.description)
        finally:
            cursor.close()

    def execute_update(self, sql: str) -> UpdateResult:
        # noinspection PyUnresolvedReferences
        cursor = self.connection.cursor()
        try:
            self.logs.debug(f"SQL update: \n{sql}")
            updates = cursor.execute(sql)
            self.commit()
            return updates
        finally:
            cursor.close()

    def commit(self) -> None:
        # noinspection PyUnresolvedReferences
        self.connection.commit()

    def rollback(self) -> None:
        # noinspection PyUnresolvedReferences
        self.connection.rollback()

    def disable_close_connection(self) -> None:
        self.close_connection_enabled = False

    def enable_close_connection(self) -> None:
        self.close_connection_enabled = True
