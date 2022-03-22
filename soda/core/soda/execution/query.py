from __future__ import annotations

from datetime import datetime, timedelta

from soda.common.exception_helper import get_exception_stacktrace


class Query:
    def __init__(
        self,
        data_source_scan: DataSourceScan,
        table: Table = None,
        partition: Partition = None,
        column: Column = None,
        unqualified_query_name: str = None,
        sql: str | None = None,
    ):
        self.logs = data_source_scan.scan._logs
        self.data_source_scan = data_source_scan
        self.query_name: str = Query.build_query_name(
            data_source_scan, table, partition, column, unqualified_query_name
        )
        self.table: Table | None = table
        self.partition: Partition | None = partition
        self.column: Column | None = column

        # The SQL query that is used _fetchone or _fetchall or _store
        # This field can also be initialized in the execute method before any of _fetchone,
        # _fetchall or _store are called
        self.sql: str = sql

        # Following fields are initialized in execute method
        self.description: tuple | None = None
        self.row: tuple | None = None
        self.rows: list[tuple] | None = None
        self.storage_ref: StorageRef | None = None
        self.exception: BaseException | None = None
        self.duration: timedelta = None

    def get_cloud_dict(self):
        from soda.execution.column import Column
        from soda.execution.partition import Partition

        return {
            "name": self.query_name,
            "dataSource": self.data_source_scan.data_source.data_source_name,
            "table": Partition.get_table_name(self.partition),
            "partition": Partition.get_partition_name(self.partition),
            "column": Column.get_partition_name(self.column),
            "sql": self.sql,
            "exception": get_exception_stacktrace(self.exception),
            "duration": self.duration,
        }

    @staticmethod
    def build_query_name(data_source_scan, table, partition, column, unqualified_query_name):
        full_query_pieces = [data_source_scan.data_source.data_source_name]
        if partition is not None and partition.partition_name is not None:
            full_query_pieces.append(f"{partition.table.table_name}[{partition.partition_name}]")
        elif table is not None:
            full_query_pieces.append(f"{table.table_name}")
        if column is not None:
            full_query_pieces.append(f"{column.column_name}")
        full_query_pieces.append(unqualified_query_name)
        return ".".join(full_query_pieces)

    def execute(self):
        """
        Execute method implementations should
          - invoke either self.fetchone, self.fetchall or self.store
          - update the metrics with value and optionally other diagnostic information
        """
        self.fetchall()

    def fetchone(self):
        """
        DataSource query execution exceptions will be caught and result in the
        self.exception being populated.
        """
        self.__append_to_scan()
        start = datetime.now()
        data_source = self.data_source_scan.data_source
        try:
            cursor = data_source.connection.cursor()
            try:
                self.logs.debug(f"Query {self.query_name}:\n{self.sql}")
                cursor.execute(self.sql)
                self.row = cursor.fetchone()
                self.description = cursor.description
            finally:
                cursor.close()
        except BaseException as e:
            self.exception = e
            self.logs.error(
                f"Query execution error in {self.query_name}: {e}\n{self.sql}",
                e,
            )
            data_source.query_failed(e)
        finally:
            self.duration = datetime.now() - start

    def fetchall(self):
        """
        DataSource query execution exceptions will be caught and result in the
        self.exception being populated.
        """
        self.__append_to_scan()
        start = datetime.now()
        data_source = self.data_source_scan.data_source
        try:
            cursor = data_source.connection.cursor()
            try:
                self.logs.debug(f"Query {self.query_name}:\n{self.sql}")
                cursor.execute(self.sql)
                self.rows = cursor.fetchall()
                self.description = cursor.description
            finally:
                cursor.close()
        except BaseException as e:
            self.exception = e
            self.logs.error(f"Query error: {self.query_name}: {e}\n{self.sql}", e)
            data_source.query_failed(e)
        finally:
            self.duration = datetime.now() - start

    def store(self):
        """
        DataSource query execution exceptions will be caught and result in the
        self.exception being populated.
        """
        self.__append_to_scan()
        sampler = self.data_source_scan.scan._configuration.sampler
        if sampler:
            start = datetime.now()
            data_source = self.data_source_scan.data_source
            try:
                cursor = data_source.connection.cursor()
                try:
                    self.logs.debug(f"Query {self.query_name}:\n{self.sql}")
                    cursor.execute(self.sql)
                    self.description = cursor.description
                    self.storage_ref = sampler.store_sample(cursor, self)
                finally:
                    cursor.close()
            except BaseException as e:
                self.exception = e
                self.logs.error(f"Query error: {self.query_name}: {e}\n{self.sql}", e)
                data_source.query_failed(e)
            finally:
                self.duration = datetime.now() - start
        else:
            self.logs.warning(f"Not executing query {self.query_name} because no sampler is configured.")

    def __append_to_scan(self):
        scan = self.data_source_scan.scan
        self.index = len(scan._queries)
        scan._queries.append(self)
