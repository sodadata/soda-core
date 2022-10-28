from __future__ import annotations

from datetime import datetime, timedelta

import sqlparse
from soda.common.exception_helper import get_exception_stacktrace
from soda.common.undefined_instance import undefined
from soda.sampler.db_sample import DbSample
from soda.sampler.sample_context import SampleContext
from soda.sampler.sampler import Sampler


class Query:
    def __init__(
        self,
        data_source_scan: DataSourceScan,
        table: Table = None,
        partition: Partition = None,
        column: Column = None,
        unqualified_query_name: str = None,
        sql: str | None = None,
        sample_name: str = "failed_rows",
        location: Location | None = None,
        samples_limit: int | None = 100,
    ):
        self.logs = data_source_scan.scan._logs
        self.data_source_scan = data_source_scan
        self.query_name: str = Query.build_query_name(
            data_source_scan, table, partition, column, unqualified_query_name
        )
        self.sample_name = sample_name
        self.table: Table | None = table
        self.partition: Partition | None = partition
        self.column: Column | None = column
        self.location: Location | None = location
        self.samples_limit: int | None = samples_limit

        # The SQL query that is used _fetchone or _fetchall or _store
        # This field can also be initialized in the execute method before any of _fetchone,
        # _fetchall or _store are called
        self.sql: str = data_source_scan.scan.jinja_resolve(sql)

        # Following fields are initialized in execute method
        self.description: tuple | None = None
        self.row: tuple | None = None
        self.rows: list[tuple] | None = None
        self.sample_ref: SampleRef | None = None
        self.exception: BaseException | None = None
        self.duration: timedelta | None = None

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

    def get_dict(self):
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
        # TODO: some of the subclasses couple setting metric with storing the sample - refactor that.
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
                message=f"Query execution error in {self.query_name}: {e}\n{self.sql}",
                exception=e,
                location=self.location,
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
            self.logs.error(f"Query error: {self.query_name}: {e}\n{self.sql}", exception=e, location=self.location)
            data_source.query_failed(e)
        finally:
            self.duration = datetime.now() - start

    def store(self):
        """
        DataSource query execution exceptions will be caught and result in the
        self.exception being populated.
        """
        self.__append_to_scan()
        sampler: Sampler = self.data_source_scan.scan._configuration.sampler
        start = datetime.now()
        data_source = self.data_source_scan.data_source
        try:
            cursor = data_source.connection.cursor()
            try:
                self.logs.debug(f"Query {self.query_name}:\n{self.sql}")
                cursor.execute(self.sql)
                self.description = cursor.description

                # Check if query does not contain forbidden columns and only create sample if it does not.
                # Query still needs to execute in such situation because some of the queries create both a metric
                # for a check and get the samples.
                # This has a limitation - some of the metrics (e.g. duplicate_count) are set when storing the sample,
                # so those need a workaround - see below.

                # Match table name and exclude config case insensitive - this might be problematic for some edge cases when case sensitivity is desired, but it makes it easier
                # for for-each checks to work here as expected.
                exclude_columns_config = {
                    k.lower(): v for k, v in self.data_source_scan.scan._configuration.exclude_columns.items()
                }

                columns = self._parse_columns_from_query(self.sql)

                has_dataset_exclude_config = False
                offending_columns = []
                exclude_columns = []
                allow_samples = True

                if self.partition and self.partition.table:
                    # TODO (DEAL WITH BEFORE MERGING) Refactor and use new (now duplicated) logic from data_source
                    table_name_lower = self.partition.table.table_name.lower()
                    if table_name_lower in exclude_columns_config:
                        # TODO: Revisit - using partition since partition is generally set on check->metrics->queries and contains reference to Table anyway.
                        exclude_columns = exclude_columns + exclude_columns_config[table_name_lower]
                        has_dataset_exclude_config = True

                if "global" in exclude_columns_config:
                    # Global exclude is just added to dataset specific.
                    # TODO: (DEAL WITH BEFORE MERGING) is "global" the best key in config here? Also, dont forget to document!!!
                    # TODO (DEAL WITH BEFORE MERGING) - this is not truly "global". If set to a list of columns we cannot block all sample queries with "*" in them, do we want such incomplete feature?
                    exclude_columns = exclude_columns + exclude_columns_config["global"]

                exclude_columns = list(set(exclude_columns))

                for column in columns:
                    if (column == "*" and has_dataset_exclude_config) or column in exclude_columns:
                        allow_samples = False
                        offending_columns.append(column)

                db_sample = DbSample(cursor, self.data_source_scan.data_source)

                # A bit of a hacky workaround for queries that also set the metric in one go.
                # TODO: revisit after decoupling getting metric values and storing samples. This can be dangerous, it sets the metric value
                # only when metric value is not set, but this could cause weird regressions.
                if hasattr(self, "metric") and self.metric and self.metric.value == undefined:
                    self.metric.set_value(len(db_sample.get_rows()))

                if allow_samples:
                    sample_context = SampleContext(
                        sample=db_sample,
                        sample_name=self.sample_name,
                        query=self.sql,
                        data_source=self.data_source_scan.data_source,
                        partition=self.partition,
                        column=self.column,
                        scan=self.data_source_scan.scan,
                        logs=self.data_source_scan.scan._logs,
                        samples_limit=self.samples_limit,
                    )

                    self.sample_ref = sampler.store_sample(sample_context)
                else:
                    self.logs.info(
                        f"Skipping samples from query '{self.query_name}'. Excluded column(s) present: {offending_columns}."
                    )
            finally:
                cursor.close()
        except BaseException as e:
            self.exception = e
            self.logs.error(f"Query error: {self.query_name}: {e}\n{self.sql}", exception=e, location=self.location)
            data_source.query_failed(e)
        finally:
            self.duration = datetime.now() - start

    def __append_to_scan(self):
        scan = self.data_source_scan.scan
        self.index = len(scan._queries)
        scan._queries.append(self)

    def _parse_columns_from_query(self, query: str) -> list[str]:
        columns = []
        statements = sqlparse.split(query)

        for statement in statements:
            column_lines = None
            start_looking_for_columns = False
            parsed = sqlparse.parse(statement)[0]
            for token in parsed.tokens:
                if token.ttype == sqlparse.tokens.Keyword.DML and token.value.lower() == "select":
                    start_looking_for_columns = True

                # This is now after a "select", look for a) no type which is list of columns or b) wildcard which is "*"
                if start_looking_for_columns and (token.ttype == None or token.ttype == sqlparse.tokens.Token.Wildcard):
                    column_lines = token.value
                    break
            # Remove newlines, double whitespaces and split by colon.
            column_lines = " ".join(column_lines.replace("\n", "").split()).split(",")

            # Strip each "column statement" of whitespace, remove everything after the first whitespace and keep everything after first dot.
            for column in column_lines:
                column, _, _ = column.strip().partition(" ")
                if "." in column:
                    _, _, column = column.strip().partition(".")
                columns.append(column)

        return columns
