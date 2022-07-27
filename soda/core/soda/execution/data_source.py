from __future__ import annotations

import datetime
import hashlib
import importlib
import json
import re
from datetime import date, datetime
from numbers import Number
from textwrap import dedent

from soda.common.exceptions import DataSourceError
from soda.common.logs import Logs
from soda.execution.data_type import DataType
from soda.execution.query.partition_queries import PartitionQueries
from soda.execution.query.query import Query
from soda.execution.query.schema_query import TableColumnsQuery
from soda.sampler.sample_ref import SampleRef


class DataSource:
    """
    Implementing a DataSource:
    @m1n0, can you add a checklist here of places where DataSource implementors need to make updates to add
    a new DataSource?

    Validation of the connection configuration properties:
    The DataSource impl is only responsible to raise an exception with an appropriate message in te #connect()
    See that abstract method below for more details.
    """

    # Maps synonym types for the convenience of use in checks.
    # Keys represent the data_source type, values are lists of "aliases" that can be used in SodaCL as synonyms.
    SCHEMA_CHECK_TYPES_MAPPING: dict = {
        "character varying": ["varchar", "text"],
        "double precision": ["decimal"],
        "timestamp without time zone": ["timestamp"],
        "timestamp with time zone": ["timestamptz"],
    }
    SQL_TYPE_FOR_CREATE_TABLE_MAP: dict = {
        DataType.TEXT: "VARCHAR(255)",
        DataType.INTEGER: "INT",
        DataType.DECIMAL: "FLOAT",
        DataType.DATE: "DATE",
        DataType.TIME: "TIME",
        DataType.TIMESTAMP: "TIMESTAMP",
        DataType.TIMESTAMP_TZ: "TIMESTAMPTZ",
        DataType.BOOLEAN: "BOOLEAN",
    }

    SQL_TYPE_FOR_SCHEMA_CHECK_MAP = {
        DataType.TEXT: "character varying",
        DataType.INTEGER: "integer",
        DataType.DECIMAL: "double precision",
        DataType.DATE: "date",
        DataType.TIME: "time",
        DataType.TIMESTAMP: "timestamp without time zone",
        DataType.TIMESTAMP_TZ: "timestamp with time zone",
        DataType.BOOLEAN: "boolean",
    }

    NUMERIC_TYPES_FOR_PROFILING = ["integer", "double precision", "double"]
    TEXT_TYPES_FOR_PROFILING = ["character varying", "varchar", "text"]

    @staticmethod
    def camel_case_data_source_type(data_source_type: str) -> str:
        if "bigquery" == data_source_type:
            return "BigQuery"
        elif "spark_df" == data_source_type:
            return "SparkDf"
        elif "sqlserver" == data_source_type:
            return "SQLServer"
        else:
            return f"{data_source_type[0:1].upper()}{data_source_type[1:]}"

    @staticmethod
    def create(
        logs: Logs,
        data_source_name: str,
        data_source_type: str,
        data_source_properties: dict,
    ) -> DataSource:
        """
        The returned data_source does not have a connection.  It is the responsibility of
        the caller to initialize data_source.connection.  To create a new connection,
        use data_source.connect(...)
        """
        try:
            data_source_properties["connection_type"] = data_source_type
            module = importlib.import_module(f"soda.data_sources.{data_source_type}_data_source")
            data_source_class = f"{DataSource.camel_case_data_source_type(data_source_type)}DataSource"
            class_ = getattr(module, data_source_class)
            return class_(logs, data_source_name, data_source_properties)
        except ModuleNotFoundError as e:
            if data_source_type == "postgresql":
                logs.error(f'Data source type "{data_source_type}" not found. Did you mean postgres?')
            else:
                raise DataSourceError(
                    f'Data source type "{data_source_type}" not found. Did you spell {data_source_type} correctly? Did you install module soda-core-{data_source_type}?'
                )
            return None

    def __init__(
        self,
        logs: Logs,
        data_source_name: str,
        data_source_properties: dict,
    ):
        self.logs = logs
        self.data_source_name = data_source_name
        self.data_source_properties: dict = data_source_properties
        # Pep 249 compliant connection object (aka DBAPI)
        # https://www.python.org/dev/peps/pep-0249/#connection-objects
        # @see self.connect() for initialization
        self.type = self.data_source_properties.get("connection_type")
        self.connection = None
        self.database: str | None = data_source_properties.get("database")
        self.schema: str | None = data_source_properties.get("schema")
        self.table_prefix: str | None = self._create_table_prefix()
        # self.data_source_scan is initialized in create_data_source_scan(...) below
        self.data_source_scan: DataSourceScan | None = None

    def create_data_source_scan(self, scan: Scan, data_source_scan_cfg: DataSourceScanCfg):
        from soda.execution.data_source_scan import DataSourceScan

        data_source_scan = DataSourceScan(scan, data_source_scan_cfg, self)
        self.data_source_scan = data_source_scan

        return self.data_source_scan

    def validate_configuration(self, logs: Logs) -> None:
        raise NotImplementedError(f"TODO: Implement {type(self)}.validate_configuration(...)")

    def get_type_name(self, type_code):
        return str(type_code)

    def create_partition_queries(self, partition):
        return PartitionQueries(partition)

    def is_supported_metric_name(self, metric_name: str) -> bool:
        return (
            metric_name in ["row_count", "missing_count", "invalid_count", "valid_count", "duplicate_count"]
            or self.get_metric_sql_aggregation_expression(metric_name, None, None) is not None
        )

    def get_metric_sql_aggregation_expression(self, metric_name: str, metric_args: list[object] | None, expr: str):
        if "min" == metric_name:
            return self.expr_min(expr)
        if "max" == metric_name:
            return self.expr_max(expr)
        if "avg" == metric_name:
            return self.expr_avg(expr)
        if "sum" == metric_name:
            return self.expr_sum(expr)
        if "min_length" == metric_name:
            return self.expr_min(self.expr_length(expr))
        if "max_length" == metric_name:
            return self.expr_max(self.expr_length(expr))
        if "avg_length" == metric_name:
            return self.expr_avg(self.expr_length(expr))
        return None

    def is_same_type_in_schema_check(self, expected_type: str, actual_type: str):
        expected_type = expected_type.strip().lower()

        if (
            actual_type in self.SCHEMA_CHECK_TYPES_MAPPING
            and expected_type in self.SCHEMA_CHECK_TYPES_MAPPING[actual_type]
        ):
            return True

        return expected_type == actual_type.lower()

    @staticmethod
    def column_metadata_columns() -> list:
        return ["column_name", "data_type", "is_nullable"]

    @staticmethod
    def column_metadata_catalog_column() -> str:
        return "table_catalog"

    ######################
    # Store Table Sample
    ######################

    def store_table_sample(self, table_name: str, limit: int | None = None) -> SampleRef:
        sql = self.sql_select_all(table_name=table_name, limit=limit)
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=f"store-sample-for-{table_name}",
            sql=sql,
            sample_name="table_sample",
        )
        query.store()
        return query.sample_ref

    def sql_select_all(self, table_name: str, limit: int | None = None) -> str:
        qualified_table_name = self.qualified_table_name(table_name)
        limit_sql = ""
        if limit is not None:
            limit_sql = f" \n LIMIT {limit}"
        sql = f"SELECT * FROM {qualified_table_name}{limit_sql}"
        return sql

    ############################################
    # For a table, get the columns metadata
    ############################################

    def get_table_columns(
        self,
        table_name: str,
        query_name: str,
        included_columns: list[str] | None = None,
        excluded_columns: list[str] | None = None,
    ) -> dict[str, str] | None:
        """
        :return: A dict mapping column names to data source data types.  Like eg
        {"id": "varchar", "size": "int8", ...}
        """
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=query_name,
            sql=self.sql_get_table_columns(
                table_name, included_columns=included_columns, excluded_columns=excluded_columns
            ),
        )
        query.execute()
        if len(query.rows) > 0:
            return {row[0]: row[1] for row in query.rows}
        return None

    def create_table_columns_query(self, partition: Partition, schema_metric: SchemaMetric) -> TableColumnsQuery:
        return TableColumnsQuery(partition, schema_metric)

    def sql_get_table_columns(
        self,
        table_name: str,
        included_columns: list[str] | None = None,
        excluded_columns: list[str] | None = None,
    ) -> str:
        def is_quoted(table_name):
            return (table_name.startswith('"') and table_name.endswith('"')) or (
                table_name.startswith("`") and table_name.endswith("`")
            )

        table_name_lower = table_name.lower()
        unquoted_table_name_lower = table_name_lower[1:-1] if is_quoted(table_name_lower) else table_name_lower

        filter_clauses = [f"lower(table_name) = '{unquoted_table_name_lower}'"]

        if self.database:
            filter_clauses.append(f"lower({self.column_metadata_catalog_column()}) = '{self.database.lower()}'")

        if self.schema:
            filter_clauses.append(f"lower(table_schema) = '{self.schema.lower()}'")

        if included_columns:
            include_clauses = []
            for col in included_columns:
                include_clauses.append(f"lower(column_name) LIKE lower('{col}')")
            include_causes_or = " OR ".join(include_clauses)
            filter_clauses.append(f"({include_causes_or})")

        if excluded_columns:
            for col in excluded_columns:
                filter_clauses.append(f"lower(column_name) NOT LIKE lower('{col}')")

        where_filter = " \n  AND ".join(filter_clauses)

        # compose query template
        # NOTE: we use `order by ordinal_position` to guarantee stable orders of columns
        # (see https://www.postgresql.org/docs/current/infoschema-columns.html)
        # this mainly has an advantage in testing but bears very little as to how Soda Cloud
        # displays those columns as they are ordered alphabetically in the UI.
        sql = (
            f"SELECT {', '.join(self.column_metadata_columns())} \n"
            f"FROM {self.sql_information_schema_columns()} \n"
            f"WHERE {where_filter}"
            "\nORDER BY ORDINAL_POSITION"
        )
        return sql

    ############################################
    # Get table names with count in one go
    ############################################

    def sql_get_table_names_with_count(
        self, include_tables: list[str] | None = None, exclude_tables: list[str] | None = None
    ) -> str:
        table_filter_expression = self.sql_table_include_exclude_filter(
            "relname", "schemaname", include_tables, exclude_tables
        )
        where_clause = f"\nWHERE {table_filter_expression} \n" if table_filter_expression else ""
        return f"SELECT relname, n_live_tup \n" f"FROM pg_stat_user_tables" f"{where_clause}"

    def sql_get_table_count(self, table_name: str) -> str:
        return f"SELECT {self.expr_count_all()} from {self.qualified_table_name(table_name)}"

    def sql_table_include_exclude_filter(
        self,
        table_column_name: str,
        schema_column_name: str | None = None,
        include_tables: list[str] = [],
        exclude_tables: list[str] = [],
    ) -> str | None:
        tablename_filter_clauses = []

        def build_table_matching_conditions(tables: list[str], comparison_operator: str):
            conditions = []

            for table in tables:
                # This is intended to be future proof and support quoted table names. The method is not used in such way and table names/patterns that still
                # need to be quoted are passed here, e.g. `%sodatest_%`. I.e. this condition is always met and default casify is always used, table name below is therefore single quoted.
                if not self.is_table_quoted(table):
                    table = self.default_casify_table_name(table)

                conditions.append(f"{table_column_name} {comparison_operator} '{table}'")
            return conditions

        if include_tables:
            sql_include_clauses = " OR ".join(build_table_matching_conditions(include_tables, "like"))
            tablename_filter_clauses.append(f"({sql_include_clauses})")

        if exclude_tables:
            tablename_filter_clauses.extend(build_table_matching_conditions(exclude_tables, "not like"))

        if hasattr(self, "schema") and self.schema and schema_column_name:
            tablename_filter_clauses.append(f"lower({schema_column_name}) = '{self.schema.lower()}'")

        return "\n      AND ".join(tablename_filter_clauses) if tablename_filter_clauses else None

    def sql_find_table_names(
        self,
        filter: str | None = None,
        include_tables: list[str] = [],
        exclude_tables: list[str] = [],
        table_column_name: str = "table_name",
        schema_column_name: str = "table_schema",
    ) -> str:
        sql = f"SELECT {table_column_name} \n" f"FROM {self.sql_information_schema_tables()}"
        where_clauses = []

        if filter:
            where_clauses.append(f"lower({self.default_casify_column_name(table_column_name)}) like '{filter.lower()}'")

        includes_excludes_filter = self.sql_table_include_exclude_filter(
            table_column_name, schema_column_name, include_tables, exclude_tables
        )
        if includes_excludes_filter:
            where_clauses.append(includes_excludes_filter)

        if where_clauses:
            where_clauses_sql = "\n  AND ".join(where_clauses)
            sql += f"\nWHERE {where_clauses_sql}"

        return sql

    def sql_information_schema_tables(self) -> str:
        return "information_schema.tables"

    def sql_information_schema_columns(self) -> str:
        return "information_schema.columns"

    def sql_analyze_table(self, table: str) -> str | None:
        return None

    def cast_to_text(self, expr: str) -> str:
        return f"CAST({expr} AS VARCHAR)"

    def profiling_sql_values_frequencies_query(
        self,
        data_type_category: str,
        table_name: str,
        column_name: str,
        limit_mins_maxs: int,
        limit_frequent_values: int,
    ) -> str:
        cast_to_text = self.cast_to_text

        value_frequencies_cte = self.profiling_sql_value_frequencies_cte(table_name, column_name)

        union = self.sql_union()

        frequent_values_cte = f"""frequent_values AS (
                            SELECT {cast_to_text("'frequent_values'")} AS metric_, ROW_NUMBER() OVER(ORDER BY frequency_ DESC) AS index_, value_, frequency_
                            FROM value_frequencies
                            ORDER BY frequency_ desc
                            LIMIT {limit_frequent_values}
                        )"""

        if data_type_category == "text":
            return dedent(
                f"""
                    WITH
                        {value_frequencies_cte},
                        {frequent_values_cte}
                    SELECT *
                    FROM frequent_values
                    ORDER BY metric_ ASC, index_ ASC
                """
            )

        elif data_type_category == "numeric":

            mins_cte = f"""mins AS (
                            SELECT {cast_to_text("'mins'")} AS metric_, ROW_NUMBER() OVER(ORDER BY value_ ASC) AS index_, value_, frequency_
                            FROM value_frequencies
                            WHERE value_ IS NOT NULL
                            ORDER BY value_ ASC
                            LIMIT {limit_mins_maxs}
                        )"""

            maxs_cte = f"""maxs AS (
                            SELECT {cast_to_text("'maxs'")} AS metric_, ROW_NUMBER() OVER(ORDER BY value_ DESC) AS index_, value_, frequency_
                            FROM value_frequencies
                            WHERE value_ IS NOT NULL
                            ORDER BY value_ DESC
                            LIMIT {limit_mins_maxs}
                        )"""

            return dedent(
                f"""
                    WITH
                        {value_frequencies_cte},
                        {mins_cte},
                        {maxs_cte},
                        {frequent_values_cte},
                        result AS (
                            SELECT * FROM mins
                            {union}
                            SELECT * FROM maxs
                            {union}
                            SELECT * FROM frequent_values
                        )
                    SELECT *
                    FROM result
                    ORDER BY metric_ ASC, index_ ASC
                """
            )

        raise AssertionError("data_type_category must be either 'numeric' or 'text'")

    def sql_union(self):
        return "UNION"

    def profiling_sql_value_frequencies_cte(self, table_name: str, column_name: str) -> str:
        quoted_column_name = self.quote_column(column_name)
        qualified_table_name = self.qualified_table_name(table_name)
        return f"""value_frequencies AS (
                            SELECT {quoted_column_name} AS value_, count(*) AS frequency_
                            FROM {qualified_table_name}
                            WHERE {quoted_column_name} IS NOT NULL
                            GROUP BY {quoted_column_name}
                        )"""

    def profiling_sql_aggregates_numeric(self, table_name: str, column_name: str) -> str:
        column_name = self.quote_column(column_name)
        qualified_table_name = self.qualified_table_name(table_name)
        return dedent(
            f"""
            SELECT
                avg({column_name}) as average
                , sum({column_name}) as sum
                , variance({column_name}) as variance
                , stddev({column_name}) as standard_deviation
                , count(distinct({column_name})) as distinct_values
                , sum(case when {column_name} is null then 1 else 0 end) as missing_values
            FROM {qualified_table_name}
            """
        )

    def profiling_sql_aggregates_text(self, table_name: str, column_name: str) -> str:
        column_name = self.quote_column(column_name)
        qualified_table_name = self.qualified_table_name(table_name)
        return dedent(
            f"""
            SELECT
                count(distinct({column_name})) as distinct_values
                , sum(case when {column_name} is null then 1 else 0 end) as missing_values
                , avg(length({column_name})) as avg_length
                , min(length({column_name})) as min_length
                , max(length({column_name})) as max_length
            FROM {qualified_table_name}
            """
        )

    def histogram_sql_and_boundaries(
        self,
        table_name: str,
        column_name: str,
        min_value: int | float,
        max_value: int | float,
        n_distinct: int,
        column_type: str,
    ) -> tuple[str | None, list[int | float]]:
        # TODO: make configurable or derive dynamically based on data quantiles etc.
        max_n_bins = 20
        number_of_bins: int = max(1, min(n_distinct, max_n_bins))
        number_of_intervals: int = number_of_bins - 1

        if min_value >= max_value:
            self.logs.warning(
                "Min of {column_name} on table: {table_name} must be smaller than max value. Min is {min_value}, and max is {max_value}".format(
                    column_name=column_name,
                    table_name=table_name,
                    min_value=min_value,
                    max_value=max_value,
                )
            )
            return None, []

        bin_width = (max_value - min_value) / number_of_intervals

        if bin_width.is_integer() and column_type == "integer":
            bin_width = int(bin_width)
            min_value = int(min_value)
            max_value = int(max_value)

        bins_list = [round(min_value + i * bin_width, 2) for i in range(0, number_of_bins)]

        field_clauses = []
        for i in range(0, number_of_bins):
            lower_bound = "" if i == 0 else f"{bins_list[i]} <= value_"
            upper_bound = "" if i == number_of_bins - 1 else f"value_ < {bins_list[i + 1]}"
            optional_and = "" if lower_bound == "" or upper_bound == "" else " AND "
            field_clauses.append(f"SUM(CASE WHEN {lower_bound}{optional_and}{upper_bound} THEN frequency_ END)")

        fields = ",\n ".join(field_clauses)

        value_frequencies_cte = self.profiling_sql_value_frequencies_cte(table_name, column_name)

        sql = dedent(
            f"""
            WITH
                {value_frequencies_cte}
            SELECT {fields}
            FROM value_frequencies"""
        )
        return sql, bins_list

    ######################
    # Query Execution
    ######################

    def get_row_counts_all_tables(
        self,
        include_tables: list[str] | None = None,
        exclude_tables: list[str] | None = None,
        query_name: str | None = None,
    ) -> dict[str, int]:
        """
        Returns a dict that maps table names to row counts.
        """
        sql = self.sql_get_table_names_with_count(include_tables=include_tables, exclude_tables=exclude_tables)
        if sql:
            query = Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=query_name or "get_row_counts_all_tables",
                sql=sql,
            )
            query.execute()
            return {self._optionally_quote_table_name_from_meta_data(row[0]): row[1] for row in query.rows}

        # Single query to get the metadata not available, get the counts one by one.
        all_tables = self.get_table_names(include_tables=include_tables, exclude_tables=exclude_tables)
        result = {}
        for table_name in all_tables:
            table_count = self.get_table_row_count(table_name)
            if table_count:
                result[table_name] = table_count

        return result

    def get_table_row_count(self, table_name: str) -> int | None:
        """Deprecated, use get_row_counts_all_tables whenever possible."""
        query_name_str = f"get_row_count_{table_name}"
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=query_name_str,
            sql=self.sql_get_table_count(self.quote_table(table_name)),
        )
        query.execute()
        if query.rows:
            return query.rows[0][0]
        return None

    def get_table_names(
        self,
        filter: str | None = None,
        include_tables: list[str] = [],
        exclude_tables: list[str] = [],
        query_name: str | None = None,
    ) -> list[str]:
        if not include_tables and not exclude_tables:
            return []
        sql = self.sql_find_table_names(filter, include_tables, exclude_tables)
        query = Query(
            data_source_scan=self.data_source_scan,
            unqualified_query_name=query_name or "get_table_names",
            sql=sql,
        )
        query.execute()
        table_names = [self._optionally_quote_table_name_from_meta_data(row[0]) for row in query.rows]
        return table_names

    def _optionally_quote_table_name_from_meta_data(self, table_name: str) -> str:
        """
        To be used by all table names coming from metadata queries.  Quotes are added if needed if the table
        doesn't match the default casify rules.  The table_name is returned unquoted if it matches the default
        casify rules.
        """
        # if the table name needs quoting
        if table_name != self.default_casify_table_name(table_name):
            # add the quotes
            return self.quote_table(table_name)
        else:
            # return the bare table name
            return table_name

    def analyze_table(self, table: str):
        if self.sql_analyze_table(table):
            Query(
                data_source_scan=self.data_source_scan,
                unqualified_query_name=f"analyze_{table}",
                sql=self.sql_analyze_table(table),
            ).execute()

    def _create_table_prefix(self):
        """
        Use
            * self.schema
            * self.database
            * self.quote_table(unquoted_table_name)
        to compose the table prefix to be used in Soda Core queries.  The returned table prefix
        should not include the dot (.) and can optionally be None.  Consider quoting as well.
        Examples:
            return None
            return self.schema
            return self.database
            return f'"{self.database}"."{self.schema}"'
        """
        return self.schema

    def update_schema(self, schema_name):
        self.schema = schema_name
        self.table_prefix = self._create_table_prefix()

    def qualified_table_name(self, table_name: str) -> str:
        """
        table_name can be quoted or unquoted
        """
        if self.table_prefix:
            return f"{self.table_prefix}.{table_name}"
        return table_name

    def quote_table_declaration(self, table_name: str) -> str:
        return self.quote_table(table_name=table_name)

    def is_table_quoted(self, table_name: str) -> bool:
        return (table_name.startswith('"') and table_name.endswith('"')) or (
            table_name.startswith("'") and table_name.endswith("'")
        )

    def quote_table(self, table_name: str) -> str:
        return f'"{table_name}"'

    def quote_column_declaration(self, column_name: str) -> str:
        return self.quote_column(column_name)

    def quote_column(self, column_name: str) -> str:
        return f'"{column_name}"'

    def get_sql_type_for_create_table(self, data_type: str) -> str:
        if data_type in self.SQL_TYPE_FOR_CREATE_TABLE_MAP:
            return self.SQL_TYPE_FOR_CREATE_TABLE_MAP.get(data_type)
        else:
            return data_type

    def get_sql_type_for_schema_check(self, data_type: str) -> str:
        data_source_type = self.SQL_TYPE_FOR_SCHEMA_CHECK_MAP.get(data_type)
        if data_source_type is None:
            raise NotImplementedError(
                f"Data type {data_type} is not mapped in {type(self)}.SQL_TYPE_FOR_SCHEMA_CHECK_MAP"
            )
        return data_source_type

    def literal(self, o: object):
        if o is None:
            return "NULL"
        elif isinstance(o, Number):
            return self.literal_number(o)
        elif isinstance(o, str):
            return self.literal_string(o)
        elif isinstance(o, datetime):
            return self.literal_datetime(o)
        elif isinstance(o, date):
            return self.literal_date(o)
        elif isinstance(o, list) or isinstance(o, set) or isinstance(o, tuple):
            return self.literal_list(o)
        elif isinstance(o, bool):
            return self.literal_boolean(o)
        raise RuntimeError(f"Cannot convert type {type(o)} to a SQL literal: {o}")

    def literal_number(self, value: Number):
        if value is None:
            return None
        return str(value)

    def literal_string(self, value: str):
        if value is None:
            return None
        return "'" + self.escape_string(value) + "'"

    def literal_list(self, l: list):
        if l is None:
            return None
        return "(" + (",".join([self.literal(e) for e in l])) + ")"

    def literal_date(self, date: date):
        date_string = date.strftime("%Y-%m-%d")
        return f"DATE '{date_string}'"

    def literal_datetime(self, datetime: datetime):
        return f"'{datetime.isoformat()}'"

    def literal_boolean(self, boolean: bool):
        return "TRUE" if boolean is True else "FALSE"

    def expr_count_all(self) -> str:
        return "COUNT(*)"

    def expr_count_conditional(self, condition: str):
        return f"COUNT(CASE WHEN {condition} THEN 1 END)"

    def expr_conditional(self, condition: str, expr: str):
        return f"CASE WHEN {condition} THEN {expr} END"

    def expr_count(self, expr):
        return f"COUNT({expr})"

    def expr_distinct(self, expr):
        return f"DISTINCT({expr})"

    def expr_length(self, expr):
        return f"LENGTH({expr})"

    def expr_min(self, expr):
        return f"MIN({expr})"

    def expr_max(self, expr):
        return f"MAX({expr})"

    def expr_avg(self, expr):
        return f"AVG({expr})"

    def expr_sum(self, expr):
        return f"SUM({expr})"

    def expr_regexp_like(self, expr: str, regex_pattern: str):
        return f"REGEXP_LIKE({expr}, '{regex_pattern}')"

    def expr_in(self, left: str, right: str):
        return f"{left} IN {right}"

    def cast_text_to_number(self, column_name, validity_format: str):
        """Cast string to number

        - first regex replace removes extra chars, keeps: "digits + - . ,"
        - second regex changes "," to "."
        - Nullif makes sure that if regexes return empty string then Null is returned instead
        """
        regex = self.escape_regex(r"'[^-0-9\.\,]'")
        return f"CAST(NULLIF(REGEXP_REPLACE(REGEXP_REPLACE({column_name}, {regex}, ''{self.regex_replace_flags()}), ',', '.'{self.regex_replace_flags()}), '') AS {self.SQL_TYPE_FOR_CREATE_TABLE_MAP[DataType.DECIMAL]})"

    def regex_replace_flags(self) -> str:
        return ", 'g'"

    def escape_string(self, value: str):
        return re.sub(r"(\\.)", r"\\\1", value)

    def escape_regex(self, value: str):
        return value

    def get_max_aggregation_fields(self):
        """
        Max number of fields to be aggregated in 1 aggregation query
        """
        return 50

    def connect(self):
        """
        Subclasses use self.data_source_properties to initialize self.connection with a PEP 249 connection

        Any BaseException may be raised in case of errors.
        The caller of this method will catch the exception and add an error log to the scan.
        """
        raise NotImplementedError(f"TODO: Implement {type(self)}.connect()")

    def fetchall(self, sql: str):
        # TODO: Deprecated - not used, use Query object instead.
        try:
            cursor = self.connection.cursor()
            try:
                self.logs.info(f"Query: \n{sql}")
                cursor.execute(sql)
                return cursor.fetchall()
            finally:
                cursor.close()
        except BaseException as e:
            self.logs.error(f"Query error: {e}\n{sql}", exception=e)
            self.query_failed(e)

    def is_connected(self):
        return self.connection is not None

    def disconnect(self) -> None:
        if self.connection:
            self.connection.close()
            # self.connection = None is used in self.is_connected
            self.connection = None

    def commit(self):
        self.connection.commit()

    def query_failed(self, e: BaseException):
        self.rollback()

    def rollback(self):
        self.connection.rollback()

    def default_casify_table_name(self, identifier: str) -> str:
        """Formats table identifier to e.g. a default case for a given data source."""
        return identifier

    def default_casify_column_name(self, identifier: str) -> str:
        """Formats column identifier to e.g. a default case for a given data source."""
        return identifier

    def default_casify_type_name(self, identifier: str) -> str:
        """Formats type identifier to e.g. a default case for a given data source."""
        return identifier

    def safe_connection_data(self):
        """Return non-critically sensitive connection details.

        Useful for debugging.
        """
        # to be overridden by subclass

    def generate_hash_safe(self):
        """Generates a safe hash from non-sensitive connection details.

        Useful for debugging, identifying data sources anonymously and tracing.
        """
        data = self.safe_connection_data()

        return self.hash_data(data)

    def hash_data(self, data) -> str:
        """Hash provided data using a non-reversible hashing algorithm."""
        encoded = json.dumps(data, sort_keys=True).encode()
        return hashlib.sha256(encoded).hexdigest()

    def test(self, sql):
        import logging
        import textwrap

        from soda.sampler.log_sampler import LogSampler
        from soda.sampler.sample_schema import SampleColumn

        cursor = self.connection.cursor()
        try:
            indented_sql = textwrap.indent(text=sql, prefix="  #   ")
            logging.debug(f"  # Query: \n{indented_sql}")
            cursor.execute(sql)
            rows = cursor.fetchall()

            columns = SampleColumn.create_sample_columns(cursor.description, self)
            table, row_count, col_count = LogSampler.pretty_print(rows, columns)
            logging.debug(f"  # Query result: \n{table}")

        except Exception as e:
            logging.error(f"Error: {e}", e)

        finally:
            cursor.close()

    def sql_select_column_with_filter_and_limit(
        self, column_name: str, table_name: str, filter_clause: str, limit: int | None = None
    ) -> str:
        limit_str = ""
        if limit:
            limit_str = f"\n LIMIT {limit}"

        sql = f"SELECT \n" f"  {column_name} \n" f"FROM {table_name}{filter_clause}{limit_str}"
        return sql

    def expr_false_condition(self):
        return "FALSE"
