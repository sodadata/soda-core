import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import cached_property
from typing import Optional

from google.api_core.exceptions import Forbidden, NotFound
from soda_bigquery.common.data_sources.bigquery_data_source_connection import (
    BigQueryDataSource as BigQueryDataSourceModel,
)
from soda_bigquery.common.data_sources.bigquery_data_source_connection import BigQueryDataSourceConnection
from soda_core.common.data_source_connection import DataSourceConnection
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import DataSourceNamespace, SodaDataTypeName
from soda_core.common.sql_ast import (
    ADD_INTERVAL,
    COLUMN,
    CONCAT_WS,
    COUNT,
    CREATE_TABLE,
    CREATE_TABLE_IF_NOT_EXISTS,
    DISTINCT,
    LITERAL,
    PERCENTILE_WITHIN_GROUP,
    RANDOM,
    STRING_HASH,
    TIME_DELTA,
    TUPLE,
    UNION,
    UNION_ALL,
    VALUES,
    WITH,
)
from soda_core.common.sql_dialect import SqlDialect
from soda_core.common.statements.metadata_primary_keys_query import MetadataPrimaryKeysQuery
from soda_core.common.statements.metadata_tables_query import MetadataTablesQuery
from soda_core.common.statements.table_types import FullyQualifiedObjectName, TableType

logger: logging.Logger = soda_logger

# tables.list reports table types in its own spelling. Map them to the INFORMATION_SCHEMA.TABLES spelling,
# so rows from the API go through the same conversion as rows from the region view. Types that are spelled
# the same in both (VIEW, EXTERNAL, SNAPSHOT) pass through unchanged.
_API_TABLE_TYPE_TO_INFORMATION_SCHEMA_TABLE_TYPE: dict[str, str] = {
    "TABLE": "BASE TABLE",
    "MATERIALIZED_VIEW": "MATERIALIZED VIEW",
}


def find_information_schema_access_denied(exception: BaseException) -> Optional[Forbidden]:
    """The 403 BigQuery returns when the account may not read an INFORMATION_SCHEMA view, or None.

    The BigQuery DB-API wraps the API error (``DatabaseError(Forbidden(...))``), so the exception's
    arguments and its cause/context chain are searched as well.

    Only access-denied errors on an INFORMATION_SCHEMA view qualify. Another 403 is a different problem that
    the API fallback must not hide. A missing ``bigquery.jobs.create`` in the project that runs the job means
    the account cannot run any query there, so contract verification would fail anyway. An exceeded quota or
    rate limit is also a 403, and says nothing about what the account may read.

    Both ways the BigQuery client raises a query's 403 keep BigQuery's error reason: a refused ``jobs.query``
    call carries the response's ``error.errors``, and a failed job carries its ``errorResult``.
    """
    pending: list[Optional[BaseException]] = [exception]
    seen: set[int] = set()
    while pending:
        current = pending.pop()
        if current is None or id(current) in seen:
            continue
        seen.add(id(current))
        if (
            isinstance(current, Forbidden)
            and is_access_denied(current)
            and "INFORMATION_SCHEMA" in (current.message or "")
        ):
            return current
        pending.extend([current.__cause__, current.__context__])
        pending.extend(argument for argument in current.args if isinstance(argument, BaseException))
    return None


def sql_like_to_regex(like_pattern: str) -> re.Pattern:
    """Compile a SQL LIKE pattern to a case-insensitive regex that matches the whole name.

    ``%`` matches any run of characters, ``_`` matches one character, and a backslash escapes the next
    character, as in BigQuery's LIKE. The metadata query lowercases both sides of its LIKE, so matching
    ignores case here too.
    """
    regex_parts: list[str] = []
    index = 0
    while index < len(like_pattern):
        character = like_pattern[index]
        if character == "\\" and index + 1 < len(like_pattern):
            index += 1
            regex_parts.append(re.escape(like_pattern[index]))
        elif character == "%":
            regex_parts.append(".*")
        elif character == "_":
            regex_parts.append(".")
        else:
            regex_parts.append(re.escape(character))
        index += 1
    return re.compile("".join(regex_parts), re.IGNORECASE | re.DOTALL)


class TableNameLikeFilters:
    """The metadata query's table-name filters, applied in Python: any include matches, and no exclude does.

    The LIKE patterns are compiled once, when the filters are built, not for every table name matched.
    """

    def __init__(
        self,
        include_table_name_like_filters: Optional[list[str]],
        exclude_table_name_like_filters: Optional[list[str]],
    ):
        self.include_regexes: list[re.Pattern] = [
            sql_like_to_regex(like_filter) for like_filter in include_table_name_like_filters or []
        ]
        self.exclude_regexes: list[re.Pattern] = [
            sql_like_to_regex(like_filter) for like_filter in exclude_table_name_like_filters or []
        ]

    def matches(self, table_name: str) -> bool:
        if self.include_regexes and not any(regex.fullmatch(table_name) for regex in self.include_regexes):
            return False
        return not any(regex.fullmatch(table_name) for regex in self.exclude_regexes)


def is_access_denied(forbidden: Forbidden) -> bool:
    """Whether a 403 from the BigQuery API means the account lacks permission, as opposed to a quota or rate limit.

    BigQuery also answers ``quotaExceeded`` and ``rateLimitExceeded`` with a 403. Those are passing conditions on
    a resource the account may well be allowed to read, so they must not be taken for a missing permission.
    """
    return any(error.get("reason") == "accessDenied" for error in forbidden.errors or [])


@dataclass
class BigQueryDataSourceNamespace(DataSourceNamespace):
    project_id: str
    dataset: str
    location: Optional[str] = None

    def get_namespace_elements(self) -> list[str]:
        if self.location:
            return [self.location, self.project_id]
        else:
            return [self.project_id, self.dataset]

    def get_database_for_metadata_query(self) -> str | None:
        return self.project_id

    def get_schema_for_metadata_query(self) -> str:
        return self.dataset


class BigQueryMetadataPrimaryKeysQuery(MetadataPrimaryKeysQuery):
    """Primary-key introspection for BigQuery.

    BigQuery's INFORMATION_SCHEMA is qualified as
    ``<project>.<dataset>.INFORMATION_SCHEMA.TABLE_CONSTRAINTS`` and its namespace
    elements are read from a BigQueryDataSourceNamespace, so the dataset prefixes
    ([project, dataset]) are mapped onto that namespace here.
    """

    def _build_namespace(self, prefixes: list[str]) -> DataSourceNamespace:
        return BigQueryDataSourceNamespace(project_id=prefixes[0], dataset=prefixes[1])


class BigQueryMetadataTablesQuery(MetadataTablesQuery):
    """Tables-metadata query that resolves BigQuery regions at execution time.

    BigQuery's INFORMATION_SCHEMA.TABLES only exists dataset- or region-qualified, and a
    region view only contains that region's datasets. The regions to query are derived from
    dataset metadata by the data source impl and the query runs once per region, so the
    region is never guessed from where a probe query happens to run.

    Each region view is read from the project in scope, or from ``storage_project_id`` when
    the scope names no project.

    A region view refuses the whole query when the account lacks table or routine permissions on
    any dataset in the region. On that 403 the region's tables are listed with the BigQuery API
    instead (``datasets.list`` + ``tables.list``), which only needs access to the datasets themselves.
    """

    def __init__(
        self,
        sql_dialect: SqlDialect,
        data_source_connection: DataSourceConnection,
        data_source_impl: "BigQueryDataSourceImpl",
    ):
        super().__init__(sql_dialect=sql_dialect, data_source_connection=data_source_connection)
        self.data_source_impl = data_source_impl

    def execute(
        self,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        include_table_name_like_filters: Optional[list[str]] = None,
        exclude_table_name_like_filters: Optional[list[str]] = None,
        types_to_return: Optional[list[TableType]] = None,
    ) -> list[FullyQualifiedObjectName]:
        # A region view without a project in front reads the project that runs the query job, which is the compute
        # project. So name the project in scope, or the storage project, where the data lives, when the scope names
        # none (as in unscoped discovery). Only with no project known at all does the view stay unqualified.
        project_id: Optional[str] = database_name or self.data_source_impl.storage_project_id
        project_qualifier: list[str] = [project_id] if project_id else []
        results: list[FullyQualifiedObjectName] = []
        for region in self.data_source_impl.regions_in_scope(project_id=project_id, dataset_id=schema_name):
            self.prefixes = [*project_qualifier, f"region-{region}"]
            try:
                results.extend(
                    super().execute(
                        database_name=database_name,
                        schema_name=schema_name,
                        include_table_name_like_filters=include_table_name_like_filters,
                        exclude_table_name_like_filters=exclude_table_name_like_filters,
                        types_to_return=types_to_return,
                    )
                )
            except Exception as exception:
                access_denied: Optional[Forbidden] = find_information_schema_access_denied(exception)
                if access_denied is None:
                    raise
                results.extend(
                    self._execute_with_api(
                        project_id=project_id,
                        region=region,
                        schema_name=schema_name,
                        include_table_name_like_filters=include_table_name_like_filters,
                        exclude_table_name_like_filters=exclude_table_name_like_filters,
                        types_to_return=types_to_return,
                        access_denied=access_denied,
                    )
                )
        return results

    def _execute_with_api(
        self,
        project_id: Optional[str],
        region: str,
        schema_name: Optional[str],
        include_table_name_like_filters: Optional[list[str]],
        exclude_table_name_like_filters: Optional[list[str]],
        types_to_return: Optional[list[TableType]],
        access_denied: Forbidden,
    ) -> list[FullyQualifiedObjectName]:
        """List the region's tables with the BigQuery API after its INFORMATION_SCHEMA view refused the account.

        A region view needs table and routine permissions on every dataset in the region. It does not filter
        out the datasets the account cannot read: it refuses the whole query. ``datasets.list`` and
        ``tables.list`` only need access to the datasets themselves and return exactly those, so an account
        that is only granted some datasets still gets its tables.
        """
        logger.warning(
            f"Reading BigQuery region '{region}' INFORMATION_SCHEMA was denied, so its tables are listed with the "
            f"BigQuery API instead. This only lists the datasets the account has access to. Reason: "
            f"{access_denied.message}"
        )
        table_name_like_filters = TableNameLikeFilters(
            include_table_name_like_filters=include_table_name_like_filters,
            exclude_table_name_like_filters=exclude_table_name_like_filters,
        )
        rows: list[tuple] = [
            row
            for row in self.data_source_impl.table_rows_from_api(
                project_id=project_id, region=region, dataset_id=schema_name
            )
            if table_name_like_filters.matches(row[2])
        ]
        return self.get_results(
            QueryResult(rows=rows, columns=None),
            types_to_return if types_to_return is not None else [TableType.TABLE],
        )


class BigQueryDataSourceImpl(DataSourceImpl, model_class=BigQueryDataSourceModel):
    def __init__(self, data_source_model: BigQueryDataSourceModel, connection: Optional[DataSourceConnection] = None):
        super().__init__(data_source_model=data_source_model, connection=connection)
        self._dataset_region_cache: dict[tuple[str, str], str] = {}

    def _create_sql_dialect(self) -> SqlDialect:
        return BigQuerySqlDialect()

    def _create_data_source_connection(self) -> DataSourceConnection:
        return BigQueryDataSourceConnection(
            name=self.data_source_model.name, connection_properties=self.data_source_model.connection_properties
        )

    @cached_property
    def storage_project_id(self) -> Optional[str]:
        """The project that holds the data. The connection falls back to the compute project when none is configured.

        Read it here, not from the connection: on a replay connection the attribute read would open a real
        connection, so snapshot tests pin this value on the impl instead. That is why this is a cached_property:
        unlike a plain property, a test can set it on the instance.
        """
        return self.data_source_connection.storage_project_id

    def region_for_dataset(self, project_id: Optional[str], dataset_id: str) -> str:
        """Get the BigQuery location of a dataset from its metadata (a REST call, not a query job)."""
        configured_location = self.data_source_model.connection_properties.location
        if configured_location:
            return configured_location
        return self._dataset_location(project_id, dataset_id)

    def _dataset_location(self, project_id: Optional[str], dataset_id: str) -> str:
        """The location the dataset actually lives in, whatever location the connection configures."""
        project_id = project_id or self.data_source_connection.project_id
        cache_key = (project_id, dataset_id)
        if cache_key not in self._dataset_region_cache:
            dataset = self.data_source_connection.client.get_dataset(f"{project_id}.{dataset_id}")
            self._dataset_region_cache[cache_key] = dataset.location
        return self._dataset_region_cache[cache_key]

    def regions_in_scope(self, project_id: Optional[str] = None, dataset_id: Optional[str] = None) -> list[str]:
        """The BigQuery regions metadata queries must cover for the given scope.

        A configured location pins the single region. A known dataset resolves to its own
        region. Otherwise the project's datasets are listed (a project-scoped REST call that
        spans all regions) and their distinct locations are returned.

        A dataset that does not exist has no region and no tables, so it puts nothing in scope.
        Callers do ask about datasets that are not there yet: the diagnostics warehouse dataset is
        looked up to see what it already holds before anything has created it. They expect an empty
        answer, not a 404.
        """
        configured_location = self.data_source_model.connection_properties.location
        if configured_location:
            return [configured_location]
        if dataset_id:
            try:
                return [self.region_for_dataset(project_id, dataset_id)]
            except NotFound:
                logger.debug(f"BigQuery dataset '{dataset_id}' does not exist, so no region is in scope for it.")
                return []
        project_id = project_id or self.data_source_connection.project_id
        regions: list[str] = []
        for listed_dataset_id in self._list_dataset_ids(project_id):
            region = self.region_for_dataset(project_id, listed_dataset_id)
            if region not in regions:
                regions.append(region)
        if not regions:
            logger.warning(
                f"No datasets visible in BigQuery project '{project_id}'. Check the connection "
                "configuration and the permissions of the account used, or set 'location' in the "
                "connection configuration."
            )
        return regions

    def _list_dataset_ids(self, project_id: Optional[str]) -> list[str]:
        """The ids of the project's datasets that the account can see (``datasets.list``, one call per page)."""
        dataset_ids: list[str] = []
        for dataset_item in self.data_source_connection.client.list_datasets(project=project_id):
            # DatasetListItem has no location property, but the raw listing resource carries it.
            # Seeding the cache with it spares a datasets.get per dataset.
            listed_location = dataset_item._properties.get("location")
            if listed_location:
                self._dataset_region_cache[(project_id, dataset_item.dataset_id)] = listed_location
            dataset_ids.append(dataset_item.dataset_id)
        return dataset_ids

    def table_rows_from_api(
        self, project_id: Optional[str], region: str, dataset_id: Optional[str] = None
    ) -> list[tuple[str, str, str, str]]:
        """The tables of one region, listed with the BigQuery API instead of its INFORMATION_SCHEMA view.

        Rows have the shape of the metadata query's rows: (project, dataset, table name, INFORMATION_SCHEMA
        table type). With a dataset in scope only that dataset is considered. Otherwise the project's datasets are
        listed. Either way only datasets that actually live in the region are kept, as in the region view. That
        holds with a configured location too: it names the region view to read, it does not move other datasets
        into it.

        A dataset that is gone by the time it is listed has no tables and is skipped. A dataset whose tables the
        account may not list is skipped with a warning, and the other datasets are still listed. Any other error,
        such as an exceeded quota, fails the listing: an incomplete listing would make datasets look deleted.
        """
        project_id = project_id or self.data_source_connection.project_id
        candidate_dataset_ids: list[str] = [dataset_id] if dataset_id else self._list_dataset_ids(project_id)
        rows: list[tuple[str, str, str, str]] = []
        for candidate_dataset_id in candidate_dataset_ids:
            if self._dataset_is_in_region(project_id, candidate_dataset_id, region):
                rows.extend(self._table_rows_of_dataset(project_id, candidate_dataset_id))
        return rows

    def _dataset_is_in_region(self, project_id: Optional[str], dataset_id: str, region: str) -> bool:
        """Whether the dataset lives in the region. A dataset that is gone lives nowhere."""
        try:
            dataset_location: str = self._dataset_location(project_id, dataset_id)
        except NotFound:
            logger.debug(f"BigQuery dataset '{dataset_id}' does not exist, so it has no tables to list.")
            return False
        # BigQuery location names are case-insensitive ('EU' and 'eu' are the same multi-region).
        return dataset_location.lower() == region.lower()

    def _table_rows_of_dataset(self, project_id: Optional[str], dataset_id: str) -> list[tuple[str, str, str, str]]:
        """The dataset's tables (``tables.list``) as metadata rows.

        Empty when the dataset is gone or the account may not list its tables. Any other error propagates.
        """
        dataset_reference: str = f"{project_id}.{dataset_id}" if project_id else dataset_id
        try:
            table_items = list(self.data_source_connection.client.list_tables(dataset_reference))
        except NotFound:
            logger.debug(f"BigQuery dataset '{dataset_reference}' does not exist, so it has no tables to list.")
            return []
        except Forbidden as forbidden:
            if not is_access_denied(forbidden):
                raise
            logger.warning(
                f"Skipping BigQuery dataset '{dataset_reference}': the account may not list its tables. "
                f"Reason: {forbidden.message}"
            )
            return []
        return [
            (
                table_item.project,
                table_item.dataset_id,
                table_item.table_id,
                _API_TABLE_TYPE_TO_INFORMATION_SCHEMA_TABLE_TYPE.get(table_item.table_type, table_item.table_type),
            )
            for table_item in table_items
        ]

    def create_metadata_tables_query(self) -> MetadataTablesQuery:
        return BigQueryMetadataTablesQuery(
            sql_dialect=self.sql_dialect,
            data_source_connection=self.data_source_connection,
            data_source_impl=self,
        )

    def _build_columns_metadata_namespace(self, prefixes: list[str]) -> DataSourceNamespace:
        return BigQueryDataSourceNamespace(project_id=prefixes[0], dataset=prefixes[1])

    def create_metadata_primary_keys_query(self) -> MetadataPrimaryKeysQuery:
        # BigQuery exposes primary keys through the dataset-qualified INFORMATION_SCHEMA constraint views.
        return BigQueryMetadataPrimaryKeysQuery(
            sql_dialect=self.sql_dialect, data_source_connection=self.data_source_connection
        )

    def _build_table_namespace_for_schema_query(self, prefixes: list[str]) -> tuple[DataSourceNamespace, str]:
        table_namespace: DataSourceNamespace = BigQueryDataSourceNamespace(
            project_id=prefixes[0],
            dataset=None,  # Schema existence queries need project-level INFORMATION_SCHEMA
        )
        schema_name = self.sql_dialect.extract_schema_from_prefix(prefixes)
        if schema_name is None:
            raise ValueError(f"Cannot determine schema name from prefixes: {prefixes}")

        return table_namespace, schema_name


class BigQuerySqlDialect(SqlDialect, sqlglot_dialect="bigquery"):
    DEFAULT_QUOTE_CHAR = "`"

    SODA_DATA_TYPE_SYNONYMS = (
        (SodaDataTypeName.TEXT, SodaDataTypeName.VARCHAR, SodaDataTypeName.CHAR),
        (SodaDataTypeName.INTEGER, SodaDataTypeName.BIGINT, SodaDataTypeName.SMALLINT),
        (SodaDataTypeName.NUMERIC, SodaDataTypeName.DECIMAL),
        (SodaDataTypeName.TIMESTAMP, SodaDataTypeName.TIMESTAMP_TZ),  # Bigquery does not support timezones
        (SodaDataTypeName.FLOAT, SodaDataTypeName.DOUBLE),
    )

    def get_data_source_data_type_name_by_soda_data_type_names(self) -> dict[str, str]:
        return {
            SodaDataTypeName.CHAR: "string",  # BigQuery only has STRING
            SodaDataTypeName.VARCHAR: "string",
            SodaDataTypeName.TEXT: "string",  # alias for varchar
            SodaDataTypeName.SMALLINT: "int64",  # all integers → INT64
            SodaDataTypeName.INTEGER: "int64",
            SodaDataTypeName.BIGINT: "int64",
            SodaDataTypeName.DECIMAL: "numeric",  # NUMERIC (fixed precision: 38 digits, 9 decimals)
            SodaDataTypeName.NUMERIC: "numeric",
            SodaDataTypeName.FLOAT: "float64",  # FLOAT & DOUBLE → FLOAT64
            SodaDataTypeName.DOUBLE: "float64",
            SodaDataTypeName.TIMESTAMP: "timestamp",  # UTC-based
            SodaDataTypeName.TIMESTAMP_TZ: "timestamp",  # still just TIMESTAMP in BigQuery
            SodaDataTypeName.DATE: "date",
            SodaDataTypeName.TIME: "time",
            SodaDataTypeName.BOOLEAN: "bool",
        }

    def get_soda_data_type_name_by_data_source_data_type_names(self) -> dict[str, SodaDataTypeName]:
        return {
            "string": SodaDataTypeName.VARCHAR,
            "smallint": SodaDataTypeName.SMALLINT,
            "int64": SodaDataTypeName.BIGINT,
            "numeric": SodaDataTypeName.NUMERIC,
            "float64": SodaDataTypeName.DOUBLE,
            "double precision": SodaDataTypeName.DOUBLE,
            "timestamp": SodaDataTypeName.TIMESTAMP,
            "date": SodaDataTypeName.DATE,
            "time": SodaDataTypeName.TIME,
            "bool": SodaDataTypeName.BOOLEAN,
            # BigQuery DATETIME is a naive (timezone-less) civil timestamp → canonical TIMESTAMP.
            "datetime": SodaDataTypeName.TIMESTAMP,
            "bignumeric": SodaDataTypeName.NUMERIC,
            # Defensive aliases — unreachable via INFORMATION_SCHEMA (BigQuery canonicalizes
            # them to INT64/NUMERIC/BIGNUMERIC/BOOL); consistent with _get_data_type_name_synonyms.
            "int": SodaDataTypeName.BIGINT,
            "integer": SodaDataTypeName.BIGINT,
            "bigint": SodaDataTypeName.BIGINT,
            "tinyint": SodaDataTypeName.SMALLINT,
            "decimal": SodaDataTypeName.NUMERIC,
            "bigdecimal": SodaDataTypeName.NUMERIC,
            "boolean": SodaDataTypeName.BOOLEAN,
        }

    def _get_data_type_name_synonyms(self) -> list[list[str]]:
        return [
            ["int64", "integer"],
            ["bool", "boolean"],
            ["numeric", "decimal"],
            ["bignumeric", "bigdecimal"],
        ]

    def get_large_numeric_cast_type_name(self) -> Optional[str]:
        """CAST aggregate args to NUMERIC so math over INT64 runs in NUMERIC(38,9),
        not FLOAT64."""
        return "NUMERIC"

    def build_union_sql(self, union: UNION | UNION_ALL, add_semicolon: Optional[bool] = None) -> str:
        """BigQuery rejects bare UNION (requires UNION ALL | UNION DISTINCT).
        Render UNION_ALL as UNION ALL and plain UNION as UNION DISTINCT so the
        set semantics of a plain UNION are preserved for any caller — profiling's
        unioned sets carry disjoint constant metric_ labels, so for that caller
        ALL vs DISTINCT is identical either way.
        """
        add_semicolon = self.apply_default_add_semicolon(add_semicolon)
        union_sql: str = "UNION ALL" if isinstance(union, UNION_ALL) else "UNION DISTINCT"
        return f"\n{union_sql}\n".join(
            [
                f"(\n{self.build_select_sql(select_element, add_semicolon=False)}\n)"
                for select_element in union.select_elements
            ]
        ) + (";" if add_semicolon else "")

    def format_metadata_data_type(self, data_type: str) -> str:
        """Strip the parameter suffix BigQuery INFORMATION_SCHEMA reports (e.g.
        ``NUMERIC(20, 4)``) so type-map lookups match; mirrors the DuckDB dialect."""
        paranthesis_index = data_type.find("(")
        if paranthesis_index != -1:
            return data_type[:paranthesis_index]
        return data_type

    def information_schema_namespace_elements(self, data_source_namespace: BigQueryDataSourceNamespace) -> list[str]:
        return [data_source_namespace.project_id, data_source_namespace.dataset, "INFORMATION_SCHEMA"]

    def create_schema_if_not_exists_sql(self, prefixes: list[str], add_semicolon: Optional[bool] = None) -> str:
        # BigQuery datasets are project-scoped, so qualify the dataset with the project
        # (prefixes[0]) — otherwise it lands in the client's default (compute) project
        # instead of the storage project. The base implementation uses the dataset name alone.
        add_semicolon = self.apply_default_add_semicolon(add_semicolon)
        # Prefixes are produced by the BigQuery data source as [project, dataset]; a different
        # shape is a caller bug, so fail fast with a clear, traceable error.
        if len(prefixes) != 2:
            raise ValueError(f"BigQuery CREATE SCHEMA expects [project, dataset] prefixes, got {prefixes}")
        project_id, dataset = prefixes
        qualified_schema_name: str = f"{self.quote_for_ddl(project_id)}.{self.quote_for_ddl(dataset)}"
        return f"CREATE SCHEMA IF NOT EXISTS {qualified_schema_name}" + (";" if add_semicolon else "")

    def supports_primary_keys(self) -> bool:
        # BigQuery stores non-enforced primary keys and reports them through the
        # dataset-qualified INFORMATION_SCHEMA constraint views.
        return True

    def _build_create_table_primary_key(self, create_table: CREATE_TABLE | CREATE_TABLE_IF_NOT_EXISTS) -> Optional[str]:
        # BigQuery only accepts a primary key declared as NOT ENFORCED.
        primary_key_column_names = getattr(create_table, "primary_key_column_names", None)
        if not primary_key_column_names:
            return None
        quoted_columns: str = ", ".join(
            self._quote_column_for_create_table(column_name) for column_name in primary_key_column_names
        )
        return f"\tPRIMARY KEY ({quoted_columns}) NOT ENFORCED"

    def default_casify(self, identifier: str) -> str:
        return identifier.upper()

    def _build_tuple_sql(self, tuple: TUPLE) -> str:
        if tuple.check_context(COUNT) and tuple.check_context(DISTINCT):
            return self._build_tuple_sql_in_distinct(tuple)
        return f"{super()._build_tuple_sql(tuple)}"

    def _build_tuple_sql_in_distinct(self, tuple: TUPLE) -> str:
        return f"TO_JSON_STRING(STRUCT({super()._build_tuple_sql(tuple)}))"

    def _regex_like_sql(self, expression: str, pattern: str) -> str:
        # `pattern` arrives quoted by this dialect's own literal_string(), the
        # triple-quoted form that escapes the backslash as \\ and an apostrophe as \'.
        return f"REGEXP_CONTAINS({expression}, {pattern})"

    def _build_percentile_within_group_sql(self, percentile_within_group: PERCENTILE_WITHIN_GROUP) -> str:
        """BigQuery has no ordered-set aggregates; render as
        ``APPROX_QUANTILES({expr}, 1000)[{int(p*1000)}]``."""
        expression_sql: str = self.build_expression_sql(percentile_within_group.expression)
        quantile_number: int = int(percentile_within_group.percentile * 1000)
        return f"APPROX_QUANTILES({expression_sql}, 1000)[{quantile_number}]"

    # Singular unit names for DATETIME_ADD. WEEK is deliberately absent: weekly
    # arithmetic renders day-denominated in both builders below, keeping
    # cross-dialect bucket indexes identical.
    _TIME_BUCKET_UNIT_NAMES: dict = {
        "days": "DAY",
        "hours": "HOUR",
        "seconds": "SECOND",
    }

    _TIME_BUCKET_UNIT_MICROSECONDS: dict = {
        "days": 24 * 60 * 60 * 1_000_000,
        "hours": 60 * 60 * 1_000_000,
        "seconds": 1_000_000,
    }

    def _build_time_delta_sql(self, time_delta: TIME_DELTA) -> str:
        start_sql: str = self.build_expression_sql(time_delta.start)
        end_sql: str = self.build_expression_sql(time_delta.end)
        # Weekly buckets index as day-buckets of 7×count: floor(days / 7·count)
        # is the same partition index.
        unit, count = time_delta.unit, time_delta.count
        if unit == "weeks":
            unit, count = "days", count * 7
        # Elapsed MICROSECONDs floor-divided by the unit size: DATETIME_DIFF at
        # DAY/HOUR counts calendar-boundary crossings (a 2h span across midnight
        # is 1 DAY), which would mis-bucket grids anchored off midnight.
        unit_microseconds: int = self._TIME_BUCKET_UNIT_MICROSECONDS[unit] * count
        sql: str = f"DATETIME_DIFF({end_sql}, {start_sql}, MICROSECOND)"
        return f"CAST(FLOOR({sql} / {unit_microseconds}) AS INT)"

    def _build_add_interval_sql(self, add_interval: ADD_INTERVAL) -> str:
        timestamp_sql: str = self.build_expression_sql(add_interval.timestamp)
        count_sql: str = self.build_expression_sql(add_interval.count_expression)
        # Weekly intervals render as INTERVAL <count> * 7 DAY.
        if add_interval.unit == "weeks":
            return f"DATETIME_ADD({timestamp_sql}, INTERVAL {count_sql} * 7 DAY)"
        unit_name: str = self._TIME_BUCKET_UNIT_NAMES[add_interval.unit]
        return f"DATETIME_ADD({timestamp_sql}, INTERVAL {count_sql} {unit_name})"

    def supports_data_type_character_maximum_length(self) -> bool:
        return False

    def supports_data_type_numeric_precision(self) -> bool:
        return False

    def supports_cte_alias_columns(self) -> bool:
        return False

    def supports_data_type_numeric_scale(self) -> bool:
        return False

    def supports_data_type_datetime_precision(self) -> bool:
        return False

    # BigQuery's NUMERIC / BIGNUMERIC are fixed-precision (no per-column tuning),
    # so INFORMATION_SCHEMA.COLUMNS doesn't expose NUMERIC_PRECISION / NUMERIC_SCALE.
    # Hard-code BigQuery's documented values so consumers of the column metadata
    # (e.g. cross-source CREATE TABLE on dialects that *do* require explicit
    # precision/scale) carry the correct decimal scale through; otherwise a
    # `numeric` column on the target defaults to scale 0 and silently truncates
    # the fractional part of every value.
    #
    # Note: these overrides intentionally bypass the base layer's
    # `data_type_has_parameter_numeric_*` / `supports_data_type_numeric_*` gates
    # (both return False for BigQuery in the CREATE TABLE path). The asymmetry
    # is source-vs-target: BigQuery-as-source needs to *carry* the precision so
    # downstream target dialects that do require it can render correctly, even
    # though BigQuery-as-target would never render it itself.
    _BIGQUERY_NUMERIC_PRECISION = 38
    _BIGQUERY_NUMERIC_SCALE = 9
    _BIGQUERY_BIGNUMERIC_PRECISION = 76
    _BIGQUERY_BIGNUMERIC_SCALE = 38

    def extract_numeric_precision(self, row, columns) -> Optional[int]:
        data_type_name: str = self.extract_data_type_name(row, columns).lower()
        if data_type_name == "numeric":
            return self._BIGQUERY_NUMERIC_PRECISION
        if data_type_name == "bignumeric":
            return self._BIGQUERY_BIGNUMERIC_PRECISION
        return super().extract_numeric_precision(row, columns)

    def extract_numeric_scale(self, row, columns) -> Optional[int]:
        data_type_name: str = self.extract_data_type_name(row, columns).lower()
        if data_type_name == "numeric":
            return self._BIGQUERY_NUMERIC_SCALE
        if data_type_name == "bignumeric":
            return self._BIGQUERY_BIGNUMERIC_SCALE
        return super().extract_numeric_scale(row, columns)

    def sql_expr_timestamp_literal(self, datetime_in_iso8601: str) -> str:
        return f"timestamp('{datetime_in_iso8601}')"

    def sql_expr_timestamp_coerce(self, expr: str) -> str:
        # CAST, not the timestamp() constructor: only CAST on the partitioning column is
        # accepted for partition elimination (a function wrap full-scans, and tables with
        # require_partition_filter reject the query outright). DATETIME as the target
        # because the cast compiles for DATE, DATETIME and TIMESTAMP expressions alike,
        # converting TIMESTAMP -> DATETIME at UTC; literal_datetime renders the matching
        # offset-less UTC bounds.
        return f"CAST({expr} AS DATETIME)"

    def literal_datetime(self, dt: datetime) -> str:
        # Offset-less UTC: an offset-carrying ISO string cannot coerce to a DATETIME
        # column, nor to the coerce hook's CAST(... AS DATETIME). Stays untyped so it
        # still coerces to TIMESTAMP columns (e.g. INFORMATION_SCHEMA.JOBS.end_time).
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return f"'{dt.isoformat()}'"

    def literal_timestamp_typed(self, dt: datetime) -> str:
        # Untyped: BigQuery coerces string literals inside DATETIME_DIFF/DATETIME_ADD
        # (the anchor positions) AND against raw TIMESTAMP/DATETIME columns in
        # user-authored custom metric SQL, where a typed DATETIME literal breaks the
        # 'col BETWEEN ${soda.PARTITION_START_TIME} ...' pattern on TIMESTAMP columns.
        return f"'{self._typed_timestamp_str(dt)}'"

    def sql_expr_timestamp_truncate_day(self, timestamp_literal: str) -> str:
        return f"date_trunc(timestamp({timestamp_literal}), day)"

    def sql_expr_timestamp_add_day(self, timestamp_literal: str) -> str:
        return f"{timestamp_literal} + interval 1 day"

    def literal_string(self, value: str) -> Optional[str]:
        if value is None:
            return None
        # BigQuery triple-quoted strings ('''...''') allow multiline content, so the
        # wrapper keeps raw newlines in multi-line check SQL from needing escaping.
        # Escape the backslash first, then EVERY single quote as \'. Escaping every
        # quote guarantees that no unescaped ''' can appear inside the content and
        # prematurely close the string (which would make BigQuery see two adjacent
        # string literals -> "concatenated string literals must be separated by
        # whitespace or comments"). Only escaping ''' runs was not enough: values
        # ending in a quote, or containing runs of 4/5/7/8 quotes, still produced a
        # bare ''' at or near the wrapper boundary.
        escaped = value.replace("\\", "\\\\").replace("'", "\\'")
        return "'''" + escaped + "'''"

    def build_cte_values_sql(self, values: VALUES, alias_columns: list[COLUMN] | None) -> str:
        # The first select row should have column aliases
        # Remaining rows don't need aliases
        def build_literal_with_alias(literal, alias: COLUMN | None) -> str:
            return f"{self.literal(literal)} AS {self.quote_column(alias.name)}"

        literal_rows: list[str] = []
        for tuple in values.values:
            if alias_columns:
                literal_sqls: list[str] = []
                for i in range(len(tuple.expressions)):
                    literal: LITERAL = tuple.expressions[i]
                    alias: COLUMN = alias_columns[i]
                    literal_sqls.append(build_literal_with_alias(literal=literal, alias=alias))
                literal_rows.append(", ".join(literal_sql for literal_sql in literal_sqls))
                alias_columns = None
            else:
                literal_rows.append(", ".join(self.build_expression_sql(e) for e in tuple.expressions))

        select_rows: list[str] = [f"SELECT {literal_row}" for literal_row in literal_rows]

        return "\nUNION ALL ".join([select_row for select_row in select_rows])

    def _build_cte_with_sql_line(self, with_element: WITH) -> str:
        return f"WITH {self.quote_default(with_element.alias)} AS ("

    def get_max_sql_statement_length(self) -> int:
        # This is a VERY conservative limit. We can probably get away with more
        # However, BigQuery started complaining that the statement was "too complicated" with a length of 1MB.
        return 200 * 1024  # 200KB

    def get_preferred_number_of_rows_for_insert(self) -> int:
        return 500

    def _build_concat_ws_sql(self, concat_ws: CONCAT_WS) -> str:
        elements: str = f", {concat_ws.separator}, ".join(self.build_expression_sql(e) for e in concat_ws.expressions)
        return f"CONCAT({elements})"

    def _build_string_hash_sql(self, string_hash: STRING_HASH) -> str:
        return f"to_hex({super()._build_string_hash_sql(string_hash)})"

    def _build_random_sql(self, random: RANDOM) -> str:
        return "RAND()"
