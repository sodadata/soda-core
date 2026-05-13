from __future__ import annotations

import datetime
import logging
import os

import pytest
from dateutil.parser import parse
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.data_source_impl import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.sql_dialect import SqlDialect

logger: logging.Logger = soda_logger


DATASOURCES_TO_RUN = [
    "postgres",  # Postgres is not required, but just to verify that the test works (also for local clean up)
    "snowflake",
    "bigquery",
    "fabric",
    "synapse",
    "athena",  # Skipping for now, there is a bug whereby we can't delete the schemas on Athena currently.
    "redshift",
    "oracle",
    "databricks",
    "dremio",
]
# Only drop schemma's starting with these prefixes.
LIST_OF_PREFIXES_TO_DROP = ["soda_diagnostics_", "ALTERNATE_DWH_", "ci_", "my_dwh_"]
# Schema's starting with these prefixes are exempt from being dropped.
LIST_OF_EXEMPTIONS = ["soda_diagnostics_dev_"]


def _dry_run() -> bool:
    return os.getenv("SODA_SCHEMA_CLEANUP_DRY_RUN", "false").lower() in ("1", "true", "yes")


def determine_if_schema_needs_to_be_dropped(schema_name: str) -> bool:
    # We determine if the schema needs to be dropped by checking if the schema name contains a date that we can parse. If it's older than 2 days, we drop it.
    try:
        schema_name = schema_name.lower()
        must_have_date: bool = False
        if schema_name.lower().startswith("soda_diagnostics_"):
            potential_date_string: str = schema_name[
                len("soda_diagnostics_") + 9 : -7
            ]  # soda_diagnostics_0db10c31_20251119_093446
            # Only drop the schema if it has a date
            must_have_date = True
        elif schema_name.upper().startswith("ALTERNATE_DWH_"):
            potential_date_string: str = schema_name[len("ALTERNATE_DWH_") + 9 :]  # ALTERNATE_DWH_0db10c31_20251119
        elif schema_name.lower().startswith("ci_"):
            # There are too many structures for this to be done in a simple way, so we have to try all the possibilities for dates.
            # Start from the beginning and try every substring of 8 characters (after removing underscore)
            no_underscore_schema_name: str = schema_name.replace("_", "")
            found_date: bool = False
            for i in range(len(no_underscore_schema_name) - 8):
                potential_date_string: str = no_underscore_schema_name[i : i + 8]
                try:
                    date_obj: datetime.datetime = parse(potential_date_string, fuzzy=False)
                    if (
                        date_obj.year >= 2025 and date_obj.year <= 2027
                    ):  # Some safeguards to avoid parsing a completely wrong date (because of some hashes etc)
                        found_date = True
                        break
                except ValueError:
                    continue
            if not found_date:
                return False  # Do not drop the schema if we cannot find a date in the schema name. (safeguard, we can manually remove these schemas if needed)
        elif schema_name.lower().startswith("my_dwh_"):
            first_part_index = len("my_dwh_") + 6
            second_part_index = first_part_index + 8
            potential_date_string: str = schema_name[
                first_part_index:second_part_index
            ]  # my_dwh_20251119_postgres_0db10c31
        else:
            return False
        date_obj: datetime.datetime = parse(potential_date_string, fuzzy=False)
        return date_obj < datetime.datetime.now() - datetime.timedelta(days=2)  # Remove everything older than 2 days.
    except ValueError:  # Cannot parse the date, so there is none -> drop it.
        return (
            not must_have_date
        )  # Only drop the schema if it should not have a date. E.g. we don't want to drop the schema if it's a soda_diagnostics_my_dwh_test schema.


def _list_dremio_candidate_schemas(data_source_test_helper: DataSourceTestHelper) -> list[tuple[str, str]]:
    """For Dremio, schemas are nested folder paths exposed via INFORMATION_SCHEMA."SCHEMATA"
    as fully dotted SCHEMA_NAME strings (e.g. 'nas.bucket.host.my_dwh_20260410_dremio_abc').

    We scope the cleanup to schemas under the configured test-root path (everything in
    dataset_prefix except the leaf) to avoid touching unrelated folders, and we match
    LIST_OF_PREFIXES_TO_DROP against the trailing dotted component.

    Returns a list of (full_dotted_schema_name, leaf_component_for_matching) tuples.
    """
    dataset_prefix: list[str] = data_source_test_helper.dataset_prefix
    if not dataset_prefix or len(dataset_prefix) < 2:
        logger.warning(f"Dremio dataset_prefix is too short to derive test root: {dataset_prefix}")
        return []
    test_root_components: list[str] = dataset_prefix[:-1]
    test_root_dotted: str = ".".join(test_root_components)
    test_root_prefix: str = test_root_dotted + "."

    sql: str = 'SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA."SCHEMATA"'
    query_result: QueryResult = data_source_test_helper.data_source_impl.execute_query(sql)

    candidates: list[tuple[str, str]] = []
    for row in query_result.rows:
        full_name: str = row[0]
        if not full_name or not full_name.startswith(test_root_prefix):
            continue
        leaf: str = full_name.rsplit(".", 1)[-1]
        candidates.append((full_name, leaf))
    logger.info(
        f"Dremio: found {len(candidates)} schemas under test root '{test_root_dotted}' "
        f"(out of total schemas in INFORMATION_SCHEMA)"
    )
    return candidates


def test_drop_old_schemas(data_source_test_helper: DataSourceTestHelper):
    if data_source_test_helper.data_source_impl.type_name not in DATASOURCES_TO_RUN:
        pytest.skip(
            f"Skipping test for {data_source_test_helper.data_source_impl.type_name} because it is not in {DATASOURCES_TO_RUN}"
        )
    if data_source_test_helper.data_source_impl.type_name == "databricks":
        # Skip if we're running a hive catalog
        if data_source_test_helper.data_source_impl._is_hive_catalog():
            pytest.skip(
                f"Skipping test for {data_source_test_helper.data_source_impl.type_name} because it is a hive catalog"
            )

    dry_run: bool = _dry_run()
    if dry_run:
        logger.info("SODA_SCHEMA_CLEANUP_DRY_RUN is set — will log targets but not execute DROP statements.")

    type_name: str = data_source_test_helper.data_source_impl.type_name

    # Build the list of (drop_target, match_name) pairs. For Dremio, drop_target is the full
    # dotted folder path and match_name is the trailing component. For other DSes they're the same.
    candidates: list[tuple[str, str]]
    if type_name == "dremio":
        candidates = _list_dremio_candidate_schemas(data_source_test_helper)
    else:
        dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect
        table_namespace, _ = data_source_test_helper.data_source_impl._build_table_namespace_for_schema_query(
            data_source_test_helper.dataset_prefix
        )
        schemas_query_sql = dialect.build_schemas_metadata_query_str(table_namespace=table_namespace)
        query_result: QueryResult = data_source_test_helper.data_source_impl.execute_query(schemas_query_sql)
        candidates = [(row[0], row[0]) for row in query_result.rows if row[0]]

    num_matched_schemas: int = 0
    num_deleted_schemas: int = 0
    for drop_target, match_name in candidates:
        if any(match_name.lower().startswith(prefix.lower()) for prefix in LIST_OF_PREFIXES_TO_DROP):
            if not any(match_name.lower().startswith(prefix.lower()) for prefix in LIST_OF_EXEMPTIONS):
                if determine_if_schema_needs_to_be_dropped(match_name):
                    num_matched_schemas += 1
                    if dry_run:
                        logger.info(f"[DRY RUN] Would drop schema: {drop_target}")
                        continue
                    logger.info(f"Dropping schema name: {drop_target}")
                    try:
                        data_source_test_helper.drop_schema_if_exists(drop_target)
                        num_deleted_schemas += 1
                    except (
                        Exception
                    ) as e:  # We catch any exception, because we don't want the CI itself to fail because of this.
                        logger.error(f"Error dropping schema: {e}")
                else:
                    logger.info(f"Schema name: {match_name} is not old enough to be dropped or is an exemption")
    if dry_run:
        logger.info(f"[DRY RUN] Number of schemas that would be deleted: {num_matched_schemas}")
    else:
        logger.info(f"Number of deleted schemas: {num_deleted_schemas}")
