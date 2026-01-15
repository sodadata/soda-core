from __future__ import annotations

import datetime
import logging

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
    "athena",
    "bigquery",
]
# Only drop schemma's starting with these prefixes.
LIST_OF_PREFIXES_TO_DROP = ["soda_diagnostics_", "ALTERNATE_DWH_", "ci_", "my_dwh_"]
# Schema's starting with these prefixes are exempt from being dropped.
LIST_OF_EXEMPTIONS = ["soda_diagnostics_dev_"]


def determine_if_schema_needs_to_be_dropped(schema_name: str) -> bool:
    # We determine if the schema needs to be dropped by checking if the schema name contains a date that we can parse. If it's older than 2 days, we drop it.
    try:
        schema_name = schema_name.lower()
        if schema_name.lower().startswith("soda_diagnostics_"):
            potential_date_string: str = schema_name[
                len("soda_diagnostics_") + 9 : -7
            ]  # soda_diagnostics_0db10c31_20251119_093446
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
        return True


@pytest.mark.diagnostic_warehouse
def test_drop_old_schemas(data_source_test_helper: DataSourceTestHelper):
    if data_source_test_helper.data_source_impl.type_name not in DATASOURCES_TO_RUN:
        pytest.skip(
            f"Skipping test for {data_source_test_helper.data_source_impl.type_name} because it is not in {DATASOURCES_TO_RUN}"
        )
    dialect: SqlDialect = data_source_test_helper.data_source_impl.sql_dialect
    # First get the list of all the schema's in the database.
    table_namespace, schema_name = data_source_test_helper.data_source_impl._build_table_namespace_for_schema_query(
        data_source_test_helper.dataset_prefix
    )
    schemas_query_sql = dialect.build_schemas_metadata_query_str(table_namespace=table_namespace)
    query_result: QueryResult = data_source_test_helper.data_source_impl.execute_query(schemas_query_sql)
    schema_names: list[str] = [row[0] for row in query_result.rows]
    for schema_name in schema_names:
        if any(schema_name.lower().startswith(prefix.lower()) for prefix in LIST_OF_PREFIXES_TO_DROP):
            if not any(schema_name.lower().startswith(prefix.lower()) for prefix in LIST_OF_EXEMPTIONS):
                if determine_if_schema_needs_to_be_dropped(schema_name):
                    logger.info(f"Dropping schema name: {schema_name}")
                    try:
                        data_source_test_helper.data_source_impl.execute_update(
                            data_source_test_helper.drop_schema_if_exists_sql(
                                schema_name
                            )  # Use if exists because we may have multiple CI runs at the same time, removing the same schema.
                        )
                    except (
                        Exception
                    ) as e:  # We catch any exception, because we don't want the CI itself to fail because of this.
                        logger.error(f"Error dropping schema: {e}")
                else:
                    logger.info(f"Schema name: {schema_name} is not old enough to be dropped")
