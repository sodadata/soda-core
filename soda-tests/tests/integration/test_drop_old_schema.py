from __future__ import annotations

import datetime
import logging
import os

import pytest
from dateutil.parser import parse
from helpers.data_source_test_helper import DataSourceTestHelper
from soda_core.common.logging_constants import soda_logger

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

    # Returns (drop_target, match_name) pairs. drop_target is what we pass to drop_schema_if_exists;
    # match_name is what we match prefix/date rules against. For most data sources they're identical;
    # adapters with nested folder paths (e.g. Dremio) override to keep the two distinct.
    candidates: list[tuple[str, str]] = data_source_test_helper.list_schemas_for_cleanup()

    dry_run_targets: list[str] = []
    num_deleted_schemas: int = 0
    for drop_target, match_name in candidates:
        if any(match_name.lower().startswith(prefix.lower()) for prefix in LIST_OF_PREFIXES_TO_DROP):
            if not any(match_name.lower().startswith(prefix.lower()) for prefix in LIST_OF_EXEMPTIONS):
                if determine_if_schema_needs_to_be_dropped(match_name):
                    if dry_run:
                        dry_run_targets.append(drop_target)
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
        if dry_run_targets:
            target_list: str = "\n  - ".join(dry_run_targets)
            logger.info(f"[DRY RUN] Would drop {len(dry_run_targets)} schemas:\n  - {target_list}")
        else:
            logger.info("[DRY RUN] No schemas matched the cleanup criteria.")
        logger.info(
            "[DRY RUN] Note: actual run may differ — DB state can change between the dry-run and the real run."
        )
    else:
        logger.info(f"Number of deleted schemas: {num_deleted_schemas}")
