from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.data_source_results import QueryResult
from soda_core.common.logging_constants import soda_logger
from soda_core.common.metadata_types import ColumnMetadata, SqlDataType
from soda_core.common.utils import format_items
from soda_core.contracts.contract_verification import (
    Check,
    CheckOutcome,
    CheckResult,
    Measurement,
    Threshold,
)
from soda_core.contracts.impl.check_types.schema_check_yaml import SchemaCheckYaml
from soda_core.contracts.impl.contract_verification_impl import (
    CheckImpl,
    CheckParser,
    ColumnImpl,
    ContractImpl,
    MeasurementValues,
    MetricImpl,
    Query,
    ThresholdLevel,
)

logger: logging.Logger = soda_logger


class SchemaCheckParser(CheckParser):
    def get_check_type_names(self) -> list[str]:
        return ["schema"]

    def parse_check(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: SchemaCheckYaml,
    ) -> Optional[CheckImpl]:
        return SchemaCheckImpl(
            contract_impl=contract_impl,
            check_yaml=check_yaml,
        )


@dataclass
class ColumnDataTypeMismatch:
    column: str
    expected_data_type: str
    expected_character_maximum_length: Optional[int]
    actual_data_type: str
    actual_character_maximum_length: Optional[int]

    def get_expected(self) -> str:
        return f"{self.expected_data_type}{self.get_optional_length_str(self.expected_character_maximum_length)}"

    def get_actual(self) -> str:
        return f"{self.actual_data_type}{self.get_optional_length_str(self.actual_character_maximum_length)}"

    @classmethod
    def get_optional_length_str(cls, character_maximum_length: Optional[int]) -> str:
        return f"({character_maximum_length})" if isinstance(character_maximum_length, int) else ""


class SchemaCheckImpl(CheckImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
        check_yaml: SchemaCheckYaml,
    ):
        super().__init__(
            contract_impl=contract_impl,
            column_impl=None,
            check_yaml=check_yaml,
        )

        logger.info(f"Found {len(contract_impl.column_impls)} columns in contract.")

        self.expected_columns: list[ColumnMetadata] = [
            ColumnMetadata(
                column_name=column_impl.column_yaml.name,
                sql_data_type=(
                    SqlDataType(
                        name=column_impl.column_yaml.data_type,
                        character_maximum_length=column_impl.column_yaml.character_maximum_length,
                    )
                    if column_impl.column_yaml.data_type
                    else None
                ),
            )
            for column_impl in contract_impl.column_impls
        ]
        logger.info(f"Built {len(self.expected_columns)} expected columns from contract.")
        self.allow_extra_columns: bool = bool(check_yaml.allow_extra_columns)
        self.allow_other_column_order: bool = bool(check_yaml.allow_other_column_order)

    def setup_metrics(
        self,
        contract_impl: ContractImpl,
        column_impl: Optional[ColumnImpl],
        check_yaml: SchemaCheckYaml,
    ):
        self.schema_metric = self._resolve_metric(
            SchemaMetricImpl(
                contract_impl=contract_impl,
            )
        )

        if contract_impl.data_source_impl:
            schema_query: Query = SchemaQuery(
                dataset_prefixes=contract_impl.dataset_prefix,
                dataset_name=contract_impl.dataset_name,
                schema_metric_impl=self.schema_metric,
                data_source_impl=contract_impl.data_source_impl,
            )
            self.queries.append(schema_query)

    def evaluate(self, measurement_values: MeasurementValues) -> CheckResult:
        logger.info("Evaluating schema check")
        outcome: CheckOutcome = CheckOutcome.NOT_EVALUATED

        expected_column_names_not_actual: list[str] = []
        actual_column_names_not_expected: list[str] = []
        column_data_type_mismatches: list[ColumnDataTypeMismatch] = []
        are_columns_out_of_order: bool = False

        logger.info(f"Getting actual columns from measurement values.")
        # The query to fetch the ColumnMetadata could have failed, in that case no measurement is returned -> actual_columns will be None.
        actual_columns: Optional[list[ColumnMetadata]] = measurement_values.get_value(self.schema_metric)
        if actual_columns is None:
            logger.error(f"Actual columns are None.")
            logger.error(f"Setting actual columns to empty list.")
            actual_columns = []

        for column in actual_columns:
            if not isinstance(column, ColumnMetadata):
                logger.error(f"Actual column is not a ColumnMetadata: {column}")
                logger.error(f"Setting actual columns to empty list.")
                actual_columns = []

        logger.info(f"Found {len(actual_columns)} actual columns.")

        if actual_columns:
            actual_column_names: list[str] = [actual_column.column_name for actual_column in actual_columns]
            actual_column_metadata_by_name: dict[str, ColumnMetadata] = {
                actual_column.column_name: actual_column for actual_column in actual_columns
            }
            expected_column_names: list[str] = [
                expected_column.column_name for expected_column in self.expected_columns
            ]

            logger.info("Starting to check for missing columns.")
            for expected_column in expected_column_names:
                if expected_column not in actual_column_names:
                    expected_column_names_not_actual.append(expected_column)
            logger.info(f"Found {len(expected_column_names_not_actual)} missing columns.")

            if not self.allow_extra_columns:
                logger.info("Starting to check for extra columns.")
                for actual_column_name in actual_column_names:
                    if actual_column_name not in expected_column_names:
                        actual_column_names_not_expected.append(actual_column_name)
                logger.info(f"Found {len(actual_column_names_not_expected)} extra columns.")

            logger.info("Starting to check for data type mismatches.")
            for expected_column in self.expected_columns:
                actual_column_metadata: ColumnMetadata = actual_column_metadata_by_name.get(expected_column.column_name)
                # Assume by default that the data types are the same
                is_same_data_type: bool = True
                try:
                    if actual_column_metadata and expected_column.sql_data_type:
                        is_same_data_type: bool = (
                            self.contract_impl.data_source_impl.sql_dialect.is_same_data_type_for_schema_check(
                                expected=expected_column.sql_data_type,
                                actual=actual_column_metadata.sql_data_type,
                            )
                        )
                except Exception as e:
                    logger.error(
                        f"Error checking for data type mismatch between column {expected_column.column_name} and actual column {actual_column_metadata.column_name}: {e}"
                    )
                    logger.error(f"Skipping column {expected_column.column_name}.")
                    continue
                if not is_same_data_type:
                    column_data_type_mismatches.append(
                        # TODO add numeric_scale, numeric_precision & datetime_precision to the ColumnDataTypeMismatch
                        ColumnDataTypeMismatch(
                            column=expected_column.column_name,
                            expected_data_type=expected_column.sql_data_type.name,
                            expected_character_maximum_length=expected_column.sql_data_type.character_maximum_length,
                            actual_data_type=actual_column_metadata.sql_data_type.name,
                            actual_character_maximum_length=actual_column_metadata.sql_data_type.character_maximum_length,
                        )
                    )
            logger.info(f"Found {len(column_data_type_mismatches)} data type mismatches.")

            if not self.allow_other_column_order:
                logger.info("Starting to check for columns out of order.")
                try:
                    previous_index: int = 0
                    for actual_column_name in actual_column_names:
                        if actual_column_name in expected_column_names:
                            index: int = expected_column_names.index(actual_column_name)
                            if index < previous_index:
                                are_columns_out_of_order = True
                            previous_index = index
                except Exception as e:
                    logger.error(f"Error checking for columns out of order: {e}")
                logger.info(f"Found {are_columns_out_of_order} columns out of order.")

            outcome = (
                CheckOutcome.PASSED
                if (
                    len(expected_column_names_not_actual) == 0
                    and len(actual_column_names_not_expected) == 0
                    and len(column_data_type_mismatches) == 0
                    and not are_columns_out_of_order
                )
                else CheckOutcome.FAILED
            )

        logger.info(f"Schema check outcome: {outcome}")

        return SchemaCheckResult(
            check=self._build_check_info(),
            outcome=outcome,
            expected_columns=self.expected_columns,
            actual_columns=actual_columns,
            expected_column_names_not_actual=expected_column_names_not_actual,
            actual_column_names_not_expected=actual_column_names_not_expected,
            column_data_type_mismatches=column_data_type_mismatches,
            are_columns_out_of_order=are_columns_out_of_order,
        )

    def _build_threshold(self) -> Threshold:
        return Threshold(must_be_less_than_or_equal=0, level=ThresholdLevel.FAIL.value)


class SchemaMetricImpl(MetricImpl):
    def __init__(
        self,
        contract_impl: ContractImpl,
    ):
        super().__init__(contract_impl=contract_impl, metric_type="schema")


class SchemaQuery(Query):
    def __init__(
        self,
        dataset_prefixes: Optional[list[str]],
        dataset_name: str,
        schema_metric_impl: SchemaMetricImpl,
        data_source_impl: Optional[DataSourceImpl],
    ):
        super().__init__(data_source_impl=data_source_impl, metrics=[schema_metric_impl])

        self.sql = data_source_impl.build_columns_metadata_query_str(
            dataset_prefixes=dataset_prefixes, dataset_name=dataset_name
        )

    def execute(self) -> list[Measurement]:
        try:
            query_result: QueryResult = self.data_source_impl.execute_query(self.sql)
        except Exception as e:
            logger.error(msg=f"Could not execute schema query {self.sql}: {e}", exc_info=True)
            return []
        try:
            metadata_columns: list[
                ColumnMetadata
            ] = self.data_source_impl.sql_dialect.build_column_metadatas_from_query_result(query_result)
        except Exception as e:
            logger.error(f"Error building column metadata from query result: {e}")
            return []
        schema_metric_impl: MetricImpl = self.metrics[0]
        return [
            Measurement(metric_id=schema_metric_impl.id, value=metadata_columns, metric_name=schema_metric_impl.type)
        ]


class SchemaCheckResult(CheckResult):
    def __init__(
        self,
        check: Check,
        outcome: CheckOutcome,
        expected_columns: list[ColumnMetadata],
        actual_columns: list[ColumnMetadata],
        expected_column_names_not_actual: list[str],
        actual_column_names_not_expected: list[str],
        column_data_type_mismatches: list[ColumnDataTypeMismatch],
        are_columns_out_of_order: bool,
    ):
        schema_events = sum(
            (
                len(expected_column_names_not_actual),
                len(actual_column_names_not_expected),
                len(column_data_type_mismatches),
            )
        )
        diagnostic_metric_values = {"schema_events_count": schema_events}

        super().__init__(
            check=check,
            outcome=outcome,
            threshold_value=schema_events,
            diagnostic_metric_values=diagnostic_metric_values,
        )

        self.expected_columns: list[ColumnMetadata] = expected_columns
        self.actual_columns: list[ColumnMetadata] = actual_columns
        self.expected_column_names_not_actual: list[str] = expected_column_names_not_actual
        self.actual_column_names_not_expected: list[str] = actual_column_names_not_expected
        self.column_data_type_mismatches: list[ColumnDataTypeMismatch] = column_data_type_mismatches
        self.are_columns_out_of_order: bool = are_columns_out_of_order

    def log_table_row_diagnostics(self, verbose: bool = True) -> str:
        diagnostics = []

        data_delimiter = "\n" if verbose else " "
        category_delimiter = "\n\n" if verbose else "\n"

        if self.actual_column_names_not_expected:
            diagnostics.append(
                f"Present, not expected columns:{data_delimiter}{format_items(self.actual_column_names_not_expected, verbose=verbose)}"
            )

        if self.expected_column_names_not_actual:
            diagnostics.append(
                f"Missing columns:{data_delimiter}{format_items(self.expected_column_names_not_actual, verbose=verbose)}"
            )

        if self.column_data_type_mismatches:
            mismatches = [
                f"{data_type_mismatch.column}: {data_type_mismatch.get_expected()} -> {data_type_mismatch.get_actual()}"
                for data_type_mismatch in self.column_data_type_mismatches
            ]
            diagnostics.append(f"Data type mismatches:{data_delimiter}{format_items(mismatches,verbose=verbose)}")

        if self.are_columns_out_of_order:
            diagnostics.append("There are columns out of order")

        if verbose:
            actual_columns = [
                f"{actual_column.column_name}({actual_column.sql_data_type})" for actual_column in self.actual_columns
            ]
            diagnostics.append(f"Actual schema:{data_delimiter}{format_items(actual_columns, verbose=verbose)}")

        return category_delimiter.join(diagnostics)

    def dataset_does_not_exists(self) -> bool:
        """
        Helper method for testing
        """
        return len(self.actual_columns) == 0

    def column_does_not_exist(self, column_name: str) -> bool:
        """
        Helper method for testing
        """
        return any(actual_column.column_name == column_name for actual_column in self.actual_columns)
