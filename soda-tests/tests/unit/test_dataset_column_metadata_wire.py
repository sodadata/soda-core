"""The ``metadata`` bucket of ``sodaCoreInsertScanResults``: the dataset's column list.

Soda Cloud fills a dataset's column list from ``metadata[].schema``. Contract
verification only has that list when a schema check already measured it, so the
payload carries ``metadata`` exactly when ``CheckCollectionResult.dataset_columns``
is populated and stays silent otherwise. No extra warehouse query is ever run for it.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from soda_core.check_collections.base import _find_measured_dataset_columns
from soda_core.common.logs import Location
from soda_core.common.metadata_types import ColumnMetadata, SqlDataType
from soda_core.common.soda_cloud import _build_check_collection_results_json_dict
from soda_core.contracts.contract_verification import (
    Check,
    CheckCollectionStatus,
    CheckOutcome,
    CheckResult,
    Contract,
    ContractVerificationResult,
    DataSource,
    YamlFileContentInfo,
)
from soda_core.contracts.impl.check_types.schema_check import SchemaCheckResult


def _make_check_result(path: str = "checks.row_count") -> CheckResult:
    return CheckResult(
        check=Check(
            column_name="id",
            type="row_count",
            qualifier=None,
            name="row count",
            relative_path=path,
            check_path=path,
            identity="abc",
            definition="row_count: ...",
            contract_file_line=1,
            contract_file_column=1,
            threshold=None,
            attributes={},
            location=Location(file_path="fake.yml", line=1, column=1),
        ),
        outcome=CheckOutcome.PASSED,
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )


def _make_result(
    *,
    dataset_qualified_name: str = "test_ds/s/t",
    dataset_columns: Optional[list[ColumnMetadata]] = None,
) -> ContractVerificationResult:
    started = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    ended = datetime(2026, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
    parts = dataset_qualified_name.split("/")
    return ContractVerificationResult(
        check_collection=Contract(
            data_source_name=parts[0],
            dataset_prefix=parts[1:-1],
            dataset_name=parts[-1],
            soda_qualified_dataset_name=dataset_qualified_name,
            source=YamlFileContentInfo(
                source_content_str="# fake",
                local_file_path="/fake/x.yml",
                soda_cloud_file_id="file-x",
            ),
        ),
        data_source=DataSource(name=parts[0], type="postgres"),
        data_timestamp=started,
        started_timestamp=started,
        ended_timestamp=ended,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=[_make_check_result()],
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
        dataset_columns=dataset_columns,
    )


def test_metadata_carries_the_dataset_columns():
    """A result that measured its columns emits one metadata entry with the
    column names and their source data types."""
    result = _make_result(
        dataset_columns=[
            ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="VARCHAR", character_maximum_length=255)),
            ColumnMetadata(column_name="size", sql_data_type=SqlDataType(name="integer")),
        ]
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert payload["metadata"] == [
        {
            "datasetQualifiedName": "test_ds/s/t",
            "schema": [
                {"columnName": "id", "sourceDataType": "varchar"},
                {"columnName": "size", "sourceDataType": "integer"},
            ],
        }
    ]


def test_metadata_absent_when_result_has_no_columns():
    """No schema check, or a schema check that never got its columns: the key
    is omitted entirely rather than sent as an empty list."""
    payload = _build_check_collection_results_json_dict([_make_result()], wire_source="soda-contract")
    assert "metadata" not in payload

    payload = _build_check_collection_results_json_dict([_make_result(dataset_columns=[])], wire_source="soda-contract")
    assert "metadata" not in payload


def test_dataset_qualified_name_matches_the_dataset_of_the_checks():
    """Cloud resolves the metadata entry and the checks through the same dataset
    identifier, so the qualified name must be the checks' dataset spelled out."""
    result = _make_result(
        dataset_qualified_name="test_ds/db/schema/CUSTOMERS",
        dataset_columns=[ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="varchar"))],
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")

    check_json = payload["checks"][0]
    checks_dataset = "/".join([check_json["dataSource"], *check_json["datasetPrefix"], check_json["table"]])
    assert payload["metadata"][0]["datasetQualifiedName"] == checks_dataset == "test_ds/db/schema/CUSTOMERS"


def test_source_data_type_drops_the_type_parameters():
    """Cloud stores the bare type name: ``numeric(10,2)`` goes out as ``numeric``."""
    result = _make_result(
        dataset_columns=[
            ColumnMetadata(
                column_name="score", sql_data_type=SqlDataType(name="NUMERIC", numeric_precision=10, numeric_scale=2)
            ),
        ]
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert payload["metadata"][0]["schema"] == [{"columnName": "score", "sourceDataType": "numeric"}]


def test_column_without_a_data_type_is_sent_by_name_only_and_logged_as_an_error(caplog):
    """Cloud marks any column missing from ``schema`` as deleted, whereas a missing
    ``sourceDataType`` leaves the stored type alone, so the column goes out by name. No data
    source produces a typeless column, so the error line makes the broken invariant visible."""
    result = _make_result(
        dataset_columns=[
            ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="varchar")),
            ColumnMetadata(column_name="mystery", sql_data_type=None),
        ]
    )
    with caplog.at_level(logging.ERROR, logger="soda"):
        payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")

    assert payload["metadata"][0]["schema"] == [
        {"columnName": "id", "sourceDataType": "varchar"},
        {"columnName": "mystery"},
    ]
    error_messages = [r.getMessage() for r in caplog.records if r.levelno == logging.ERROR]
    assert len(error_messages) == 1
    assert "'mystery'" in error_messages[0]
    assert "test_ds/s/t" in error_messages[0]


def test_one_metadata_entry_per_dataset_in_a_batch():
    """Combine-upload batches contribute one entry per result that has columns;
    results without columns contribute nothing."""
    results = [
        _make_result(
            dataset_qualified_name="test_ds/s/a",
            dataset_columns=[ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="varchar"))],
        ),
        _make_result(dataset_qualified_name="test_ds/s/b"),
        _make_result(
            dataset_qualified_name="test_ds/s/c",
            dataset_columns=[ColumnMetadata(column_name="code", sql_data_type=SqlDataType(name="integer"))],
        ),
    ]
    payload = _build_check_collection_results_json_dict(results, wire_source="data-standard")
    assert [entry["datasetQualifiedName"] for entry in payload["metadata"]] == ["test_ds/s/a", "test_ds/s/c"]


def test_repeated_dataset_in_a_batch_is_deduplicated():
    """Two results over the same dataset produce one entry: the first one wins."""
    results = [
        _make_result(
            dataset_qualified_name="test_ds/s/a",
            dataset_columns=[ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="varchar"))],
        ),
        _make_result(
            dataset_qualified_name="test_ds/s/a",
            dataset_columns=[ColumnMetadata(column_name="other", sql_data_type=SqlDataType(name="integer"))],
        ),
    ]
    payload = _build_check_collection_results_json_dict(results, wire_source="data-standard")
    assert payload["metadata"] == [
        {
            "datasetQualifiedName": "test_ds/s/a",
            "schema": [{"columnName": "id", "sourceDataType": "varchar"}],
        }
    ]


def test_column_with_an_empty_data_type_name_is_sent_by_name_only_and_logged_as_an_error(caplog):
    """A type name that is blank rather than absent is treated the same way."""
    result = _make_result(
        dataset_columns=[
            ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="varchar")),
            ColumnMetadata(column_name="blank", sql_data_type=SqlDataType(name="")),
        ]
    )
    with caplog.at_level(logging.ERROR, logger="soda"):
        payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")

    assert payload["metadata"][0]["schema"] == [
        {"columnName": "id", "sourceDataType": "varchar"},
        {"columnName": "blank"},
    ]
    assert any("'blank'" in r.getMessage() for r in caplog.records if r.levelno == logging.ERROR)


def _make_schema_check_result(*, outcome: CheckOutcome, actual_columns: list[ColumnMetadata]) -> SchemaCheckResult:
    return SchemaCheckResult(
        check=_make_check_result("checks.schema").check,
        outcome=outcome,
        expected_columns=[],
        actual_columns=actual_columns,
        expected_column_names_not_actual=[],
        actual_column_names_not_expected=[],
        column_data_type_mismatches=[],
        are_columns_out_of_order=False,
    )


def test_no_schema_check_leaves_the_dataset_columns_unknown():
    """A contract without a schema check never measured the columns, and the
    engine does not go and query for them."""
    assert _find_measured_dataset_columns([_make_check_result()]) is None


def test_schema_check_without_actual_columns_leaves_them_unknown():
    """The schema query can fail, which leaves the schema check result with no
    actual columns. Nothing to report then."""
    schema_check_result = _make_schema_check_result(outcome=CheckOutcome.NOT_EVALUATED, actual_columns=[])
    assert _find_measured_dataset_columns([_make_check_result(), schema_check_result]) is None


def test_failed_schema_check_still_reports_its_columns():
    """A schema check that failed still measured what the dataset actually has —
    that is the very list Cloud needs."""
    actual_columns = [ColumnMetadata(column_name="id", sql_data_type=SqlDataType(name="varchar"))]
    schema_check_result = _make_schema_check_result(outcome=CheckOutcome.FAILED, actual_columns=actual_columns)
    assert _find_measured_dataset_columns([_make_check_result(), schema_check_result]) == actual_columns
