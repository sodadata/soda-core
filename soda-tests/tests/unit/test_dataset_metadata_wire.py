"""Unit tests for the top-level ``metadata`` block in the wire payload.

Locks the shape of the dataset ``metadata`` array that
``_build_check_collection_results_json_dict`` emits when a check collection
runs a schema check. The block lets the backend refresh the dataset's column
schema in Cloud, reusing the ``{columnName, sourceDataType}`` shape v3 discover
emits (DTL-1807).
"""

from __future__ import annotations

from datetime import datetime, timezone

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


def _make_check(*, type: str, relative_path: str) -> Check:
    return Check(
        column_name=None,
        type=type,
        qualifier=None,
        name=type,
        relative_path=relative_path,
        identity="abc",
        definition=f"{type}: ...",
        contract_file_line=1,
        contract_file_column=1,
        threshold=None,
        attributes={},
        location=Location(file_path="fake.yml", line=1, column=1),
    )


def _column(name: str, sql_data_type: SqlDataType) -> ColumnMetadata:
    return ColumnMetadata(column_name=name, sql_data_type=sql_data_type)


def _make_schema_check_result(actual_columns: list[ColumnMetadata]) -> SchemaCheckResult:
    return SchemaCheckResult(
        check=_make_check(type="schema", relative_path="checks.schema"),
        outcome=CheckOutcome.PASSED,
        expected_columns=[],
        actual_columns=actual_columns,
        expected_column_names_not_actual=[],
        actual_column_names_not_expected=[],
        column_data_type_mismatches=[],
        are_columns_out_of_order=False,
    )


def _make_row_count_check_result() -> CheckResult:
    return CheckResult(
        check=_make_check(type="row_count", relative_path="checks.row_count"),
        outcome=CheckOutcome.PASSED,
        diagnostic_metric_values={"check_rows_tested": 0, "dataset_rows_tested": 0},
    )


def _make_result(
    *,
    soda_qualified_dataset_name: str,
    dataset_prefix: list[str],
    dataset_name: str,
    check_results: list,
) -> ContractVerificationResult:
    ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    return ContractVerificationResult(
        check_collection=Contract(
            data_source_name="test_ds",
            dataset_prefix=dataset_prefix,
            dataset_name=dataset_name,
            soda_qualified_dataset_name=soda_qualified_dataset_name,
            source=YamlFileContentInfo(
                source_content_str="# c",
                local_file_path="/fake/c.yml",
                soda_cloud_file_id="file-c",
            ),
        ),
        data_source=DataSource(name="test_ds", type="postgres"),
        data_timestamp=ts,
        started_timestamp=ts,
        ended_timestamp=ts,
        status=CheckCollectionStatus.PASSED,
        measurements=[],
        check_results=check_results,
        sending_results_to_soda_cloud_failed=False,
        log_records=[],
        post_processing_stages=[],
    )


def test_metadata_present_with_schema_of_column_name_and_source_data_type():
    """A collection that ran a schema check → one ``metadata`` entry whose
    ``schema`` mirrors the discovered columns as ``{columnName, sourceDataType}``."""
    result = _make_result(
        soda_qualified_dataset_name="postgres/public/customers",
        dataset_prefix=["public"],
        dataset_name="customers",
        check_results=[
            _make_schema_check_result(
                actual_columns=[
                    _column("id", SqlDataType(name="integer")),
                    _column("name", SqlDataType(name="character varying", character_maximum_length=255)),
                ]
            )
        ],
    )

    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")

    assert payload["metadata"] == [
        {
            "datasetQualifiedName": "postgres/public/customers",
            "schema": [
                {"columnName": "id", "sourceDataType": "integer"},
                {"columnName": "name", "sourceDataType": "character varying"},
            ],
        }
    ]


def test_metadata_source_data_type_is_unparameterized():
    """``sourceDataType`` is the raw un-parameterized type (``character
    varying``), NOT the parameterized ``character varying(255)`` form."""
    result = _make_result(
        soda_qualified_dataset_name="postgres/public/customers",
        dataset_prefix=["public"],
        dataset_name="customers",
        check_results=[
            _make_schema_check_result(
                actual_columns=[_column("name", SqlDataType(name="character varying", character_maximum_length=255))]
            )
        ],
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert payload["metadata"][0]["schema"][0]["sourceDataType"] == "character varying"


def test_metadata_absent_when_no_schema_check():
    """No schema check → no ``metadata`` key at all (``to_jsonnable`` strips it)."""
    result = _make_result(
        soda_qualified_dataset_name="postgres/public/customers",
        dataset_prefix=["public"],
        dataset_name="customers",
        check_results=[_make_row_count_check_result()],
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert "metadata" not in payload


def test_metadata_absent_when_schema_check_has_no_columns():
    """A schema check that discovered no columns (missing dataset) → no
    ``metadata`` entry. We never send an empty schema — that would soft-delete
    every column on the backend."""
    result = _make_result(
        soda_qualified_dataset_name="postgres/public/customers",
        dataset_prefix=["public"],
        dataset_name="customers",
        check_results=[_make_schema_check_result(actual_columns=[])],
    )
    payload = _build_check_collection_results_json_dict([result], wire_source="soda-contract")
    assert "metadata" not in payload


def test_metadata_one_entry_per_dataset_in_session():
    """A combined multi-file session emits one ``metadata`` entry per distinct
    dataset that ran a schema check — prefixed and non-prefixed dqns alike."""
    prefixed = _make_result(
        soda_qualified_dataset_name="postgres/public/customers",
        dataset_prefix=["public"],
        dataset_name="customers",
        check_results=[_make_schema_check_result(actual_columns=[_column("id", SqlDataType(name="integer"))])],
    )
    non_prefixed = _make_result(
        soda_qualified_dataset_name="postgres/orders",
        dataset_prefix=[],
        dataset_name="orders",
        check_results=[_make_schema_check_result(actual_columns=[_column("total", SqlDataType(name="numeric"))])],
    )
    no_schema = _make_result(
        soda_qualified_dataset_name="postgres/public/events",
        dataset_prefix=["public"],
        dataset_name="events",
        check_results=[_make_row_count_check_result()],
    )

    payload = _build_check_collection_results_json_dict(
        [prefixed, non_prefixed, no_schema], wire_source="data-standard"
    )

    assert payload["metadata"] == [
        {
            "datasetQualifiedName": "postgres/public/customers",
            "schema": [{"columnName": "id", "sourceDataType": "integer"}],
        },
        {
            "datasetQualifiedName": "postgres/orders",
            "schema": [{"columnName": "total", "sourceDataType": "numeric"}],
        },
    ]
