import pytest

from dbt.contracts.results import TestStatus

from sodasql import dbt as soda_dbt


MANIFEST = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v3.json"
    },
    "nodes": {
        "model.soda.stg_soda__scan": {
            "raw_sql": "select result, warehouse from {{ source('soda', 'scan') }}",
            "resource_type": "model",
            "depends_on": {"macros": [], "nodes": ["source.soda.scan"]},
            "config": {
                "enabled": True,
                "alias": None,
                "schema": "stg_soda",
                "database": None,
                "tags": [],
                "meta": {},
                "materialized": "view",
                "persist_docs": {"relation": True, "columns": True},
                "quoting": {},
                "column_types": {},
                "full_refresh": None,
                "on_schema_change": "ignore",
                "file_format": "delta",
                "location_root": "/mnt/lake/staging",
                "post-hook": [],
                "pre-hook": [],
            },
            "database": None,
            "schema": "main_stg_soda",
            "fqn": ["soda", "staging", "soda", "stg_soda__scan"],
            "unique_id": "model.soda.stg_soda__scan",
            "package_name": "soda",
            "root_path": "/users/soda/github/soda/dbt",
            "path": "staging/soda/stg_soda__scan.sql",
            "original_file_path": "models/staging/soda/stg_soda__scan.sql",
            "name": "stg_soda__scan",
            "alias": "stg_soda__scan",
            "checksum": {
                "name": "sha256",
                "checksum": "11426f8b2005521716d6924831b399c92ff8eb9045573f0e",
            },
            "tags": [],
            "refs": [],
            "sources": [["soda", "scan"]],
            "description": "the soda scans",
            "columns": {
                "result": {
                    "name": "result",
                    "description": "the result",
                    "meta": {},
                    "data_type": None,
                    "quote": None,
                    "tags": [],
                },
                "warehouse": {
                    "name": "warehouse",
                    "description": "the warehouse",
                    "meta": {},
                    "data_type": None,
                    "quote": None,
                    "tags": [],
                },
            },
            "meta": {},
            "docs": {"show": True},
            "patch_path": "soda://models/staging/soda/stg_soda.yml",
            "compiled_path": None,
            "build_path": None,
            "deferred": False,
            "unrendered_config": {
                "materialized": "view",
                "file_format": "delta",
                "persist_docs": {"relation": True, "columns": True},
                "location_root": "/mnt/lake/staging",
                "schema": "stg_soda",
            },
            "created_at": 1637317368,
        },
        "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f": {
            "raw_sql": '{{ test_accepted_values(**_dbt_schema_test_kwargs) }}{{ config(alias="accepted_values_stg_soda__scan") }}',
            "test_metadata": {
                "name": "accepted_values",
                "kwargs": {
                    "values": ["pass", "fail"],
                    "column_name": "result",
                    "model": "{{ get_where_subquery(ref('stg_soda__scan')) }}",
                },
                "namespace": None,
            },
            "compiled": True,
            "resource_type": "test",
            "depends_on": {
                "macros": [
                    "macro.dbt.test_accepted_values",
                    "macro.dbt.get_where_subquery",
                    "macro.dbt.should_store_failures",
                    "macro.dbt.statement",
                ],
                "nodes": ["model.soda.stg_soda__scan"],
            },
            "config": {
                "enabled": True,
                "alias": None,
                "schema": "dbt_test__audit",
                "database": None,
                "tags": [],
                "meta": {},
                "materialized": "test",
                "severity": "ERROR",
                "store_failures": None,
                "where": None,
                "limit": None,
                "fail_calc": "count(*)",
                "warn_if": "!= 0",
                "error_if": "!= 0",
            },
            "database": None,
            "schema": "dbt_test__audit",
            "fqn": ["soda", "schema_test", "not_None_stg_soda_scan_result"],
            "unique_id": "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f",
            "package_name": "soda",
            "root_path": "/Users/soda/github/soda/dbt",
            "path": "schema_test/not_None_stg_soda_scan_result.sql",
            "original_file_path": "models/staging/soda/stg_soda__scan.yml",
            "name": "not_None_stg_soda__scan_result",
            "alias": "not_None_stg_soda_scan_result",
            "checksum": {"name": "none", "checksum": ""},
            "tags": ["schema"],
            "refs": [["stg_soda__scan"]],
            "sources": [],
            "description": "",
            "columns": {},
            "meta": {},
            "docs": {"show": True},
            "patch_path": None,
            "compiled_path": "target/compiled/soda/models/staging/soda/stg_soda__scan.yml/schema_test/not_None_stg_soda_scan_result.sql",
            "build_path": "target/run/soda/models/staging/soda/stg_soda.yml/schema_test/not_None_stg_soda_scan_result.sql",
            "deferred": False,
            "unrendered_config": {},
            "created_at": 1637317368,
            "compiled_sql": "\n    \n    \n\nwith all_values as (\n\n    select\n        result as value_field,\n        count(*) as n_records\n\n    from main_stg_soda.stg_soda__scan\n    group by result\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    'fail','pass')\n\n\n",
            "extra_ctes_injected": True,
            "extra_ctes": [],
            "relation_name": None,
            "column_name": "result",
        },
        "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e": {
            "raw_sql": '{{ test_accepted_values(**_dbt_schema_test_kwargs) }}{{ config(alias="accepted_values_stg_soda__scan") }}',
            "test_metadata": {
                "name": "accepted_values",
                "kwargs": {
                    "values": ["spark", "postgres"],
                    "column_name": "warehouse",
                    "model": "{{ get_where_subquery(ref('stg_soda__scan')) }}",
                },
                "namespace": None,
            },
            "compiled": True,
            "resource_type": "test",
            "depends_on": {
                "macros": [
                    "macro.dbt.test_not_None",
                    "macro.dbt.get_where_subquery",
                    "macro.dbt.should_store_failures",
                    "macro.dbt.statement",
                ],
                "nodes": ["model.soda.stg_soda__scan"],
            },
            "config": {
                "enabled": True,
                "alias": None,
                "schema": "dbt_test__audit",
                "database": None,
                "tags": [],
                "meta": {},
                "materialized": "test",
                "severity": "ERROR",
                "store_failures": None,
                "where": None,
                "limit": None,
                "fail_calc": "count(*)",
                "warn_if": "!= 0",
                "error_if": "!= 0",
            },
            "database": None,
            "schema": "dbt_test__audit",
            "fqn": ["soda", "schema_test", "not_None_stg_soda__scan__warehouse"],
            "unique_id": "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e",
            "package_name": "soda",
            "root_path": "/Users/soda/github/soda/dbt",
            "path": "schema_test/not_None_stg_soda__scan__warehouse.sql",
            "original_file_path": "models/staging/soda/stg_soda__scan.yml",
            "name": "not_None_stg_soda__scan__warehouse",
            "alias": "not_None_stg_soda__scan__warehouse",
            "checksum": {"name": "none", "checksum": ""},
            "tags": ["schema"],
            "refs": [["stg_soda__scan"]],
            "sources": [],
            "description": "",
            "columns": {},
            "meta": {},
            "docs": {"show": True},
            "patch_path": None,
            "compiled_path": "target/compiled/soda/models/staging/soda/stg_soda__scan.yml/schema_test/not_None_stg_soda__scan__warehouse.sql",
            "build_path": "target/run/soda/models/staging/soda/stg_soda__scan.yml/schema_test/not_None_stg_soda__scan__warehouse.sql",
            "deferred": False,
            "unrendered_config": {},
            "created_at": 1637317368,
            "compiled_sql": "\n    \n    \n\nwith all_values as (\n\n    select\n        warehouse as value_field,\n        count(*) as n_records\n\n    from main_stg_soda.stg_soda__scan\n    group by warehouse\n\n)\n\nselect *\nfrom all_values\nwhere value_field not in (\n    'spark','postgres')\n\n\n",
            "extra_ctes_injected": True,
            "extra_ctes": [],
            "relation_name": None,
            "column_name": "warehouse",
        },
    },
}

RUN_RESULTS = {
    "metadata": {
        "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v3.json"
    },
    "results": [
        {
            "status": "pass",
            "timing": [
                {
                    "name": "compile",
                    "started_at": "2021-11-19T10:22:58.132733Z",
                    "completed_at": "2021-11-19T10:22:58.141662Z",
                },
                {
                    "name": "execute",
                    "started_at": "2021-11-19T10:22:58.142108Z",
                    "completed_at": "2021-11-19T10:23:02.286903Z",
                },
            ],
            "thread_id": "Thread-1",
            "execution_time": 4.257888078689575,
            "adapter_response": {},
            "message": None,
            "failures": 0,
            "unique_id": "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f",
        },
        {
            "status": "fail",
            "timing": [
                {
                    "name": "compile",
                    "started_at": "2021-11-19T10:23:02.506897Z",
                    "completed_at": "2021-11-19T10:23:02.514333Z",
                },
                {
                    "name": "execute",
                    "started_at": "2021-11-19T10:23:02.514773Z",
                    "completed_at": "2021-11-19T10:23:15.568742Z",
                },
            ],
            "thread_id": "Thread-1",
            "execution_time": 13.31378722190857,
            "adapter_response": {},
            "message": None,
            "failures": 3,
            "unique_id": (
                "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e"
            ),
        },
    ],
}


def test_parse_manifest_raises_not_implemented_error() -> None:
    """
    A NotImplementedError should be raised when manifest version is not v3.
    """
    with pytest.raises(NotImplementedError):
        soda_dbt.parse_manifest({"metadata": {"dbt_schema_version": "not v3"}})


@pytest.mark.parametrize(
    "unique_id",
    [
        "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f",
        "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e",
    ],
)
def test_parse_manifest_contains_unique_ids(unique_id: str) -> None:
    """Validate the unique_id are present."""
    parsed_manifest = soda_dbt.parse_manifest(MANIFEST)

    assert unique_id in parsed_manifest.keys()


def test_parse_run_results_raises_not_implemented_error() -> None:
    """
    A NotImplementedError should be raised when run results version is not v3.
    """
    with pytest.raises(NotImplementedError):
        soda_dbt.parse_run_results({"metadata": {"dbt_schema_version": "not v3"}})


@pytest.mark.parametrize(
    "result_index, status",
    [
        (0, TestStatus.Pass),
        (1, TestStatus.Fail),
    ],
)
def test_parse_run_results_status(
    result_index: int,
    status: TestStatus,
) -> None:
    """Validate the status of the nth result."""
    parsed_run_results = soda_dbt.parse_run_results(RUN_RESULTS)

    assert parsed_run_results[result_index].status == status


@pytest.mark.parametrize(
    "result_index, failures",
    [
        (0, 0),
        (1, 3),
    ],
)
def test_parse_run_results_failures(
    result_index: int,
    failures: int,
) -> None:
    """Validate the failures of the nth result."""
    parsed_run_results = soda_dbt.parse_run_results(RUN_RESULTS)

    assert parsed_run_results[result_index].failures == failures


@pytest.mark.parametrize(
    "result_index, unique_id",
    [
        (0, "test.soda.accepted_values_stg_soda__scan__result__pass_fail.81f"),
        (1, "test.soda.accepted_values_stg_soda__scan__warehouse__spark__postgres.2e"),
    ],
)
def test_parse_run_results_unique_id(result_index: int, unique_id: str) -> None:
    """Validate the unique_id of the nth result."""
    parsed_run_results = soda_dbt.parse_run_results(RUN_RESULTS)

    assert parsed_run_results[result_index].unique_id == unique_id
