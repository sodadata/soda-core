import pytest

from sodasql import dbt as soda_dbt

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
            "unique_id": "test.soda.accepted_values_soda_scan__pass_fail.81f",
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
                "test.ritoku.accepted_values_soda_warehouse__spark__postgres.2e"
            ),
        },
    ],
}


def test_parse_run_results_raises_not_implemented_error() -> None:
    """
    A NotImplementedError should be raised when run results version is not v3.
    """
    with pytest.raises(NotImplementedError):
        soda_dbt.parse_run_results({"metadata": {"dbt_schema_version": "not v3"}})


@pytest.mark.parametrize(
    "result_index, status",
    [
        (0, soda_dbt.Status.PASS),
        (1, soda_dbt.Status.FAIL),
    ],
)
def test_parse_run_results_status(
    result_index: int,
    status: soda_dbt.Status,
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
