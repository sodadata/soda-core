from types import SimpleNamespace

import duckdb
import pytest
from helpers.mock_soda_cloud import MockSodaCloud
from helpers.test_functions import dedent_and_strip
from soda_core.common.data_source_results import QueryResult
from soda_core.common.dataset_identifier import DatasetIdentifier
from soda_core.common.exceptions import InvalidArgumentException
from soda_core.common.metadata_types import SamplerType
from soda_core.common.soda_cloud_dto import (
    DatasetConfigurationDTO,
    TestRowSamplerConfigurationDTO,
)
from soda_core.common.soda_cloud_dto import SamplerType as SamplerTypeDTO
from soda_core.common.yaml import ContractYamlSource
from soda_core.contracts import FailedRowsSampler
from soda_core.contracts.api.verify_api import verify_contract
from soda_core.contracts.contract_verification import Check, CheckOutcome, CheckResult
from soda_core.contracts.impl.check_types.failed_rows_check import (
    FailedRowsCountQuery,
    FailedRowsSampleQuery,
)
from soda_core.contracts.impl.contract_verification_impl import ContractImpl
from soda_duckdb.common.data_sources.duckdb_data_source import (
    DuckDBDataSourceImpl,
    DuckDBSqlDialect,
)


class RecordingSampler(FailedRowsSampler):
    def __init__(self):
        self.calls = []

    def process_sample(self, rows, columns, check_result):
        self.calls.append((rows, columns, check_result))


class RaisingSampler(FailedRowsSampler):
    def process_sample(self, rows, columns, check_result):
        raise RuntimeError("sampler failed")


def _duckdb_data_source(rows):
    connection = duckdb.connect(":memory:")
    connection.execute("CREATE TABLE failures (id INTEGER, reason VARCHAR, key_value VARCHAR)")
    connection.executemany("INSERT INTO failures VALUES (?, ?, ?)", rows)
    return DuckDBDataSourceImpl.from_existing_cursor(connection, name="fx")


def _local_contract(
    monkeypatch, contract_yaml, data_source, sampler, limit=2, sampler_type=SamplerTypeDTO.ABSOLUTE_LIMIT
):
    cloud = MockSodaCloud()
    cloud.set_dataset_configuration_response(
        DatasetIdentifier.parse("fx/main/failures"),
        DatasetConfigurationDTO(
            test_row_sampler_configuration=TestRowSamplerConfigurationDTO(
                enabled=True,
                test_row_sampler={"type": sampler_type, "limit": limit},
            )
        ),
    )
    monkeypatch.setattr(
        "soda_core.contracts.api.verify_api._create_contract_yamls",
        lambda *args, **kwargs: [ContractYamlSource.from_str(dedent_and_strip(contract_yaml))],
    )
    monkeypatch.setattr("soda_core.contracts.api.verify_api.SodaCloud.from_config", lambda *args, **kwargs: cloud)
    from soda_core.contracts.api.verify_api import verify_contract_locally

    return verify_contract_locally(
        data_sources=[data_source],
        contract_file_path="mock-contract.yml",
        soda_cloud_file_path="mock-soda-cloud.yml",
        sampler=sampler,
    )


class FakeDialect:
    def build_select_sql(self, elements):
        return "SELECT COUNT(*) FROM failed_rows"


class FakeDataSource:
    def __init__(self, count, rows, columns=("id", "reason")):
        self.sql_dialect = FakeDialect()
        self.count = count
        self.rows = rows
        self.columns = columns
        self.row_limits = []
        self.one_by_one_calls = 0

    def execute_query(self, sql):
        return QueryResult(rows=[(self.count,)], columns=())

    def execute_query_one_by_one(self, sql, row_callback, row_limit=None, **kwargs):
        self.one_by_one_calls += 1
        self.row_limits.append(row_limit)
        description = tuple((name, None, None, None, None, None, None) for name in self.columns)
        for row in self.rows[:row_limit] if row_limit is not None else self.rows:
            row_callback(row, description)
        return description


def _check_result(outcome=CheckOutcome.FAILED):
    check = Check(
        type="failed_rows",
        qualifier=None,
        name="failed rows",
        relative_path="checks.failed_rows",
        identity="identity",
        definition="query",
        column_name=None,
        contract_file_line=1,
        contract_file_column=1,
        threshold=None,
        attributes=None,
        location=None,
    )
    return CheckResult(check=check, outcome=outcome)


def _check_impl(sampler, rows, limit=2):
    return SimpleNamespace(
        contract_impl=SimpleNamespace(sampler=sampler, sampler_limit=limit, sampler_type=SamplerType.ABSOLUTE_LIMIT),
        sample_rows=rows,
        sample_columns=["id", "reason"],
    )


def test_public_sampler_receives_rows_columns_and_final_result_once():
    sampler = RecordingSampler()
    check_impl = _check_impl(sampler, [{"id": 1, "reason": "bad"}])
    result = _check_result()

    from soda_core.contracts.impl.check_types.failed_rows_check import (
        FailedRowsCheckImpl,
    )

    FailedRowsCheckImpl.process_sample(check_impl, result)

    assert sampler.calls == [([{"id": 1, "reason": "bad"}], ["id", "reason"], result)]


def test_failed_rows_count_query_respects_engine_limit_and_preserves_columns():
    sampler = RecordingSampler()
    data_source = FakeDataSource(3, [(1, "a"), (2, "b"), (3, "c")])
    check_impl = _check_impl(sampler, [])
    metric = SimpleNamespace(id="metric-id", type="failed_rows")
    query = FailedRowsCountQuery(data_source, [metric], "SELECT id, reason FROM failures", check_impl)

    measurements = query.execute()

    assert measurements[0].value == 3
    assert data_source.row_limits == [2]
    assert check_impl.sample_rows == [{"id": 1, "reason": "a"}, {"id": 2, "reason": "b"}]
    assert check_impl.sample_columns == ["id", "reason"]


def test_percentage_sampler_limit_is_not_used_as_row_limit():
    sampler = RecordingSampler()
    data_source = FakeDataSource(2, [(1, "a"), (2, "b")])
    check_impl = _check_impl(sampler, [])
    check_impl.contract_impl.sampler_type = SamplerType.PERCENTAGE
    metric = SimpleNamespace(id="metric-id", type="failed_rows")

    FailedRowsCountQuery(data_source, [metric], "SELECT id, reason FROM failures", check_impl).execute()

    assert data_source.row_limits == [None]


def test_failed_rows_sample_query_supports_expression_style_rows():
    sampler = RecordingSampler()
    data_source = FakeDataSource(0, [(10, "invalid")])
    check_impl = _check_impl(sampler, [])
    query = FailedRowsSampleQuery(data_source, "SELECT * FROM failures WHERE invalid", check_impl)

    assert query.execute() == []
    assert check_impl.sample_rows == [{"id": 10, "reason": "invalid"}]
    assert check_impl.sample_columns == ["id", "reason"]


def test_empty_sample_does_not_invoke_callback():
    sampler = RecordingSampler()
    check_impl = _check_impl(sampler, [])

    from soda_core.contracts.impl.check_types.failed_rows_check import (
        FailedRowsCheckImpl,
    )

    FailedRowsCheckImpl.process_sample(check_impl, _check_result(CheckOutcome.PASSED))

    assert sampler.calls == []


def test_no_sampler_does_not_invoke_callback():
    check_impl = _check_impl(None, [{"id": 1, "reason": "bad"}])
    from soda_core.contracts.impl.check_types.failed_rows_check import (
        FailedRowsCheckImpl,
    )

    FailedRowsCheckImpl.process_sample(check_impl, _check_result())


def test_sampler_exception_is_not_silently_swallowed():
    check_impl = _check_impl(RaisingSampler(), [{"id": 1, "reason": "bad"}])
    from soda_core.contracts.impl.check_types.failed_rows_check import (
        FailedRowsCheckImpl,
    )

    with pytest.raises(RuntimeError, match="sampler failed"):
        FailedRowsCheckImpl.process_sample(check_impl, _check_result())


def test_sampler_has_no_limit_configuration_api():
    assert "sample_limit" not in FailedRowsSampler.__abstractmethods__
    assert "sample_limit" not in FailedRowsSampler.__dict__


def test_sampler_is_rejected_explicitly_for_runner_execution():
    with pytest.raises(InvalidArgumentException, match="only supported for local"):
        verify_contract(
            contract_file_path=None,
            dataset_identifier=None,
            data_source_file_path=None,
            soda_cloud_file_path=None,
            publish=False,
            use_runner=True,
            sampler=RecordingSampler(),
        )


def test_verify_contract_locally_delivers_query_sample_end_to_end(monkeypatch):
    sampler = RecordingSampler()
    result = _local_contract(
        monkeypatch,
        """
        dataset: fx/main/failures
        columns: []
        checks:
          - failed_rows:
              qualifier: query_check
              query: SELECT id, reason, key_value FROM failures WHERE id > 0
        """,
        _duckdb_data_source([(1, "bad", "a"), (2, "bad", "b"), (3, "bad", "c")]),
        sampler,
        limit=2,
    )

    check_result = result.contract_verification_results[0].check_results[0]
    assert check_result.outcome == CheckOutcome.FAILED
    assert len(sampler.calls) == 1
    rows, columns, callback_result = sampler.calls[0]
    assert rows == [{"id": 1, "reason": "bad", "key_value": "a"}, {"id": 2, "reason": "bad", "key_value": "b"}]
    assert columns == ["id", "reason", "key_value"]
    assert callback_result is check_result


def test_verify_contract_locally_delivers_expression_sample_end_to_end(monkeypatch):
    sampler = RecordingSampler()
    result = _local_contract(
        monkeypatch,
        """
        dataset: fx/main/failures
        columns: []
        checks:
          - failed_rows:
              expression: id > 1
        """,
        _duckdb_data_source([(1, "ok", "a"), (2, "bad", "b"), (3, "bad", "c")]),
        sampler,
        limit=1,
    )

    assert result.contract_verification_results[0].check_results[0].outcome == CheckOutcome.FAILED
    assert len(sampler.calls) == 1
    assert sampler.calls[0][0] == [{"id": 2, "reason": "bad", "key_value": "b"}]
    assert sampler.calls[0][1] == ["id", "reason", "key_value"]


def test_expression_percentage_sampling_uses_dialect_sampling_not_row_limit(monkeypatch):
    sampler = RecordingSampler()
    monkeypatch.setattr(ContractImpl, "is_test_verification_on_runner", property(lambda self: True))
    monkeypatch.setattr("soda_core.check_collections.base.map_sampler_type_from_dto", lambda _: SamplerType.PERCENTAGE)
    sampling_calls = []
    monkeypatch.setattr(DuckDBSqlDialect, "_build_sample_sql", lambda self, sampler_type, sample_size: "")

    def apply_sampling(self, sql, sampler_limit, sampler_type):
        sampling_calls.append((sampler_limit, sampler_type))
        return f"{sql.rstrip(' ;')} LIMIT 1"

    monkeypatch.setattr(DuckDBSqlDialect, "apply_sampling", apply_sampling)
    result = _local_contract(
        monkeypatch,
        """
        dataset: fx/main/failures
        columns: []
        checks:
          - failed_rows:
              expression: id > 0
        """,
        _duckdb_data_source([(1, "bad", "a"), (2, "bad", "b")]),
        sampler,
        limit=100,
        sampler_type=SamplerTypeDTO.ABSOLUTE_LIMIT,
    )

    assert result.contract_verification_results[0].check_results[0].outcome == CheckOutcome.FAILED
    assert len(sampler.calls) == 1
    assert len(sampler.calls[0][0]) == 1
    assert (100, SamplerType.PERCENTAGE) in sampling_calls


def test_verify_contract_locally_with_zero_failed_rows_does_not_invoke_sampler(monkeypatch):
    sampler = RecordingSampler()
    result = _local_contract(
        monkeypatch,
        """
        dataset: fx/main/failures
        columns: []
        checks:
          - failed_rows:
              query: SELECT id, reason FROM failures WHERE id < 0
        """,
        _duckdb_data_source([(1, "ok", "a"), (2, "ok", "b")]),
        sampler,
        limit=1,
    )

    check_result = result.contract_verification_results[0].check_results[0]
    assert check_result.outcome == CheckOutcome.PASSED
    assert check_result.diagnostic_metric_values["failed_rows_count"] == 0
    assert sampler.calls == []


def test_sampler_does_not_change_failed_rows_count_or_threshold_result(monkeypatch):
    contract = """
    dataset: fx/main/failures
    columns: []
    checks:
      - failed_rows:
          query: SELECT id, reason FROM failures WHERE id > 0
          must_be_less_than: 2
    """
    with_sampler = (
        _local_contract(
            monkeypatch,
            contract,
            _duckdb_data_source([(1, "bad", "a"), (2, "bad", "b"), (3, "bad", "c")]),
            RecordingSampler(),
            limit=1,
        )
        .contract_verification_results[0]
        .check_results[0]
    )
    without_sampler = (
        _local_contract(
            monkeypatch,
            contract,
            _duckdb_data_source([(1, "bad", "a"), (2, "bad", "b"), (3, "bad", "c")]),
            None,
            limit=1,
        )
        .contract_verification_results[0]
        .check_results[0]
    )

    assert with_sampler.outcome == without_sampler.outcome == CheckOutcome.FAILED
    assert with_sampler.threshold_value == without_sampler.threshold_value == 3
    assert with_sampler.diagnostic_metric_values["failed_rows_count"] == 3
    assert without_sampler.diagnostic_metric_values["failed_rows_count"] == 3


def test_verify_contract_locally_delivers_keys_query_keys_not_complete_records(monkeypatch):
    sampler = RecordingSampler()
    result = _local_contract(
        monkeypatch,
        """
        dataset: fx/main/failures
        columns: []
        checks:
          - failed_rows:
              keys_query: SELECT key_value FROM failures WHERE id > 0
        """,
        _duckdb_data_source([(1, "bad", "a"), (2, "bad", "b")]),
        sampler,
        limit=2,
    )

    check_result = result.contract_verification_results[0].check_results[0]
    assert check_result.outcome == CheckOutcome.FAILED
    assert len(sampler.calls) == 1
    assert sampler.calls[0][0] == [{"key_value": "a"}, {"key_value": "b"}]
    assert sampler.calls[0][1] == ["key_value"]


def test_same_sampler_is_reused_without_sharing_samples(monkeypatch):
    sampler = RecordingSampler()
    result = _local_contract(
        monkeypatch,
        """
        dataset: fx/main/failures
        columns: []
        checks:
          - failed_rows:
              qualifier: first
              query: SELECT id, reason FROM failures WHERE id > 0
          - failed_rows:
              qualifier: second
              query: SELECT id, reason FROM failures WHERE id > 1
        """,
        _duckdb_data_source([(1, "a", "a"), (2, "b", "b"), (3, "c", "c")]),
        sampler,
        limit=1,
    )

    assert len(result.contract_verification_results[0].check_results) == 2
    assert len(sampler.calls) == 2
    assert sampler.calls[0][0] == [{"id": 1, "reason": "a"}]
    assert sampler.calls[1][0] == [{"id": 2, "reason": "b"}]
    assert sampler.calls[0][2] is not sampler.calls[1][2]


def test_sampler_exception_propagates_from_local_verification(monkeypatch):
    with pytest.raises(RuntimeError, match="sampler failed"):
        _local_contract(
            monkeypatch,
            """
            dataset: fx/main/failures
            columns: []
            checks:
              - failed_rows:
                  query: SELECT id FROM failures WHERE id > 0
            """,
            _duckdb_data_source([(1, "bad", "a")]),
            RaisingSampler(),
        )
