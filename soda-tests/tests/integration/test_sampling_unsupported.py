"""A source that cannot sample must refuse a sampled scan, not run it unsampled.

`supports_row_sampling()` defaults True, so this is dormant for every SQL data source. Salesforce is the
only source that returns False today, and the guard has two halves that both matter:

- at construction, the sampler is not attached to the query. Without this half, query BUILDING reaches
  the dialect's `_build_sample_sql`, whose default raises NotImplementedError inside
  `CheckCollectionImpl.__init__` — before the execute loop exists to catch anything — so the whole
  verification aborts.
- at execution, the queries are refused and the reason is reported, which is what turns the refusal into
  NOT_EVALUATED for every check on the dataset rather than a raised exception.

These run on every data source by patching the capability flag, reproducing the condition Salesforce
reports natively. Nothing here asserted the guard before; it was covered only by a live Salesforce test.
"""

from unittest import mock

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_table import TestTableSpecification
from soda_core.common.soda_cloud_dto import (
    DatasetConfigurationDTO,
    TestRowSamplerConfigurationDTO,
)

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("sampling_unsupported")
    .column_varchar("country", 3)
    .column_integer("amount")
    .rows(rows=[("BE", 10), ("BE", 20), ("US", 30), ("US", 40), ("US", 50)])
    .build()
)


def _request_sampling(data_source_test_helper: DataSourceTestHelper, test_table) -> None:
    """Configure a sampler the way Cloud does, so `should_apply_sampling` is genuinely derived."""
    data_source_test_helper.enable_soda_cloud_mock()
    data_source_test_helper.soda_cloud.set_dataset_configuration_response(
        dataset_identifier=test_table.dataset_identifier,
        dataset_configuration_dto=DatasetConfigurationDTO(
            test_row_sampler_configuration=TestRowSamplerConfigurationDTO(
                enabled=True, test_row_sampler={"type": "absoluteLimit", "limit": 3}
            )
        ),
    )


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_runner",
    new_callable=mock.PropertyMock(return_value=True),
)
@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_contract_test_scan_definition_type",
    new_callable=mock.PropertyMock(return_value=True),
)
def test_a_source_that_cannot_sample_refuses_a_sampled_scan(
    mocked_runner, mocked_scan_type, data_source_test_helper: DataSourceTestHelper, monkeypatch, caplog
):
    from soda_core.common.sql_dialect import SqlDialect

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    _request_sampling(data_source_test_helper, test_table)
    monkeypatch.setattr(SqlDialect, "supports_row_sampling", lambda self: False)

    result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - row_count:
                  threshold:
                    must_be: 5
        """,
    )
    outcomes = [cr.outcome.name for cvr in result.contract_verification_results for cr in cvr.check_results]
    assert outcomes == ["NOT_EVALUATED"], "a full-scan number must not be reported as a sample"
    assert result.has_errors
    assert "cannot sample rows" in caplog.text


@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_running_on_runner",
    new_callable=mock.PropertyMock(return_value=True),
)
@mock.patch(
    "soda_core.common.env_config_helper.EnvConfigHelper.is_contract_test_scan_definition_type",
    new_callable=mock.PropertyMock(return_value=True),
)
def test_every_check_on_the_dataset_is_refused_together(
    mocked_runner, mocked_scan_type, data_source_test_helper: DataSourceTestHelper, monkeypatch
):
    """Sampling is configured per dataset, so honouring some checks unsampled would be worse than none."""
    from soda_core.common.sql_dialect import SqlDialect

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    _request_sampling(data_source_test_helper, test_table)
    monkeypatch.setattr(SqlDialect, "supports_row_sampling", lambda self: False)

    result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: amount
                checks:
                  - aggregate:
                      function: sum
                      threshold:
                        must_be: 150
            checks:
              - row_count:
                  threshold:
                    must_be: 5
        """,
    )
    outcomes = [cr.outcome.name for cvr in result.contract_verification_results for cr in cvr.check_results]
    assert outcomes == ["NOT_EVALUATED", "NOT_EVALUATED"]


def test_a_source_that_can_sample_is_untouched(data_source_test_helper: DataSourceTestHelper):
    """The control, and the reason this guard is safe to add to shared code: the flag defaults True, so
    with no sampling requested nothing changes for any existing data source."""
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    assert data_source_test_helper.data_source_impl.sql_dialect.supports_row_sampling() is True

    result = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            checks:
              - row_count:
                  threshold:
                    must_be: 5
        """,
    )
    assert result.check_results[0].threshold_value == 5
