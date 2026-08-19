"""A data source may declare which check types it can answer.

Default is None — no restriction — so every existing data source keeps running every registered check
type. A source that speaks something other than SQL declares its set, and a check outside it reports
NOT_EVALUATED naming the source, rather than failing further in where the error reads like a defect. A
`group_by` check on Salesforce, for instance, sends a user-written grouped query to an org that answers
SOQL, and comes back as a raw MALFORMED_QUERY.

NOT_EVALUATED rather than EXCLUDED is the point: EXCLUDED means the user deselected the check, which is
a different statement from "you asked for this and are not getting it".

The refusal is logged as an error, which puts the contract on ERROR rather than UNKNOWN. A check outside
the declared set can never evaluate against this source, so it is a contract defect a human has to fix,
not a transient miss — a green exit would hide a contract that permanently asserts less than it claims.

These patch the capability onto whatever source the suite runs against, so the behaviour is asserted
everywhere rather than only on the source that declares a set.
"""

from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.mock_soda_cloud import MockResponse
from helpers.test_table import TestTableSpecification
from soda_core.common.data_source_impl import DataSourceImpl

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("supported_check_types")
    .column_varchar("id")
    .column_integer("amount")
    .rows(rows=[("1", 10), ("2", 20), (None, 30)])
    .build()
)


def _only(data_source_test_helper: DataSourceTestHelper, monkeypatch, *types: str) -> None:
    monkeypatch.setattr(
        type(data_source_test_helper.data_source_impl),
        "supported_check_types",
        property(lambda self: frozenset(types)),
    )


def _outcomes(result) -> list[str]:
    return [cr.outcome.name for cvr in result.contract_verification_results for cr in cvr.check_results]


def test_a_check_type_outside_the_declared_set_is_not_evaluated(
    data_source_test_helper: DataSourceTestHelper, monkeypatch, caplog
):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    _only(data_source_test_helper, monkeypatch, "row_count")

    result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
                checks:
                  - missing:
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
    )
    # The supported check still runs and measures; only the unsupported one is refused.
    assert _outcomes(result) == ["NOT_EVALUATED", "PASSED"]
    assert "does not support 'missing' checks" in caplog.text
    # ERROR, not UNKNOWN: a check that can never evaluate here is a contract defect needing a human,
    # so the run exits 3 instead of quietly exiting 0 while asserting less than the contract claims.
    assert result.has_errors


def test_a_refused_check_does_not_stop_its_siblings(
    data_source_test_helper: DataSourceTestHelper, monkeypatch
):
    """Refusing one check must not cost the rest of the contract.

    The refusal is applied where checks are parsed, before metrics are set up — which is what keeps a
    check that runs a user-written query from sending it to a source that cannot run it. That placement
    is observable here only as its consequence: the refused check reports nothing while every supported
    check beside it still measures a real value.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    _only(data_source_test_helper, monkeypatch, "row_count", "aggregate")

    result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
                checks:
                  - missing:
              - name: amount
                checks:
                  - aggregate:
                      function: sum
                      threshold:
                        must_be: 60
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
    )
    check_results = [cr for cvr in result.contract_verification_results for cr in cvr.check_results]
    assert [cr.outcome.name for cr in check_results] == ["NOT_EVALUATED", "PASSED", "PASSED"]
    assert [cr.threshold_value for cr in check_results] == [None, 60, 3]


def test_a_refused_check_can_still_be_published_to_cloud(
    data_source_test_helper: DataSourceTestHelper, monkeypatch
):
    """The payload builder dereferences diagnostics for several check types and only guards EXCLUDED.

    A refused check that carried no diagnostics raised an AttributeError out of the whole verification,
    discarding every other check's result with it — worse than the failure the gate prevents. Every
    check type below reaches a different branch of that builder.
    """
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    _only(data_source_test_helper, monkeypatch)  # nothing supported
    data_source_test_helper.enable_soda_cloud_mock(
        [MockResponse(status_code=200, json_object={"fileId": "a81bc81b-dead-4e5d-abff-90865d1e13b1"})]
    )

    result = data_source_test_helper.verify_contract(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
                checks:
                  - missing:
                  - invalid:
                      valid_values: ['1', '2']
                  - duplicate:
              - name: amount
                checks:
                  - aggregate:
                      function: sum
                      threshold:
                        must_be: 60
            checks:
              - row_count:
              - schema:
        """,
    )
    assert _outcomes(result) == ["NOT_EVALUATED"] * 6


def test_the_default_places_no_restriction(data_source_test_helper: DataSourceTestHelper):
    """The control, and why this is safe to add to shared code: unset means everything runs."""
    assert data_source_test_helper.data_source_impl.supported_check_types is None
    assert DataSourceImpl.supported_check_types.fget(data_source_test_helper.data_source_impl) is None

    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    result = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str="""
            columns:
              - name: id
                checks:
                  - missing:
                      threshold:
                        must_be: 1
            checks:
              - row_count:
                  threshold:
                    must_be: 3
        """,
    )
    assert [cr.threshold_value for cr in result.check_results] == [1, 3]
