from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import get_diagnostic_value
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import (
    CheckOutcome,
    CheckResult,
    ContractVerificationResult,
)

referencing_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referencing")
    .column_integer("id")
    .column_varchar("country", 2)
    .column_varchar("zip", 6)
    .rows(
        rows=[
            (1, "NL", "NL4775"),
            (2, "NL", "XXXXXX"),
            (3, "XX", "NL4775"),
            (4, "BE", "NL4775"),
            (5, "XX", "XXXXXX"),
            (6, "XX", None),
            (7, None, "XXXXXX"),
            (8, None, None),
        ]
    )
    .build()
)


referenced_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referenced")
    .column_varchar("country_code", 2)
    .column_varchar("zip_code", 6)
    .rows(
        rows=[
            ("NL", "NL4775"),
            ("BE", "2300"),
        ]
    )
    .build()
)


# Reference table that intentionally shares an `id` column with the source
# (`referencing_table_specification`). Used to exercise the case where a
# check-level filter on `id` becomes ambiguous in the JOIN-style invalid
# reference query.
referenced_table_with_shared_id_specification = (
    TestTableSpecification.builder()
    .table_purpose("invalid_referenced_shared_id")
    .column_integer("id")
    .column_varchar("country_code", 2)
    .rows(
        rows=[
            (100, "NL"),
            (101, "BE"),
        ]
    )
    .build()
)


def test_invalid_count(data_source_test_helper: DataSourceTestHelper):
    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/2945ba9d-b1ff-4cfd-b277-d5e4edfa2bd5/checks

    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 3


def test_invalid_count_warn(data_source_test_helper: DataSourceTestHelper):
    # https://dev.sodadata.io/o/f35cb402-ad17-4aca-9166-02c9eb75c979/datasets/2945ba9d-b1ff-4cfd-b277-d5e4edfa2bd5/checks

    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_warn(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
                      threshold:
                        level: warn
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 3


def test_invalid_count_with_check_filter(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    zip_quoted: str = data_source_test_helper.quote_column("zip")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
                      filter: |
                        {zip_quoted} = 'NL4775'
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 1
    assert get_diagnostic_value(check_result, "check_rows_tested") == 3


def test_invalid_count_with_check_filter_on_shared_column(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_with_shared_id_specification)

    id_quoted: str = data_source_test_helper.quote_column("id")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
                      filter: |
                        {id_quoted} > 1
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 3
    assert get_diagnostic_value(check_result, "check_rows_tested") == 7


def test_invalid_count_with_check_and_dataset_filter(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    zip_quoted: str = data_source_test_helper.quote_column("zip")
    id_quoted: str = data_source_test_helper.quote_column("id")

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            filter: |
              {id_quoted} > 1
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
                      filter: |
                        {zip_quoted} = 'NL4775'
        """,
    )
    check_result: CheckResult = contract_verification_result.check_results[0]
    assert get_diagnostic_value(check_result, "invalid_count") == 1
    assert get_diagnostic_value(check_result, "check_rows_tested") == 2


def test_two_invalid_reference_checks_with_different_filters_resolve_to_distinct_metrics(
    data_source_test_helper: DataSourceTestHelper,
):
    """Two reference-invalidity checks on the same column with the same
    valid_reference_data but different `filter:` must produce independent
    measurements. Regression for InvalidReferenceCountMetricImpl omitting
    check_filter from its identity hash — without that, both metrics share
    an id and the last InvalidReferenceCountQuery write clobbers the first.
    """
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    id_quoted: str = data_source_test_helper.quote_column("id")

    # Test data has 8 rows; reference table contains valid country codes NL, BE.
    #   id <= 4 → rows 1-4, of which only row 3 (XX) is invalid (not missing,
    #             not in ref) → invalid_count = 1.
    #   id  > 4 → rows 5-8, of which rows 5,6 (XX) are invalid; rows 7,8 have
    #             None country (missing, excluded from invalid) → invalid_count = 2.
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_fail(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                checks:
                  - invalid:
                      qualifier: low_ids
                      filter: |
                        {id_quoted} <= 4
                  - invalid:
                      qualifier: high_ids
                      filter: |
                        {id_quoted} > 4
        """,
    )

    check_by_qualifier = {cr.check.qualifier: cr for cr in contract_verification_result.check_results}
    low_ids = check_by_qualifier["low_ids"]
    high_ids = check_by_qualifier["high_ids"]

    assert get_diagnostic_value(low_ids, "invalid_count") == 1
    assert get_diagnostic_value(high_ids, "invalid_count") == 2

    # Both have > 0 invalid vs default must_be=0 → both fail. Pinned so a
    # wrong outcome doesn't hide behind assert_contract_fail (any-check-fail).
    assert low_ids.outcome == CheckOutcome.FAILED
    assert high_ids.outcome == CheckOutcome.FAILED


def test_invalid_count_excl_missing(data_source_test_helper: DataSourceTestHelper):
    referencing_table = data_source_test_helper.ensure_test_table(referencing_table_specification)
    referenced_table = data_source_test_helper.ensure_test_table(referenced_table_specification)

    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=referencing_table,
        contract_yaml_str=f"""
            columns:
              - name: country
                valid_reference_data:
                  dataset: {data_source_test_helper.build_dqn(referenced_table)}
                  column: country_code
                missing_values: [XX]
                checks:
                  - invalid:
        """,
    )
    assert (
        get_diagnostic_value(
            check_result=contract_verification_result.check_results[0], diagnostic_name="invalid_count"
        )
        == 0
    )
