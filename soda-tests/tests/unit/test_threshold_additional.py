import logging

import duckdb
from helpers.impl_test_helpers import build_contract_impl
from helpers.test_functions import dedent_and_strip
from soda_core.common.data_source_impl import DataSourceImpl
from soda_core.common.yaml import ContractYamlSource, DataSourceYamlSource, YamlObject
from soda_core.contracts.contract_verification import (
    CheckCollectionStatus,
    CheckOutcome,
    ContractVerificationSession,
)
from soda_core.contracts.impl.contract_verification_impl import (
    ThresholdImpl,
    ThresholdLevel,
    warn_can_fire_alone,
)
from soda_core.contracts.impl.contract_yaml import ThresholdYaml


def parse_threshold_yaml(threshold_yaml_str: str) -> ThresholdYaml:
    yaml_source = ContractYamlSource.from_str(dedent_and_strip(threshold_yaml_str))
    yaml_object: YamlObject = yaml_source.parse()
    return ThresholdYaml(threshold_yaml_object=yaml_object)


def assert_no_errors(caplog) -> None:
    errors: list[str] = [record.getMessage() for record in caplog.records if record.levelno >= logging.ERROR]
    assert errors == []


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_additional_threshold_parsed():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 1
        additional:
          must_be_less_than: 0
          level: warn
        """
    )
    assert threshold_yaml.must_be_less_than == 1
    assert threshold_yaml.level == "fail"
    assert threshold_yaml.additional.must_be_less_than == 0
    assert threshold_yaml.additional.level == "warn"


def test_additional_absent_is_none():
    threshold_yaml = parse_threshold_yaml("must_be_less_than: 1")
    assert threshold_yaml.additional is None
    assert threshold_yaml.level == "fail"


def test_additional_level_defaults_to_fail():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
        """
    )
    assert threshold_yaml.level == "warn"
    assert threshold_yaml.additional.level == "fail"


def test_additional_with_range_parsed():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_between:
          greater_than: 0
          less_than: 100
        additional:
          must_be_between:
            greater_than: 10
            less_than: 90
          level: warn
        """
    )
    assert threshold_yaml.additional.must_be_between.greater_than == 10
    assert threshold_yaml.additional.must_be_between.less_than == 90


# ---------------------------------------------------------------------------
# Validations
# ---------------------------------------------------------------------------


def test_outer_warn_with_bare_additional_is_valid(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
        """
    )
    assert_no_errors(caplog)


def test_outer_fail_with_warn_additional_is_valid(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 1
        additional:
          must_be_less_than: 10
          level: warn
        """
    )
    assert_no_errors(caplog)


def test_same_level_by_default_is_error(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 1
        """
    )
    assert "level 'fail'" in caplog.text
    assert "'additional' threshold level 'fail'" in caplog.text
    assert "must be different" in caplog.text


def test_explicit_fail_fail_is_error(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: fail
        additional:
          must_be_less_than: 1
          level: fail
        """
    )
    assert "must be different" in caplog.text


def test_warn_warn_is_error(caplog):
    parse_threshold_yaml(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
          level: warn
        """
    )
    assert "level 'warn'" in caplog.text
    assert "'additional' threshold level 'warn'" in caplog.text
    assert "must be different" in caplog.text


def test_additional_without_outer_comparison_is_error(caplog):
    parse_threshold_yaml(
        """
        additional:
          must_be_less_than: 1
          level: warn
        """
    )
    assert "must specify a comparison" in caplog.text


def test_unknown_key_in_additional_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  must_be_less_then: 1")
    assert "not allowed in an 'additional' threshold" in caplog.text


def test_metric_in_additional_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  metric: percent\n  must_be_less_than: 1")
    assert "not allowed in an 'additional' threshold" in caplog.text


def test_unit_in_additional_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  unit: day\n  must_be_less_than: 1")
    assert "not allowed in an 'additional' threshold" in caplog.text


def test_nested_additional_is_error(caplog):
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 5
          level: warn
          additional:
            must_be_less_than: 1
        """
    )
    assert "not allowed in an 'additional' threshold" in caplog.text
    assert threshold_yaml.additional.additional is None


def test_additional_without_comparison_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional: {}")
    assert "exactly one comparison" in caplog.text


def test_additional_with_two_comparisons_is_error(caplog):
    parse_threshold_yaml("must_be_less_than: 10\nadditional:\n  must_be_less_than: 5\n  must_be_greater_than: 1")
    assert "exactly one comparison" in caplog.text


def test_null_additional_is_error(caplog):
    threshold_yaml = parse_threshold_yaml("must_be_less_than: 10\nadditional:")
    assert threshold_yaml.additional is None
    assert "'additional' threshold must be an object" in caplog.text


def test_invalid_additional_does_not_also_report_a_level_conflict(caplog):
    """The level of an additional without a comparison is noise on top of the arity error."""
    parse_threshold_yaml("must_be_less_than: 10\nadditional: {}")
    assert "exactly one comparison" in caplog.text
    assert "must be different" not in caplog.text


def test_two_outer_comparisons_with_additional_is_error(caplog):
    """Regression: the outer threshold is one comparison too.

    Without this rule ThresholdImpl.create returns None for the fail slot with zero
    errors — the check stays NOT_EVALUATED forever while a warn threshold is still
    uploaded to Cloud.
    """
    parse_threshold_yaml(
        """
        must_be_greater_than: 10
        must_be_less_than: 1000
        additional:
          must_be_greater_than: 100
          level: warn
        """
    )
    assert "must specify exactly one comparison itself" in caplog.text
    assert "must_be_greater_than, must_be_less_than" in caplog.text


def test_two_outer_comparisons_without_additional_is_unchanged(caplog):
    """A flat multi-comparison threshold stays valid YAML (no error, so published
    contracts keep verifying); the impl logs a warning when it builds no threshold
    from it — see test_two_comparisons_without_additional_build_no_threshold_and_warn."""
    parse_threshold_yaml("must_be_greater_than: 10\nmust_be_less_than: 1000")
    assert_no_errors(caplog)


def test_outer_range_plus_comparison_with_additional_is_error(caplog):
    parse_threshold_yaml(
        """
        must_be_greater_than: 10
        must_be_between:
          greater_than: 0
          less_than: 100
        additional:
          must_be_greater_than: 100
          level: warn
        """
    )
    assert "must specify exactly one comparison itself" in caplog.text


# ---------------------------------------------------------------------------
# Level normalization: validation and slot assignment must agree
# ---------------------------------------------------------------------------


def test_unrecognized_outer_level_with_bare_additional_is_error(caplog):
    """Regression: 'warning' evaluates as 'fail', so this is a fail/fail conflict.

    Comparing the raw strings ('warning' != 'fail') let this through and inverted the
    slots: the intended warn threshold became the fail one.
    """
    parse_threshold_yaml(
        """
        must_be_greater_than: 5
        level: warning
        additional:
          must_be_greater_than: 1
        """
    )
    assert "must be different" in caplog.text
    assert "(from 'warning')" in caplog.text


def test_unrecognized_outer_level_with_warn_additional_is_valid(caplog):
    """'warning' is a fail level, so pairing it with a warn additional is legal."""
    parse_threshold_yaml(
        """
        must_be_greater_than: 10
        level: warning
        additional:
          must_be_greater_than: 100
          level: warn
        """
    )
    assert_no_errors(caplog)


def test_empty_outer_level_with_bare_additional_is_error(caplog):
    parse_threshold_yaml("must_be_greater_than: 5\nlevel: ''\nadditional:\n  must_be_greater_than: 1")
    assert "must be different" in caplog.text


def test_effective_level_matches_threshold_level_enum():
    """The two layers derive the level through the same normalization."""
    for level_str, expected in (
        ("fail", ThresholdLevel.FAIL),
        ("FAIL", ThresholdLevel.FAIL),
        ("warn", ThresholdLevel.WARN),
        ("WARN", ThresholdLevel.WARN),
        ("warning", ThresholdLevel.FAIL),
        ("", ThresholdLevel.FAIL),
    ):
        threshold_yaml = parse_threshold_yaml(f"must_be_less_than: 10\nlevel: '{level_str}'")
        assert ThresholdLevel(threshold_yaml.get_effective_level()) == ThresholdLevel.from_str(level_str)
        assert ThresholdLevel.from_str(level_str) == expected


def test_non_string_level_does_not_raise():
    """`level: 5` is a YAML type error, not an AttributeError mid-parse."""
    assert ThresholdLevel.from_str(None) == ThresholdLevel.FAIL


# ---------------------------------------------------------------------------
# Impl assignment by level
# ---------------------------------------------------------------------------


def create_impl(threshold_yaml_str: str) -> ThresholdImpl:
    return ThresholdImpl.create(threshold_yaml=parse_threshold_yaml(threshold_yaml_str))


def test_flat_threshold_unchanged():
    impl = create_impl("must_be_less_than: 10")
    assert impl.level == ThresholdLevel.FAIL
    assert impl.passes(9) and not impl.passes(10)


def test_flat_warn_level_threshold_unchanged():
    impl = create_impl("must_be_less_than: 10\nlevel: warn")
    assert impl.level == ThresholdLevel.WARN
    assert impl.passes(9) and not impl.passes(10)


def test_outer_fail_additional_warn_keeps_outer_as_primary():
    impl = create_impl(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 1
          level: warn
        """
    )
    assert impl.level == ThresholdLevel.FAIL
    assert impl.must_be_less_than == 10


def test_outer_warn_additional_fail_makes_additional_the_primary():
    impl = create_impl(
        """
        must_be_less_than: 10
        level: warn
        additional:
          must_be_less_than: 1
        """
    )
    assert impl.level == ThresholdLevel.FAIL
    assert impl.must_be_less_than == 1


def test_unrecognized_outer_level_keeps_outer_in_the_fail_slot():
    """'warning' is a fail level here too — the slots must not invert."""
    impl = create_impl(
        """
        must_be_greater_than: 10
        level: warning
        additional:
          must_be_greater_than: 100
          level: warn
        """
    )
    assert impl.level == ThresholdLevel.FAIL
    assert impl.must_be_greater_than == 10


def test_additional_ignores_default_threshold():
    from soda_core.contracts.impl.contract_verification_impl import ThresholdType

    default = ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be_greater_than=0)
    impl = ThresholdImpl.create(
        threshold_yaml=parse_threshold_yaml(
            """
            must_be_greater_than: 100
            additional:
              must_be_greater_than: 10
              level: warn
            """
        ),
        default_threshold=default,
    )
    assert impl is not default
    assert impl.level == ThresholdLevel.FAIL
    assert impl.must_be_greater_than == 100


def test_create_from_comparisons_builds_warn_impl_from_additional():
    threshold_yaml = parse_threshold_yaml(
        """
        must_be_less_than: 10
        additional:
          must_be_less_than: 5
          level: warn
        """
    )
    warn_impl = ThresholdImpl.create_from_comparisons(threshold_yaml.additional, ThresholdLevel.WARN)
    assert warn_impl.level == ThresholdLevel.WARN
    assert warn_impl.passes(4) and not warn_impl.passes(5)


# ---------------------------------------------------------------------------
# Dead-warn lint (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_warn_tighter_than_fail_can_fire():
    fail_impl = create_impl("must_be_less_than: 10")
    warn_impl = ThresholdImpl.create_from_comparisons(parse_threshold_yaml("must_be_less_than: 5"), ThresholdLevel.WARN)
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_warn_wider_than_fail_is_dead():
    fail_impl = create_impl("must_be_less_than: 5")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_less_than: 10"), ThresholdLevel.WARN
    )
    assert not warn_can_fire_alone(fail_impl, warn_impl)


def test_warn_equal_to_fail_is_dead():
    fail_impl = create_impl("must_be_less_than: 10")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_less_than: 10"), ThresholdLevel.WARN
    )
    assert not warn_can_fire_alone(fail_impl, warn_impl)


def test_nested_ranges_can_fire():
    fail_impl = create_impl("must_be_between:\n  greater_than: 0\n  less_than: 100")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_between:\n  greater_than: 10\n  less_than: 90"), ThresholdLevel.WARN
    )
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_outer_range_warn_can_fire():
    # warn passes outside [0,100] (not_between 0..100); fail passes outside [10,90].
    # fail's pass region is NOT inside warn's pass region -> warn can fire (value 5).
    fail_impl = create_impl("must_be_not_between:\n  less_than_or_equal: 10\n  greater_than_or_equal: 90")
    warn_impl = ThresholdImpl.create_from_comparisons(
        parse_threshold_yaml("must_be_not_between:\n  less_than_or_equal: 0\n  greater_than_or_equal: 100"),
        ThresholdLevel.WARN,
    )
    assert warn_can_fire_alone(fail_impl, warn_impl)


def test_dead_warn_logs_lint_warning(caplog):
    with caplog.at_level(logging.WARNING):
        build_contract_impl(
            """
            dataset: my_data_source/my_dataset
            columns:
              - name: id
                data_type: integer
            checks:
              - row_count:
                  threshold:
                    must_be_greater_than: 100
                    additional:
                      must_be_greater_than: 10
                      level: warn
            """
        )
    assert "can never produce a warn outcome" in caplog.text


# ---------------------------------------------------------------------------
# Contract verification aborts on a level conflict
# ---------------------------------------------------------------------------
#
# The whole safety argument for the level-conflict rule is that the error aborts the
# run: a contract that reaches ThresholdImpl with two fail-level thresholds puts one
# of them in the warn slot. These tests pin that no check is ever evaluated - on the
# validate-only path *and* on a real scan.

_CONFLICTING_CONTRACT_YAML = """
dataset: test_ds/main/{table}
columns:
  - name: id
checks:
  - row_count:
      threshold:
        must_be_greater_than: 10
        additional:
          must_be_greater_than: 100
"""

_LEGAL_CONTRACT_YAML = """
dataset: test_ds/main/{table}
columns:
  - name: id
checks:
  - row_count:
      threshold:
        must_be_greater_than: 10
        additional:
          must_be_greater_than: 100
          level: warn
"""

_DUCKDB_DATA_SOURCE_YAML = """
type: duckdb
name: test_ds
connection:
    database: "{database}"
    schema: main
"""


def _verify(contract_yaml_str: str, **kwargs):
    return ContractVerificationSession.execute(
        contract_yaml_sources=[ContractYamlSource.from_str(dedent_and_strip(contract_yaml_str))],
        soda_cloud_publish_results=False,
        soda_cloud_use_runner=False,
        **kwargs,
    )


def test_conflicting_levels_abort_validation():
    result = _verify(_CONFLICTING_CONTRACT_YAML.format(table="my_table"), only_validate_without_execute=True)

    assert result.has_errors is True
    assert "must be different" in result.get_errors_str()
    assert result.contract_verification_results[0].status == CheckCollectionStatus.ERROR
    assert result.contract_verification_results[0].check_results == []


def _duckdb_data_source(tmp_path, rows: int) -> DataSourceImpl:
    database_path = tmp_path / "thresholds.duckdb"
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute("CREATE TABLE my_table (id INTEGER)")
        connection.executemany("INSERT INTO my_table VALUES (?)", [(i,) for i in range(rows)])
    finally:
        connection.close()
    return DataSourceImpl.from_yaml_source(
        DataSourceYamlSource.from_str(_DUCKDB_DATA_SOURCE_YAML.format(database=database_path))
    )


def test_conflicting_levels_abort_a_real_scan(tmp_path):
    """Same contract on a real (duckdb) scan: errored, and nothing evaluated."""
    result = _verify(
        _CONFLICTING_CONTRACT_YAML.format(table="my_table"),
        data_source_impls=[_duckdb_data_source(tmp_path, rows=50)],
    )

    assert result.has_errors is True
    assert "must be different" in result.get_errors_str()
    assert result.contract_verification_results[0].status == CheckCollectionStatus.ERROR
    assert result.contract_verification_results[0].check_results == []


def test_legal_levels_do_evaluate_on_a_real_scan(tmp_path):
    """Control for the test above: the same harness does produce a check result."""
    result = _verify(
        _LEGAL_CONTRACT_YAML.format(table="my_table"),
        data_source_impls=[_duckdb_data_source(tmp_path, rows=50)],
    )

    assert result.has_errors is False
    check_results = result.contract_verification_results[0].check_results
    assert [check_result.outcome for check_result in check_results] == [CheckOutcome.WARN]


# ---------------------------------------------------------------------------
# Silent degradations now log (review round: soda-core#2818)
# ---------------------------------------------------------------------------


def test_two_comparisons_without_additional_build_no_threshold_and_warn(caplog):
    """The legacy flat two-comparison shape still builds no threshold — the check stays
    NOT_EVALUATED — but no longer silently. A warning, not an error: an error would flip
    published contracts with this shape to ERRORED on engine upgrade while the Cloud
    schema still accepts them at publish time."""
    with caplog.at_level(logging.WARNING):
        impl = ThresholdImpl.create(parse_threshold_yaml("must_be_greater_than: 10\nmust_be_less_than: 1000"))
    assert impl is None
    assert "does not specify exactly one comparison" in caplog.text
    assert "must_be_greater_than, must_be_less_than" in caplog.text
    assert_no_errors(caplog)


def test_unknown_outer_threshold_key_warns(caplog):
    """A typo like 'aditional' used to be dropped silently, turning an intended two-level
    threshold into a fail-only one. Warned, not rejected: thresholds never had unknown-key
    validation, so stray keys in published contracts must keep verifying."""
    with caplog.at_level(logging.WARNING):
        threshold_yaml = parse_threshold_yaml(
            """
            must_be_less_than: 10
            aditional:
              must_be_less_than: 5
              level: warn
            """
        )
    assert threshold_yaml.additional is None
    assert "'aditional' is not a known threshold key" in caplog.text
    assert_no_errors(caplog)


def test_metric_unit_level_and_additional_are_known_outer_keys(caplog):
    with caplog.at_level(logging.WARNING):
        parse_threshold_yaml(
            """
            metric: percent
            unit: rows
            level: fail
            must_be_less_than: 10
            additional:
              must_be_less_than: 5
              level: warn
            """
        )
    assert "not a known threshold key" not in caplog.text


def test_additional_with_default_threshold_names_the_default(caplog):
    """A check type's default threshold does not combine with an 'additional' (the Cloud
    contract schema requires the outer comparison to be explicit); the error names the
    default the author has to restate."""
    from soda_core.contracts.impl.contract_verification_impl import ThresholdType

    default = ThresholdImpl(type=ThresholdType.SINGLE_COMPARATOR, must_be_greater_than=0)
    impl = ThresholdImpl.create(
        threshold_yaml=parse_threshold_yaml(
            """
            additional:
              must_be_less_than: 5
              level: warn
            """
        ),
        default_threshold=default,
    )
    assert impl is None
    assert "does not combine with an 'additional' threshold" in caplog.text
    assert "must_be_greater_than: 0" in caplog.text


def test_warn_outer_orientation_logs_older_runner_notice(caplog):
    """A pre-`additional` engine ignores the additional threshold, so with the fail
    comparison nested there the check degrades to warn-only on that engine. The parse-time
    notice is the only signal that reaches Cloud-authored contracts."""
    with caplog.at_level(logging.WARNING):
        build_contract_impl(
            """
            dataset: my_data_source/my_dataset
            columns:
              - name: id
                data_type: integer
            checks:
              - row_count:
                  threshold:
                    level: warn
                    must_be_greater_than: 100
                    additional:
                      must_be_greater_than: 10
            """
        )
    assert "cannot fail there" in caplog.text


def test_fail_outer_orientation_logs_no_older_runner_notice(caplog):
    with caplog.at_level(logging.WARNING):
        build_contract_impl(
            """
            dataset: my_data_source/my_dataset
            columns:
              - name: id
                data_type: integer
            checks:
              - row_count:
                  threshold:
                    must_be_greater_than: 10
                    additional:
                      must_be_greater_than: 100
                      level: warn
            """
        )
    assert "cannot fail there" not in caplog.text
