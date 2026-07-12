from helpers.data_source_test_helper import DataSourceTestHelper
from helpers.test_functions import dedent_and_strip
from helpers.test_table import TestTableSpecification
from soda_core.contracts.contract_verification import ContractVerificationResult

test_table_specification = (
    TestTableSpecification.builder()
    .table_purpose("logs_formatting")
    .column_varchar("id")
    .rows(
        rows=[
            ("1",),
            ("2",),
            ("3",),
        ]
    )
    .build()
)


def test_split_log_lines(data_source_test_helper: DataSourceTestHelper):
    test_table = data_source_test_helper.ensure_test_table(test_table_specification)
    contract_verification_result: ContractVerificationResult = data_source_test_helper.assert_contract_pass(
        test_table=test_table,
        contract_yaml_str=dedent_and_strip(
            """
    checks:
      - row_count:
          threshold:
            must_be: 3
    """
        ),
    )
    log_lines = contract_verification_result.get_logs()

    expected = [
        # A string-sourced contract (no local file path, no collection id) falls
        # back to the yaml source's description instead of printing 'None'.
        "Verifying contract 📜 Contract YAML string 🤞",
        # some (many) non-deterministic log lines are skipped, we just need to make sure table is correctly split over multiple lines
        "+-----------------+------------------------------------+-------------+-----------+--------------+------------+------------------------+",
        "| Column          | Check                              | Threshold   | Outcome   | Check Type   | Identity   | Diagnostics            |",
        "+=================+====================================+=============+===========+==============+============+========================+",
        # "| [dataset-level] | Row count meets expected threshold | level: fail | ✅ PASSED | row_count    | 2ccd8a76   | check_rows_tested: 3   |",
        "|                 |                                    | must be: 3  |           |              |            | dataset_rows_tested: 3 |",
        "+-----------------+------------------------------------+-------------+-----------+--------------+------------+------------------------+",
        "# Summary:",
        "|----------------|---|----|",
        "| Checks         | 1 |    |",
        "| Passed         | 1 | ✅ |",
        "| Failed         | 0 | ✅ |",
        "| Warned         | 0 | ✅ |",
        "| Not Evaluated  | 0 | ✅ |",
        "| Excluded       | 0 | ✅ |",
        "| Runtime Errors | 0 | ✅ |",
    ]
    for line in expected:
        assert line in log_lines

    # The log_table_extra_columns seam is opt-in: contract verification never
    # supplies extra columns, so no "Window" (or other caller-supplied)
    # column may ever appear in its table.
    assert not any("Window" in line for line in log_lines)
