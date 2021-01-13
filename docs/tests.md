# Tests

Tests are evaluated as part of a scan and produce test results which are
part of the scan's result.  When using the CLI to run a scan (`soda scan`), the exit code will be
based on the test results. A failing test will case the exit code to be `non-zero`.

Tests can be written as simple Python expressions where the metrics are available
as variables.

For example:

* `min > 25`
* `missing_percentage <= 2.5`
* `5 <= avg_length and avg_length <= 10`

Tests can be specified on 3 different places:

* `tests` in `scan.yml` on top level for testing `row_count` and other table level metrics
* `tests` in `scan.yml` on a column level for testing column metrics
* `tests` in [user defined SQL metrics](sql_metrics.md) yaml files
