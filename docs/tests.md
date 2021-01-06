# Tests

Tests are evaluated as part of scans and product test results as 
part of the scan result.  When using the CLI, the exit code will be 
determined by the test results.
 
Tests are simple Python expressions where the metrics are available 
as variables. 

For example:

* `min > 25`
* `missing_percentage <= 2.5`
* `5 <= avg_length and avg_length <= 10`

Tests can be specified on 3 different places:

* `tests` in scan.yml on top level for testing `row_count` and other table level metrics (TODO) 
* `tests` in scan.yml on a column level for testing column metrics
* `tests` in user defined SQL metrics yaml files (TODO)  

> **Disclaimer**: This is experimental for now.  We may limit the expression 
capabilities as necessary to provide safely execute tests on cloud accounts.