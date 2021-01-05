# Tests

In scan.yml files it's possible to add tests that determine if a 
scan succeeds or fails.  Tests are simple expressions like for example 

* `min > 25`
* `missing_percentage <= 2.5`
* `5 <= avg_length and avg_length <= 10`

Each test should be based on 1 single metric.
Tests are expressions written in Python syntax

Disclaimer: This is experimental for now.  We may limit the expression 
capabilities as necessary to provide safely execute tests on cloud accounts.