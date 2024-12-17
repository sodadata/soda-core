# Design decisions

# Roadmap

* Set up a local development environment
  * Execute contract verification programmatically
  * Provide YAML file content via file path, yaml string or yaml dict.
  * Show errors with line numbers and/or links to documentation
  * Programmatic access to the contract results
  * Verify multiple contracts on a single connection
  * Print logs to console (python logging) in local dev env or orchestration tool logging
  * Send logs to soda cloud in streaming to tail the logs for testing a contract verification using agent
* Parse contract incl static analysis without requiring a connection
* Basic check types: missing, validity & uniqueness
* Send contract verification results to Soda Cloud
* Produce single failed rows table inside data source for all checks (masks)  
* Execute contract verification on Soda Agent
* Embed contract verification in Airflow
* Set up contract verification as part of CI/CD
* Check type pluggability
* Execute Soda Cloud checks as part of a programmatic contract verification
* Reference contract and data source files in another external git repository
* Named, reusable filters + default filter

# TODOs

* [ ] Implement SPI pluggability with annotaitons like Milan showed here: https://sodadata.slack.com/archives/D0286LXELAX/p1732118792426599
* [ ] Decide on Yaml source location references during execution
* [ ] Figure out what `os.getenv("TEST_TABLE_SEED", None)` does in TestTable.__test_table_hash

# Architectural decisions

* [ ] Consider merging all Soda set up configs like data sources & soda cloud into a single file
* [ ] Fluent API for contract verification. TODO compare alternatives.
* [ ] Improve split between check evaluation and failed rows extraction
* [ ] Review the list below
* [ ] Support sending logs to soda cloud in parallel execution (Logs passing)


* Enable observability to reuse data source & test infrastructure without the contract verification flow
* Library pluggability: enable commercial extensions without code duplication or changing imports
* Separate domain models for yaml parsing and execution
* Proper, custom error messages
* Testing error messages 
* Improved SPI for data sources
  * Improve modularity with query builders
  * Reduce responsibility of sql dialect 
  * AST model for queries
* Keep the lazy test table creation

# Contract YAML language decisions

* [ ] Recon approach
* [ ] Should the reference from contract to data source be a file reference or a name reference?
* [ ] Reuse & check templates
* [ ] CI/CD support

# Python API decisions

* [ ] Fluent API or alternative: how to force max 1 values?
* [ ] Consider soda.yml as a project file to configure soda cloud and other repo-wide configs
