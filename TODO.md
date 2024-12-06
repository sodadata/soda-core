# Design decisions

Document design goals

* Fluent API for contract verification
* Reusable API for leveraging data source connection management
* Separate domain models for yaml parsing and execution
* Proper, custom error messages
* Error messages file location and docs link
* Test error messages
* Validate contracts incl static analysis (generating errors) without building a connection 
* Query construction errors without building a connection (Stretch goal) 
* Improve SPI for data sources
  * Improve modularity with query builders
  * Reduce responsibility of sql dialect 
* Keep the lazy test table creation
* New SPI for check type
* Verify multiple contracts on a single connection
* Log requirements
  * print to console (python logging) in local dev env or orchestration tool logging
  * send to soda cloud in batch 
  * send to soda cloud in streaming to tail the logs for testing a contract verification using agent  

Contract features
* Build failed rows (all row level checks) 
* Improve split between check evaluation and failed rows extraction

TODO
* Implement SPI pluggability according to https://sodadata.slack.com/archives/D0286LXELAX/p1732118792426599
* Consider merging all Soda set up configs like data sources & soda cloud into a single file
* Decide on Yaml source location references during execution

# YAML file decisions

* [ ] Should the reference from contract to data source be a file reference or a name reference?
* [ ] Should the checks be grouped by name to enforce singular, optional check types like missing, validity etc?
* [ ] Should the check configurations be grouped in the central column like validity configs?

# Python API decisions

* [ ] Consider soda.yml as a project file to configure soda cloud and other repo-wide configs

# Features

* [ ] Add named filters & default filter
      filter name must part of the identity of the metrics
      - no filter part if no filter is specified
      - "default" is the filter name if there is only a default specified with "filter_expression_sql"
      - {filter_name} if a filter is activated from a named map of filters
      self.filter: str | None = None

# Other

* [ ] Exclude test suite from OSS codebase
* [ ] Figure out what `os.getenv("TEST_TABLE_SEED", None)` does in TestTable.__test_table_hash
