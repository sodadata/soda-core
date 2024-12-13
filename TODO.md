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


# TODOs

* Clean up the list of TODOs here
* Implement SPI pluggability according to https://sodadata.slack.com/archives/D0286LXELAX/p1732118792426599
* Consider merging all Soda set up configs like data sources & soda cloud into a single file
* Decide on Yaml source location references during execution

### Contract features
* Build failed rows (all row level checks) 
* Improve split between check evaluation and failed rows extraction

### YAML file decisions
* [ ] Should the reference from contract to data source be a file reference or a name reference?
* [ ] Should the checks be grouped by name to enforce singular, optional check types like missing, validity etc?
* [ ] Should the check configurations be grouped in the central column like validity configs?

### Python API decisions
* [ ] Consider soda.yml as a project file to configure soda cloud and other repo-wide configs

### Features
* [ ] Add named filters & default filter
      filter name must part of the identity of the metrics
      - no filter part if no filter is specified
      - "default" is the filter name if there is only a default specified with "filter_expression_sql"
      - {filter_name} if a filter is activated from a named map of filters
      self.filter: str | None = None

### Other
* [ ] Exclude test suite from OSS codebase
* [ ] Figure out what `os.getenv("TEST_TABLE_SEED", None)` does in TestTable.__test_table_hash


# High level roadmap

UC 1: Set up a local development environment
* Create Python virtual environment
* Instal Soda OSS
* Create data source & contract YAML files
* Create Python script that executes the contract checks
* Test the Python script
* Available in OSS & commercial

UC 2: Embed contract verification in Airflow
* In the Airflow DAG, add a Soda contract verification operator
* Point the operator to the data source and contract YAML configuration files
* Test the Airflow pipeline
* Review the logs in the Airflow console
* Optionally make the Airflow DAG fail if contract verification fails or errors
* Available in OSS & commercial

UC 3: Add Soda Cloud connection to existing contract verification
* Add Soda Cloud YAML configuration file next to the data source file.
* Reference the Soda Cloud configuration file in the contract verification API
* Test it
* Configure notifications on Soda Cloud

UC 4: Execute contract verification on Soda Agent
* On a Soda Python API based contract verification (or Airflow operator), that has Soda Cloud configured...
* Add a configuration parameter that makes the contract verification be sent to Soda Cloud over an HTTP request, which will execute it on the Soda Agent.  All the files loaded in the contract verification API like data source and contract files will be sent to Soda Cloud over the HTTP request to Soda Cloud.  Credentials need to be configured on Soda Agent

UC 5: Set up contract verification as part of CI/CD
* Given: An Airflow DAG that runs as part of a GitHub Actions workflow
* Given: The Airflow DAG includes a contract verification operator
* Given: On every PR commit, the Airflow DAG is executed (and hence also the contract verification)
* Ensure that the contract verification results are published to the PR as a comment

UC 6: Extract contract differences in CI/CD & verify dataset stability guarantees
* On every commit on a PR, ...
* Collect all the contract files and summarize all the contract changes as comment on the PR
* Verify that the changes to the contracts comply with the configured stability guarantees.  Like eg: no columns should be removed, but additions are allowed.  Or... columns can only be removed after they have been deprecated for more than 6 months.

UC 7: Execute Soda Cloud checks inside the Airflow DAG
* Given: Soda Cloud users make checks in the Soda Cloud repository
* Configure the Python API or Airflow operator so that they execute the checks from the Soda Cloud repository instead of checks from local files.
* Complication: Syncing contract version changes with the contract verification execution
* Solution notes: We may want to append checks to a local file contract verification.  That way the schema is managed by the producer and extra checks can be added by Soda Cloud users.

UC 8: Reference contract and data source files in another external git repository
* Given: Data source and contract YAML files are stored in a git repository separate from the code that executes the contract verification
* Goal: Perform contract verification referencing the files in the external git repository
* Complication: Syncing contract version changes with the contract verification execution
