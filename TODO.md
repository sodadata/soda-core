# Roadmap

### Phase 1: Ensure other parts can use the new engine

* Execute contract verification programmatically in test suite
  * Provide data source & contract YAML string as input
  * Variable resolving for credentials
  * Verify multiple contracts on a single connection
  * Minimal check types: schema, row_count, missing
  * Check type pluggability with annotations (Milan) https://sodadata.slack.com/archives/D0286LXELAX/p1732118792426599
  * Parse contract incl static analysis without requiring a connection
  * Programmatic access to the contract results
  * Print logs to console (python logging) in local dev env
  * Keep the lazy test table creation
* Public API: YAML file content via file path, yaml string or yaml dict.
* Show errors with line numbers and/or links to documentation
* Library pluggability: enable commercial extensions without code duplication or changing imports
* Enable observability to reuse data source & test infrastructure without the contract verification flow
* Improved SPI for data sources: query builders, minimal sql dialect, AST model for queries
* Initial, raw docs
* Publish new v4 libraries to PyPI

### Phase 2: Ensure the hard parts are done first & decisions are taken

* [Research] Produce single failed rows table inside data source for all checks (masks)  
* [Research] Send logs to soda cloud in streaming to tail the logs for testing a contract verification using agent
* [Research] Can we achieve full check type pluggability incl JSONSchema & no-code UI?  
* [Research] Consider soda.yml as a single project file to configure soda cloud and other repo-wide configs
* [Research] Recon approach
* [Research] Check identities
* [Research] Should the reference from contract to data source be a file reference or a name reference?
* [Research] Reuse, check templates & support for CI/CD & staging environments
* [Research] Filters: Named, reusable, default & time partition filters

### Phase 3: Execute towards new GA version 4

* JSONSchema file for getting code completion in IDE when authoring contract files
* Basic check types: validity, uniqueness & custom SQL
* Set up tox compatibility matrix
* Nice docs
* Ensure logs can be directed to Airflow logs 
* Advanced check types: freshness, custom SQL expression & query checks
* Verify a contract on a data frame using a Spark session
* Send contract verification results to Soda Cloud
* Execute contract verification on Soda Agent
* Set up soda-core CI/CD test suite running tox
* Embed contract verification in Airflow
* Execute Soda Cloud checks as part of a programmatic contract verification
* Reference contract and data source files in another external git repository
* Skipping optional checks, dynamic conditional skipping & check dependencies
* Advanced failed rows configuration options

# TODOs

* [ ] Figure out what `os.getenv("TEST_TABLE_SEED", None)` does in TestTable.__test_table_hash
