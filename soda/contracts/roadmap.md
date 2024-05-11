# Roadmap

* [ ] Update to new configuration file format, create unified plugin config file format, clean up file extensions also in docs
* [ ] Quoting problem https://github.com/sodadata/soda-core/issues/2056
* [ ] filter_sql on checks https://github.com/sodadata/soda-core/issues/2054
* [ ] Consider to add a data contract language version to the format
* [ ] Merge soda-core-contract module inside soda-core module


* [ ] Test all data source configurations
  * [ ] BigQuery and others on the new data source file format.  Document this as part of the contract docs 
  * [ ] Document how to run a contract inside a notebook (azure/databricks)


* [ ] Commercial features
  * [ ] Test Soda Cloud link
  * [ ] Add failed rows query support


* [ ] Packaging and engineering UX
  * [ ] Add CLI support for verifying contract
  * [ ] Add CLI support to create a new contract based on profiling information
  * [ ] Add Docker container for verifying contract
  * [ ] Airflow support


* [ ] Engineering tooling support for engineering use cases
  * [ ] Build stability checker that distills contract changes and compares them to stability policy
  * [ ] Propose contract updates for contract verification check failures


* [ ] Add support for nested JSON data types
* [ ] Skipping checks 
* [ ] Add attributes upload to Soda Cloud
* [ ] Splitting/merging multiple files into single contract (or include).  Consider Templating.
* [ ] Add a way to monitor arrival time SLOs (Requires log analysis)
