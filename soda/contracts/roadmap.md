# Roadmap

### Next TODOs
* [ ] Close the connection if opened at end of execute
* [ ] Quoting problem https://github.com/sodadata/soda-core/issues/2056
* [ ] filter_sql on checks https://github.com/sodadata/soda-core/issues/2054
* [ ] Finish the auto-search path for data sources yaml files (user home and 5 levels up the folder structure)
* [ ] Decide on owner key and fix the JSON Schema.  Consider the notifications key as well.
* [ ] Skipping checks
* [ ] Splitting/merging multiple files into single contract (or include).  Consider Templating.
* [ ] Harmonize the sql_ keys (all in the front or all in the back)
* [ ] Document how to run a contract inside a notebook (azure/databricks)
* [ ] Clean up file extensions
* [ ] Add failed rows query support
* [ ] Test Soda Cloud link

### Later (work to be refined)
* [ ] Add Docker container for verifying contract
* [ ] Add CLI support for verifying contract
* [ ] Add CLI support to create a new contract based on profiling information
* [ ] Add attributes upload to Soda Cloud
* [ ] Add a way to monitor arrival time SLOs (Requires log analysis)
* [ ] Add a data contract language version to the format
* [ ] Distill changes as GitHub webhook
* [ ] Propose contract updates for contract verification check failures
* [ ] Add support for nested JSON data types
