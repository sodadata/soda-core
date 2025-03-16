# TODOs

* [ ] Add test for log messages to Soda Cloud with docs, exceptions and location
* [ ] Renamings
  * DataSource -> DataSourceImpl
  * DataSourceInfo -> DataSource
  * ContractVerificationResult to ContractVerificationSession
  * ContractResult to ContractVerificationResult
* [ ] License headers
* [ ] Define and test naming restrictions. What are the data source naming restrictions?  Same for the qualifiers.  All to ensure we can generate consistent check identities.
* [ ] Create a release procedure in [README.md](README.md#creating-a-new-release)
* [ ] Test & document doc links
* [ ] Consider excluding invalid values from reference check
* [ ] Consider adding index checking to the schema check
* [ ] Figure out what `os.getenv("TEST_TABLE_SEED", None)` does in TestTable.__test_table_hash
